"""Claude API client for the Analytics Agent.

Wraps the Anthropic SDK with:
  - SSM Parameter Store key fetch at startup
  - Retry on transient errors (rate limit, timeout, connection)
  - Tool-use fallback: handles get_schema calls before returning final SQL
  - Response parsing: extracts <sql> / <assumptions> and insight text

Two public methods:
  generate_sql(messages, system_prompt) -> (sql, assumptions)
  generate_insight(question, sql, result_markdown) -> insight string

The correction loop (up to 3 attempts) lives in generator.py, not here.
This client makes exactly one logical API call per method, which may
internally expand to multiple HTTP requests when tool use is involved.
"""

import logging
import re
import time
from collections.abc import Iterator
from typing import Any, Final, cast

import anthropic
import boto3
from botocore.exceptions import ClientError

from agent.config import AWSConfig, Config
from agent.exceptions import (
    ConfigurationError,
    InsightGenerationError,
    SQLGenerationError,
)
from agent.prompts import (
    GET_SCHEMA_TOOL,
    INSIGHT_SYSTEM_PROMPT,
    build_insight_messages,
)
from agent.schema import SchemaResolver

logger = logging.getLogger(__name__)

MODEL: Final[str] = "claude-sonnet-4-6"

# Tokens for each call type. SQL responses include the query + assumptions list.
# Insight responses are 2-3 sentences plus a short chart title tag.
# Classify returns one word. Intent inference is one sentence.
_MAX_TOKENS_SQL: Final[int] = 1024
_MAX_TOKENS_INSIGHT: Final[int] = 600
_MAX_TOKENS_CLASSIFY: Final[int] = 5
_MAX_TOKENS_CONVERSATIONAL: Final[int] = 512
_MAX_TOKENS_INTENT: Final[int] = 150

# System prompt for the binary question classifier.
# "ANALYTICAL" = needs a new Athena query. "CONVERSATIONAL" = answerable from prior context.
_CLASSIFY_SYSTEM: Final[str] = (
    "Classify the user message. Reply with exactly one word.\n"
    "ANALYTICAL — requires querying a database for new or updated data. Also use "
    "ANALYTICAL when the user explicitly asks for a chart, plot, graph, or "
    "visualisation — even if they are requesting a repeat or translation, because "
    "charts are only produced by running a fresh query.\n"
    "CONVERSATIONAL — can be answered from prior conversation context alone: asking "
    "what was said, requesting a text-only translation or summary of a prior answer, "
    "asking for clarification, or any meta-question that does not need fresh data "
    "and does not require a chart."
)

# System prompt for inferring the business question from a SQL query (no original
# question provided — intentionally blind so the inference is unbiased).
_INTENT_SYSTEM: Final[str] = (
    "You are a SQL analyst. Given a SQL query, identify the business question it "
    "is trying to answer. Reply with exactly one plain-English sentence starting "
    "with 'What' or 'Which' or 'How'. Do not mention SQL, tables, or column names. "
    "Base your answer solely on what the query measures and filters."
)

# System prompt for answering conversational/meta questions without hitting Athena.
_CONVERSATIONAL_SYSTEM: Final[str] = (
    "You are a data analytics assistant. Answer the user's question using only the "
    "prior conversation context provided. Do not invent or estimate data values. "
    "If the context does not contain enough information to answer, say so clearly. "
    "Be concise: 2-3 sentences."
)

# Transient errors worth retrying. Permanent errors (auth, bad request,
# permission denied) are not retried — they indicate a configuration problem.
_TRANSIENT_ERRORS: Final[tuple[type[Exception], ...]] = (
    anthropic.RateLimitError,
    anthropic.APITimeoutError,
    anthropic.APIConnectionError,
)
_MAX_RETRIES: Final[int] = 3
_RETRY_DELAYS: Final[tuple[float, ...]] = (2.0, 5.0, 10.0)

# Max tool-use rounds per generate_sql call. One round covers 99% of cases.
# A second round triggers a warning — it signals a schema loading gap.
_MAX_TOOL_ROUNDS: Final[int] = 2

# Regex to extract <sql> and <assumptions> blocks from Claude's response.
_SQL_TAG_RE: Final[re.Pattern[str]] = re.compile(r"<sql>(.*?)</sql>", re.DOTALL | re.IGNORECASE)
_ASSUMPTIONS_TAG_RE: Final[re.Pattern[str]] = re.compile(
    r"<assumptions>(.*?)</assumptions>", re.DOTALL | re.IGNORECASE
)

# Regexes to extract <chart_title> and <insight> blocks from insight responses.
_CHART_TITLE_TAG_RE: Final[re.Pattern[str]] = re.compile(
    r"<chart_title>(.*?)</chart_title>", re.DOTALL | re.IGNORECASE
)
_INSIGHT_BODY_TAG_RE: Final[re.Pattern[str]] = re.compile(
    r"<insight>(.*?)</insight>", re.DOTALL | re.IGNORECASE
)


class ClaudeClient:
    """Wraps the Anthropic Messages API for the Analytics Agent.

    Usage:
        client = ClaudeClient(config, schema_resolver)
        sql, assumptions = client.generate_sql(messages, system_prompt)
        insight = client.generate_insight(question, sql, result_markdown)
    """

    def __init__(
        self,
        config: Config,
        schema_resolver: SchemaResolver,
        api_key: str | None = None,
    ) -> None:
        """Initialise the client.

        Args:
            config: Fully validated agent config.
            schema_resolver: Used to serve tool-use get_schema calls.
            api_key: Optional API key override. If None, the key is fetched
                from SSM Parameter Store using config.aws.ssm_api_key_param.
                Pass a value in tests to skip the SSM call.
        """
        if api_key is None:
            api_key = self._fetch_api_key(config.aws)
        self._client = anthropic.Anthropic(api_key=api_key)
        self._schema_resolver = schema_resolver
        self._config = config

    # ── Public API ─────────────────────────────────────────────────────────────

    def generate_sql(
        self,
        messages: list[dict[str, str]],
        system_prompt: str,
    ) -> tuple[str, list[str]]:
        """Generate SQL from a messages conversation history.

        Makes one API call (or more if tool use is triggered). Does not run
        the validation-correction loop — that is handled by generator.py.

        Args:
            messages: Conversation history. For an initial question this is
                [{"role": "user", "content": question}]. For a correction round
                the history includes the prior assistant response and correction
                message; use prompts.build_sql_correction_messages() to build it.
            system_prompt: Full system prompt with embedded Gold schemas from
                prompts.build_system_prompt().

        Returns:
            Tuple of (sql, assumptions) where sql is the raw generated SQL
            string (not yet validated) and assumptions is a list of strings.

        Raises:
            SQLGenerationError: if Claude's response cannot be parsed, or if the
                API is unavailable after all retries.
        """
        response = self._call(
            messages=messages,
            system=system_prompt,
            tools=[GET_SCHEMA_TOOL],
            max_tokens=_MAX_TOKENS_SQL,
        )

        # Resolve any tool-use calls before parsing the final answer.
        if response.stop_reason == "tool_use":
            response = self._handle_tool_use(
                response=response,
                messages=messages,
                system=system_prompt,
            )

        return self._parse_sql_response(response)

    def generate_insight(
        self,
        question: str,
        sql: str,
        result_markdown: str,
    ) -> tuple[str, str]:
        """Generate a plain-English insight and a short chart title from query results.

        Args:
            question: The original plain-English question from the user.
            sql: The validated SQL that was executed.
            result_markdown: The query result formatted as a markdown table.

        Returns:
            Tuple of (insight_text, chart_title). Both are in the same language
            as the question. chart_title is a short 5-8 word description.

        Raises:
            InsightGenerationError: if Claude's response is empty or malformed,
                or if the API is unavailable after all retries.
        """
        messages = build_insight_messages(question, sql, result_markdown)
        response = self._call(
            messages=messages,
            system=INSIGHT_SYSTEM_PROMPT,
            tools=None,
            max_tokens=_MAX_TOKENS_INSIGHT,
        )
        return self._parse_insight_with_title(response)

    def classify_question(self, question: str, prior_context: str) -> str:
        """Return 'analytical' or 'conversational' for the given question.

        Uses a lightweight single-call Claude classifier. Works in any language.
        Defaults to 'analytical' on any error so the existing pipeline handles it.

        Args:
            question: The raw user question.
            prior_context: Summary of prior Q&A turns from the session.

        Returns:
            'conversational' if the question can be answered from prior context
            alone. 'analytical' if it needs a new Athena query.
        """
        content = question
        if prior_context:
            content = f"Prior context:\n{prior_context}\n\nUser message: {question}"
        try:
            response = self._call(
                messages=[{"role": "user", "content": content}],
                system=_CLASSIFY_SYSTEM,
                tools=None,
                max_tokens=_MAX_TOKENS_CLASSIFY,
            )
            text = self._extract_text(response).strip().upper()
            if "CONVERSATIONAL" in text:
                logger.info("Question classified as CONVERSATIONAL.")
                return "conversational"
        except Exception as exc:  # noqa: BLE001
            logger.warning("Question classification failed (%s) — defaulting to analytical.", exc)
        return "analytical"

    def answer_conversational(self, question: str, prior_context: str) -> str:
        """Answer a conversational/meta question from prior conversation context.

        Does not query Athena. Uses Claude with the prior context summary to
        answer questions like "what did you say last?" or "give me that in German".

        Args:
            question: The raw user question.
            prior_context: Summary of prior Q&A turns from the session.

        Returns:
            A short plain-English answer (2-3 sentences).

        Raises:
            InsightGenerationError: if Claude returns an empty response.
        """
        content = question
        if prior_context:
            content = f"Prior conversation context:\n{prior_context}\n\nUser question: {question}"
        response = self._call(
            messages=[{"role": "user", "content": content}],
            system=_CONVERSATIONAL_SYSTEM,
            tools=None,
            max_tokens=_MAX_TOKENS_CONVERSATIONAL,
        )
        return self._parse_insight_response(response)

    def stream_insight_tokens(
        self,
        question: str,
        sql: str,
        result_markdown: str,
    ) -> tuple[Iterator[str], dict[str, str]]:
        """Stream insight tokens from Claude, parsing the result on completion.

        Drives the same insight Claude call as generate_insight(), but using
        the streaming Messages API so callers can yield tokens progressively.

        Args:
            question: The original plain-English question.
            sql: The validated SQL that was executed.
            result_markdown: The query result as a markdown table.

        Returns:
            A (token_iterator, result_container) tuple. Exhausting the iterator
            streams text from Claude; result_container is populated with
            'insight' and 'chart_title' keys once the iterator is exhausted.

        The caller MUST exhaust the iterator before reading result_container.
        """
        result: dict[str, str] = {}
        messages = build_insight_messages(question, sql, result_markdown)

        def _iter() -> Iterator[str]:
            full_text = ""
            with self._client.messages.stream(
                model=MODEL,
                max_tokens=_MAX_TOKENS_INSIGHT,
                system=INSIGHT_SYSTEM_PROMPT,
                messages=messages,
            ) as stream:
                for text in stream.text_stream:
                    full_text += text
                    yield text
            insight, chart_title = self._parse_insight_text(full_text.strip())
            result["insight"] = insight
            result["chart_title"] = chart_title

        return _iter(), result

    def infer_question_from_sql(self, sql: str) -> str:
        """Infer the business question a SQL query is trying to answer.

        Makes a fresh Claude call with ONLY the SQL — the original question is
        deliberately withheld so the inference is unbiased. The result is used
        as a sanity check: if the inferred question matches the user's intent,
        the generated SQL is answering the right thing.

        Args:
            sql: The validated SQL query that was executed against Athena.

        Returns:
            One plain-English sentence describing what the SQL is measuring.
            Returns an empty string on any error so callers can treat it as
            optional metadata.
        """
        try:
            response = self._call(
                messages=[
                    {
                        "role": "user",
                        "content": (
                            "What business question is this SQL query trying to answer?\n\n"
                            f"```sql\n{sql}\n```"
                        ),
                    }
                ],
                system=_INTENT_SYSTEM,
                tools=None,
                max_tokens=_MAX_TOKENS_INTENT,
            )
            return self._extract_text(response).strip()
        except Exception as exc:  # noqa: BLE001
            logger.warning("SQL intent inference failed: %s", exc)
            return ""

    # ── Private helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _fetch_api_key(aws_config: AWSConfig) -> str:
        """Fetch the Anthropic API key from SSM Parameter Store.

        Raises:
            ConfigurationError: if the parameter does not exist or access is denied.
        """
        ssm = boto3.client("ssm", region_name=aws_config.region)
        try:
            response = ssm.get_parameter(
                Name=aws_config.ssm_api_key_param,
                WithDecryption=True,
            )
            return str(response["Parameter"]["Value"])
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("ParameterNotFound", "AccessDeniedException"):
                raise ConfigurationError(
                    f"Cannot fetch Anthropic API key from SSM parameter "
                    f"'{aws_config.ssm_api_key_param}': {exc}"
                ) from exc
            raise ConfigurationError(f"Unexpected SSM error fetching API key: {exc}") from exc

    def _call(
        self,
        messages: list[dict[str, str]],
        system: str,
        tools: list[dict[str, Any]] | None,
        max_tokens: int,
    ) -> anthropic.types.Message:
        """Make one Messages API call with retry on transient errors.

        Retries up to _MAX_RETRIES times with increasing delays on rate limit,
        timeout, or connection errors. All other errors propagate immediately.

        Raises:
            SQLGenerationError: after exhausting retries on transient errors.
        """
        kwargs: dict[str, Any] = {
            "model": MODEL,
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
        }
        if tools:
            kwargs["tools"] = tools

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                return self._client.messages.create(**kwargs)  # type: ignore[no-any-return]
            except _TRANSIENT_ERRORS as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES:
                    delay = _RETRY_DELAYS[attempt - 1]
                    logger.warning(
                        "Transient API error (attempt %d/%d): %s. Retrying in %.0fs.",
                        attempt,
                        _MAX_RETRIES,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

        raise SQLGenerationError(
            f"Claude API unavailable after {_MAX_RETRIES} retries: {last_exc}"
        ) from last_exc

    def _handle_tool_use(
        self,
        response: anthropic.types.Message,
        messages: list[dict[str, str]],
        system: str,
    ) -> anthropic.types.Message:
        """Resolve get_schema tool calls and return the final response.

        Appends the assistant's tool-use message, executes each tool call
        using the SchemaResolver, then makes another API call with the
        tool results. Caps at _MAX_TOOL_ROUNDS rounds.

        Raises:
            SQLGenerationError: if Claude keeps calling tools beyond the limit,
                or if an unknown tool name is requested.
        """
        current_messages: list[dict[str, Any]] = list(messages)

        for round_number in range(1, _MAX_TOOL_ROUNDS + 1):
            if round_number == _MAX_TOOL_ROUNDS:
                logger.warning(
                    "Tool-use round %d reached (max %d). "
                    "All Gold schemas should already be in the system prompt. "
                    "Check that SchemaResolver.load_all_schemas() ran at startup.",
                    round_number,
                    _MAX_TOOL_ROUNDS,
                )

            # Add the assistant's tool-use turn to the conversation.
            current_messages.append({"role": "assistant", "content": response.content})

            # Build the tool_result block for each tool_use content block.
            tool_results: list[dict[str, Any]] = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                if block.name != "get_schema":
                    raise SQLGenerationError(
                        f"Claude requested unknown tool '{block.name}'. "
                        "Only 'get_schema' is defined."
                    )
                table_name: str = cast(dict[str, Any], block.input).get("table_name", "")
                logger.debug(
                    "Tool call: get_schema(table_name=%r) — round %d",
                    table_name,
                    round_number,
                )
                schema_text = self._get_schema_for_tool(table_name)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": schema_text,
                    }
                )

            current_messages.append({"role": "user", "content": tool_results})

            response = self._call(
                messages=current_messages,
                system=system,
                tools=[GET_SCHEMA_TOOL],
                max_tokens=_MAX_TOKENS_SQL,
            )

            if response.stop_reason != "tool_use":
                return response

        raise SQLGenerationError(
            f"Claude made more than {_MAX_TOOL_ROUNDS} tool-use rounds without "
            "producing a final SQL response. Check that all Gold schemas are "
            "loaded in the system prompt."
        )

    def _get_schema_for_tool(self, table_name: str) -> str:
        """Fetch a single table schema for a tool_result response.

        Returns a formatted schema string. On any error, returns an error
        message string — the tool_result must always have content so Claude
        can gracefully handle the failure in its next response.
        """
        try:
            schema = self._schema_resolver.get_schema(table_name)
            return schema.to_prompt_text()
        except Exception as exc:  # noqa: BLE001
            logger.warning("get_schema tool failed for %r: %s", table_name, exc)
            return (
                f"Schema not found for table '{table_name}': {exc}. "
                "Use only the tables documented in the system prompt."
            )

    def _parse_sql_response(
        self,
        response: anthropic.types.Message,
    ) -> tuple[str, list[str]]:
        """Extract sql and assumptions from a <sql>/<assumptions> tagged response.

        Raises:
            SQLGenerationError: if the <sql> block is missing or empty.
        """
        text = self._extract_text(response)

        sql_match = _SQL_TAG_RE.search(text)
        if not sql_match:
            raise SQLGenerationError(
                f"Claude response did not contain a <sql> block. "
                f"Raw response (first 300 chars): {text[:300]!r}"
            )

        sql = sql_match.group(1).strip()
        if not sql:
            raise SQLGenerationError("Claude returned an empty <sql> block.")

        assumptions: list[str] = []
        assumptions_match = _ASSUMPTIONS_TAG_RE.search(text)
        if assumptions_match:
            raw = assumptions_match.group(1).strip()
            assumptions = [
                line.lstrip("- ").strip()
                for line in raw.splitlines()
                if line.strip() and line.strip() not in ("-", "")
            ]
        else:
            logger.warning(
                "Claude response contained no <assumptions> block. "
                "The question will be answered without assumption transparency."
            )

        logger.debug(
            "Parsed SQL response: %d chars of SQL, %d assumptions.",
            len(sql),
            len(assumptions),
        )
        return sql, assumptions

    def _parse_insight_text(self, text: str) -> tuple[str, str]:
        """Parse (insight, chart_title) from raw text. Shared by streaming and non-streaming.

        Raises:
            InsightGenerationError: if text is empty or yields no insight.
        """
        if not text:
            raise InsightGenerationError("Claude returned an empty insight response.")

        chart_title = ""
        title_match = _CHART_TITLE_TAG_RE.search(text)
        if title_match:
            chart_title = title_match.group(1).strip()

        insight_match = _INSIGHT_BODY_TAG_RE.search(text)
        if insight_match:
            insight = insight_match.group(1).strip()
        else:
            insight = _CHART_TITLE_TAG_RE.sub("", text).strip()

        if not insight:
            raise InsightGenerationError("Claude returned an empty insight response.")

        return insight, chart_title

    def _parse_insight_with_title(
        self,
        response: anthropic.types.Message,
    ) -> tuple[str, str]:
        """Extract insight text and chart title from a tagged Claude response.

        Expects the response to contain <chart_title>...</chart_title> and
        <insight>...</insight> tags. Falls back gracefully when tags are absent
        so existing calls do not break if the model omits them.

        Returns:
            Tuple of (insight_text, chart_title).

        Raises:
            InsightGenerationError: if both the tagged and raw insight are empty.
        """
        text = self._extract_text(response).strip()
        return self._parse_insight_text(text)

    def _parse_insight_response(
        self,
        response: anthropic.types.Message,
    ) -> str:
        """Extract plain text from Claude's response (used for conversational answers).

        Raises:
            InsightGenerationError: if the response is empty.
        """
        text = self._extract_text(response).strip()
        if not text:
            raise InsightGenerationError("Claude returned an empty insight response.")
        return text

    @staticmethod
    def _extract_text(response: anthropic.types.Message) -> str:
        """Extract concatenated text from all TextBlock content blocks."""
        parts: list[str] = []
        for block in response.content:
            if hasattr(block, "text"):
                parts.append(block.text)
        return "\n".join(parts)
