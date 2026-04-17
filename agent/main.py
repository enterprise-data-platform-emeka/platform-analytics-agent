"""Analytics Agent entry point.

Two modes of operation:

  CLI (single question):
      python -m agent.main "Which country has the highest revenue?"

  FastAPI server (HTTP):
      uvicorn agent.main:app --host 0.0.0.0 --port 8080

Both modes share one AgentSession object. The session loads config, resolves
Gold schemas, and builds the system prompt once at startup. Each question then
runs the full pipeline:

    question + prior_context (optional)
        -> SQLGenerator.generate()      (Claude call 1: NL -> SQL)
        -> AthenaExecutor.execute()     (Athena query)
        -> result_validator.validate()  (anomaly checks)
        -> InsightGenerator.generate()  (Claude call 2: SQL + rows -> insight)
        -> ChartGenerator.generate()    (matplotlib PNG + Plotly HTML)
        -> AuditLogger.write()          (non-fatal S3 write)
        -> AskResult                    (returned to caller)

The CLI prints the formatted response and exits. The FastAPI endpoint returns
a JSON body with insight, chart URLs, and a session_id for multi-turn
follow-up questions.
"""

import base64
import csv
import io
import json
import logging
import os
import random
import sys
import time
import uuid
from collections import deque
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Final, cast

from agent.audit import AuditLogger
from agent.charts import ChartGenerator, ChartOutput
from agent.claude_client import ClaudeClient
from agent.config import Config
from agent.exceptions import (
    AgentError,
    ConfigurationError,
    SchemaResolutionError,
    SQLGenerationError,
)
from agent.executor import AthenaExecutor
from agent.generator import SQLGenerator
from agent.insight import InsightGenerator, InsightResponse
from agent.logging import configure_logging
from agent.prompts import build_system_prompt
from agent.result_validator import validate
from agent.schema import SchemaResolver
from agent.session import Conversation, SessionStore, Turn
from agent.validator import SQLValidator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Language detection — localised status messages
# ---------------------------------------------------------------------------


def _detect_language(text: str) -> str:
    """Detect the dominant script from Unicode block ranges.

    Returns a language code ('zh', 'ja', 'ko', 'ar', 'ru', 'el', 'he', 'th')
    or 'en' when only Latin/ASCII characters are found.
    """
    counts: dict[str, int] = {}
    for ch in text:
        cp = ord(ch)
        if 0x4E00 <= cp <= 0x9FFF or 0x3400 <= cp <= 0x4DBF:
            counts["zh"] = counts.get("zh", 0) + 1
        elif 0x3040 <= cp <= 0x309F or 0x30A0 <= cp <= 0x30FF:
            counts["ja"] = counts.get("ja", 0) + 1
        elif 0xAC00 <= cp <= 0xD7AF or 0x1100 <= cp <= 0x11FF:
            counts["ko"] = counts.get("ko", 0) + 1
        elif 0x0600 <= cp <= 0x06FF:
            counts["ar"] = counts.get("ar", 0) + 1
        elif 0x0400 <= cp <= 0x04FF:
            counts["ru"] = counts.get("ru", 0) + 1
        elif 0x0370 <= cp <= 0x03FF:
            counts["el"] = counts.get("el", 0) + 1
        elif 0x0590 <= cp <= 0x05FF:
            counts["he"] = counts.get("he", 0) + 1
        elif 0x0E00 <= cp <= 0x0E7F:
            counts["th"] = counts.get("th", 0) + 1
    if not counts:
        return "en"
    return max(counts, key=lambda k: counts[k])


_STATUS_MESSAGES: dict[str, dict[str, str]] = {
    "zh": {
        "Analyzing your question...": "正在分析您的问题...",
        "Answering from context...": "正在根据上下文作答...",
        "Generating SQL query...": "正在生成 SQL 查询...",
        "Querying your data warehouse...": "正在查询数据仓库...",
        "Generating insight...": "正在生成洞察...",
    },
    "ja": {
        "Analyzing your question...": "質問を分析中...",
        "Answering from context...": "コンテキストから回答中...",
        "Generating SQL query...": "SQL クエリを生成中...",
        "Querying your data warehouse...": "データウェアハウスを照会中...",
        "Generating insight...": "インサイトを生成中...",
    },
    "ko": {
        "Analyzing your question...": "질문을 분석하는 중...",
        "Answering from context...": "컨텍스트에서 답변 중...",
        "Generating SQL query...": "SQL 쿼리 생성 중...",
        "Querying your data warehouse...": "데이터 웨어하우스 조회 중...",
        "Generating insight...": "인사이트 생성 중...",
    },
    "ar": {
        "Analyzing your question...": "جارٍ تحليل سؤالك...",
        "Answering from context...": "جارٍ الإجابة من السياق...",
        "Generating SQL query...": "جارٍ إنشاء استعلام SQL...",
        "Querying your data warehouse...": "جارٍ الاستعلام عن مستودع البيانات...",
        "Generating insight...": "جارٍ إنشاء التحليل...",
    },
    "ru": {
        "Analyzing your question...": "Анализ вопроса...",
        "Answering from context...": "Ответ из контекста...",
        "Generating SQL query...": "Генерация SQL запроса...",
        "Querying your data warehouse...": "Запрос к хранилищу данных...",
        "Generating insight...": "Генерация аналитики...",
    },
    "el": {
        "Analyzing your question...": "Ανάλυση ερώτησης...",
        "Answering from context...": "Απάντηση από το πλαίσιο...",
        "Generating SQL query...": "Δημιουργία ερωτήματος SQL...",
        "Querying your data warehouse...": "Ερώτηση αποθήκης δεδομένων...",
        "Generating insight...": "Δημιουργία ανάλυσης...",
    },
    "he": {
        "Analyzing your question...": "מנתח את השאלה שלך...",
        "Answering from context...": "עונה מהקשר...",
        "Generating SQL query...": "מייצר שאילתת SQL...",
        "Querying your data warehouse...": "שואל את מחסן הנתונים...",
        "Generating insight...": "מייצר תובנה...",
    },
    "th": {
        "Analyzing your question...": "กำลังวิเคราะห์คำถาม...",
        "Answering from context...": "กำลังตอบจากบริบท...",
        "Generating SQL query...": "กำลังสร้าง SQL query...",
        "Querying your data warehouse...": "กำลังสอบถามคลังข้อมูล...",
        "Generating insight...": "กำลังสร้างข้อมูลเชิงลึก...",
    },
}


def _status_msg(text: str, lang: str) -> str:
    """Return the status message in the detected language, with English fallback."""
    return _STATUS_MESSAGES.get(lang, {}).get(text, text)


@dataclass
class AskResult:
    """The combined output of one question-answer pipeline run.

    Attributes:
        response: InsightResponse with insight text, assumptions, cost, flags.
        chart: ChartOutput with PNG bytes, Plotly HTML, and presigned S3 URL.
        sql: The final SQL query executed against Athena.
        inferred_question: Claude's blind inference of what the SQL is answering.
            Empty string if the inference call failed or sql is empty.
        columns: Ordered column names from the Athena result set.
        rows: Raw query result rows (capped at 100) for client-side table toggle.
        sql_retry_count: Number of SQL generation retries (0 = first try succeeded).
        row_count: Total rows returned by Athena before capping.
    """

    response: InsightResponse
    chart: ChartOutput
    sql: str
    inferred_question: str = ""
    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, str]] = field(default_factory=list)
    sql_retry_count: int = 0
    row_count: int = 0


class AgentSession:
    """One fully-wired agent session.

    Instantiate once per process (CLI) or once at application startup
    (FastAPI). All expensive one-time operations — config validation,
    Glue schema loading, system prompt construction, and API key fetch
    — happen in __init__.

    Usage:
        session = AgentSession()
        result = session.ask("Which country has the highest revenue?")
        print(result.response.format_for_display())
        # result.chart.presigned_url for the PNG

    Raises:
        ConfigurationError: if required env vars are missing.
        SchemaResolutionError: if Glue Catalog is unreachable on startup.
    """

    def __init__(self) -> None:
        self._config = Config.from_env()
        logger.info("Config loaded: %r", self._config)

        resolver = SchemaResolver(self._config.aws)
        schemas = resolver.load_all_schemas()

        self._system_prompt = build_system_prompt(
            schemas=schemas,
            gold_database=self._config.aws.glue_gold_database,
            max_rows=self._config.agent.max_rows,
        )

        client = ClaudeClient(config=self._config, schema_resolver=resolver)
        validator = SQLValidator(
            gold_database=self._config.aws.glue_gold_database,
            max_rows=self._config.agent.max_rows,
        )

        self._client = client
        self._generator = SQLGenerator(client=client, validator=validator)
        self._executor = AthenaExecutor(
            config=self._config.aws,
            max_rows=self._config.agent.max_rows,
        )
        self._insight_generator = InsightGenerator(client=client)
        self._chart_generator = ChartGenerator(config=self._config.aws)
        self._audit = AuditLogger(config=self._config.aws)

        logger.info(
            "AgentSession ready — %d Gold schemas loaded, system prompt %d chars.",
            len(schemas),
            len(self._system_prompt),
        )

    def ask(self, question: str, prior_context: str = "") -> AskResult:
        """Run the full question-answer pipeline for one question.

        When prior_context is provided (multi-turn follow-up), it is appended
        to the system prompt so Claude can resolve references like "What about
        Q4?" without needing the full conversation history in the messages list.

        Args:
            question: Plain-English analytical question from the user.
            prior_context: Optional summary of prior Q&A turns from
                Conversation.context_summary(). Empty string for first turn.

        Returns:
            AskResult containing InsightResponse and ChartOutput.

        Raises:
            SQLGenerationError: if Claude cannot produce valid SQL in 3 attempts.
            ExecutionError: if Athena rejects or times out on the query.
            InsightGenerationError: if Claude returns a malformed insight.
        """
        logger.info("ask() called: %r (prior_context=%d chars)", question[:120], len(prior_context))

        # Classify before hitting the SQL pipeline. Conversational questions
        # (meta, translation, clarification) are answered directly from prior
        # context without generating SQL or querying Athena.
        question_type = self._client.classify_question(question, prior_context)
        if question_type == "conversational":
            insight = self._client.answer_conversational(question, prior_context)
            return AskResult(
                response=InsightResponse(insight=insight, assumptions=[]),
                chart=ChartOutput(),
                sql="",
            )

        system_prompt = self._system_prompt
        if prior_context:
            system_prompt = f"{system_prompt}\n\n{prior_context}"

        generated = self._generator.generate(
            question=question,
            system_prompt=system_prompt,
        )
        logger.info(
            "SQL ready after %d attempt(s): %s",
            generated.attempts,
            generated.sql[:120],
        )

        query_result = self._executor.execute(generated.sql)
        logger.info(
            "Athena execution complete: execution_id=%s rows=%d bytes_scanned=%d",
            query_result.execution_id,
            len(query_result.rows),
            query_result.bytes_scanned,
        )

        validation_report = validate(query_result)
        if not validation_report.is_clean:
            logger.warning("Result validation flags: %s", validation_report.flags)

        response = self._insight_generator.generate(
            question=question,
            sql=generated.sql,
            query_result=query_result,
            assumptions=generated.assumptions,
            validation_report=validation_report,
        )

        chart = self._chart_generator.generate(
            result=query_result,
            question=question,
            title=response.chart_title,
        )

        self._audit.write(question=question, sql=generated.sql, response=response)

        inferred_question = self._client.infer_question_from_sql(generated.sql, question=question)

        return AskResult(
            response=response,
            chart=chart,
            sql=generated.sql,
            inferred_question=inferred_question,
            columns=query_result.columns,
            rows=query_result.rows[:100],
            sql_retry_count=max(0, generated.attempts - 1),
            row_count=len(query_result.rows),
        )


# ---------------------------------------------------------------------------
# Rate limiter — in-memory, per session_id
# ---------------------------------------------------------------------------

_RATE_LIMIT_MAX: Final[int] = 10  # max requests per window
_RATE_LIMIT_WINDOW: Final[float] = 60.0  # seconds

_rate_buckets: dict[str, deque[float]] = {}


def _check_rate_limit(session_id: str) -> bool:
    """Return True if the request is allowed, False if the limit is exceeded.

    Slides a 60-second window per session_id. Works correctly for a single
    container deployment. Not safe across multiple replicas.
    """
    now = time.monotonic()
    if session_id not in _rate_buckets:
        _rate_buckets[session_id] = deque()
    bucket = _rate_buckets[session_id]
    while bucket and now - bucket[0] > _RATE_LIMIT_WINDOW:
        bucket.popleft()
    if len(bucket) >= _RATE_LIMIT_MAX:
        return False
    bucket.append(now)
    return True


# ---------------------------------------------------------------------------
# Engineer audit log writer
# ---------------------------------------------------------------------------

_ENGINEER_LOG_PREFIX = "metadata/engineer-log"


def _write_engineer_log(
    *,
    aws_config: Any,
    session_id: str,
    request_id: str,
    timestamp_utc: str,
    question: str,
    sql: str,
    inferred_question: str,
    discrepancy_detail: str,
    verdict: str,
    bytes_scanned: int,
    athena_cost_usd: float,
    response_time_seconds: float,
    athena_query_execution_id: str,
    sql_retry_count: int,
    row_count_returned: int,
    chart_type_rendered: str,
    language: str,
) -> None:
    """Write one engineer audit log row to S3 as a single-row CSV.

    Non-fatal: logs ERROR and returns normally on any failure.
    S3 key pattern:
        metadata/engineer-log/date=YYYY-MM-DD/session={session_id}/{request_id}.csv
    """
    try:
        import boto3

        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        key = f"{_ENGINEER_LOG_PREFIX}/date={date_str}" f"/session={session_id}/{request_id}.csv"

        buf = io.StringIO()
        fieldnames = [
            "session_id",
            "request_id",
            "timestamp_utc",
            "question_asked",
            "sql_executed",
            "claude_interpretation",
            "discrepancy_detail",
            "verdict",
            "bytes_scanned",
            "athena_cost_usd",
            "response_time_seconds",
            "athena_query_execution_id",
            "sql_retry_count",
            "row_count_returned",
            "chart_type_rendered",
            "language",
        ]
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "session_id": session_id,
                "request_id": request_id,
                "timestamp_utc": timestamp_utc,
                "question_asked": question,
                "sql_executed": sql,
                "claude_interpretation": inferred_question,
                "discrepancy_detail": discrepancy_detail,
                "verdict": verdict,
                "bytes_scanned": bytes_scanned,
                "athena_cost_usd": athena_cost_usd,
                "response_time_seconds": round(response_time_seconds, 3),
                "athena_query_execution_id": athena_query_execution_id,
                "sql_retry_count": sql_retry_count,
                "row_count_returned": row_count_returned,
                "chart_type_rendered": chart_type_rendered,
                "language": language,
            }
        )

        s3 = boto3.client("s3", region_name=aws_config.region)
        s3.put_object(
            Bucket=aws_config.bronze_bucket,
            Key=key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        logger.info(
            "Engineer log written: s3://%s/%s",
            aws_config.bronze_bucket,
            key,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Engineer log write failed (non-fatal): %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel

    app = FastAPI(
        title="EDP Analytics Agent",
        description="Plain-English questions against Gold Athena tables.",
        version="1.0.0",
    )

    # Both initialised once at startup.
    _session: AgentSession | None = None
    _store: SessionStore = SessionStore()

    @app.on_event("startup")
    async def _startup() -> None:
        global _session
        configure_logging()
        _session = AgentSession()

    class AskRequest(BaseModel):
        question: str
        session_id: str | None = None

    class AskResponse(BaseModel):
        insight: str
        assumptions: list[str]
        validation_flags: list[str]
        execution_id: str
        bytes_scanned: int
        cost_usd: float
        session_id: str
        chart_type: str
        presigned_url: str | None
        html_chart: str | None
        png_b64: str | None  # base64-encoded matplotlib PNG for PDF report generation
        sql: str
        inferred_question: str  # Claude's blind inference of what the SQL answers
        chart_height: int  # Plotly iframe height in pixels
        columns: list[str]  # result column names for client-side table toggle
        rows: list[dict[str, str]]  # raw result rows (capped at 100)
        request_id: str  # UUID tracing this request through all log systems
        verdict: str  # 'Yes' = discrepancy detected, 'No' = intent matches
        discrepancy_detail: str  # one-sentence description of the discrepancy, or 'None'

    @app.post("/ask", response_model=AskResponse)
    async def ask_endpoint(body: AskRequest) -> AskResponse:
        """Accept a plain-English question and return a structured insight.

        Pass session_id from a prior response to enable multi-turn follow-ups.
        A new session_id is created on the first request and must be echoed
        back on subsequent requests to preserve conversation context.
        """
        if not body.question.strip():
            raise HTTPException(status_code=400, detail="question must not be empty")

        assert _session is not None  # guaranteed by startup event

        request_id = str(uuid.uuid4())
        request_start = time.monotonic()
        lang = _detect_language(body.question)

        # Rate limit: 10 requests per 60 seconds per session_id.
        sid_for_limit = body.session_id or "anonymous"
        if not _check_rate_limit(sid_for_limit):
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded: max 10 requests per minute per session.",
            )

        # Resolve conversation context for multi-turn follow-ups.
        conversation: Conversation | None = None
        if body.session_id:
            conversation = _store.get(body.session_id)

        prior_context = conversation.context_summary() if conversation else ""

        try:
            result = _session.ask(body.question, prior_context=prior_context)
        except SQLGenerationError as exc:
            logger.error("SQL generation failed: %s", exc)
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except AgentError as exc:
            logger.error("Agent error: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        # Create a new session on first turn; reuse the existing one on follow-ups.
        session_id: str = body.session_id if conversation is not None else _store.create()  # type: ignore[assignment]
        _store.append_turn(
            session_id,
            Turn(
                question=body.question,
                sql=result.sql,
                insight=result.response.insight,
                assumptions=result.response.assumptions,
            ),
        )

        png_b64 = (
            base64.b64encode(result.chart.png_bytes).decode() if result.chart.png_bytes else None
        )

        # Verdict: compare original question vs SQL interpretation.
        verdict, discrepancy_detail = "No", "None"
        if result.sql and result.inferred_question:
            verdict, discrepancy_detail = _session._client.get_verdict(
                body.question, result.inferred_question
            )

        # Engineer audit log — written for analytical queries only.
        if result.sql:
            _write_engineer_log(
                aws_config=_session._config.aws,
                session_id=session_id,
                request_id=request_id,
                timestamp_utc=datetime.now(UTC).isoformat(),
                question=body.question,
                sql=result.sql,
                inferred_question=result.inferred_question,
                discrepancy_detail=discrepancy_detail,
                verdict=verdict,
                bytes_scanned=result.response.bytes_scanned,
                athena_cost_usd=result.response.cost_usd,
                response_time_seconds=time.monotonic() - request_start,
                athena_query_execution_id=result.response.execution_id,
                sql_retry_count=result.sql_retry_count,
                row_count_returned=result.row_count,
                chart_type_rendered=result.chart.chart_type,
                language=lang,
            )

        return AskResponse(
            insight=result.response.insight,
            assumptions=result.response.assumptions,
            validation_flags=result.response.validation_flags,
            execution_id=result.response.execution_id,
            bytes_scanned=result.response.bytes_scanned,
            cost_usd=result.response.cost_usd,
            session_id=session_id,
            chart_type=result.chart.chart_type,
            presigned_url=result.chart.presigned_url,
            html_chart=result.chart.html,
            png_b64=png_b64,
            sql=result.sql,
            inferred_question=result.inferred_question,
            chart_height=result.chart.chart_height,
            columns=result.columns,
            rows=result.rows,
            request_id=request_id,
            verdict=verdict,
            discrepancy_detail=discrepancy_detail,
        )

    class SendReportRequest(BaseModel):
        to_email: str
        question: str
        pdf_b64: str  # complete PDF built by the UI, base64-encoded

    @app.post("/send-report")
    async def send_report(body: SendReportRequest) -> dict[str, str]:
        """Attach a pre-built PDF report and send it via AWS SES.

        The UI generates the full PDF (question, chart, insight, assumptions,
        SQL, cost, query intent) and passes it as pdf_b64. The backend decodes
        and attaches it without rebuilding anything.

        Requires the SES_SENDER_EMAIL environment variable to be set and the
        address to be verified in SES. In SES sandbox mode, the recipient
        address must also be verified.
        """
        sender = os.environ.get("SES_SENDER_EMAIL", "")
        region = os.environ.get("AWS_REGION", "eu-central-1")

        if not sender:
            raise HTTPException(
                status_code=503,
                detail="SES_SENDER_EMAIL is not configured. Set it in the ECS task environment.",
            )

        try:
            pdf_bytes = base64.b64decode(body.pdf_b64)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid pdf_b64: {exc}") from exc

        # Send via SES.
        try:
            msg = MIMEMultipart()
            msg["Subject"] = f"EDP Report: {body.question[:60]}"
            msg["From"] = sender
            msg["To"] = body.to_email
            msg.attach(MIMEText("Please find your analytics report attached.", "plain"))

            attachment = MIMEBase("application", "pdf")
            attachment.set_payload(pdf_bytes)
            encoders.encode_base64(attachment)
            attachment.add_header(
                "Content-Disposition",
                "attachment",
                filename="edp_analytics_report.pdf",
            )
            msg.attach(attachment)

            import boto3

            ses = boto3.client("ses", region_name=region)
            ses.send_raw_email(
                Source=sender,
                Destinations=[body.to_email],
                RawMessage={"Data": msg.as_string()},
            )
        except Exception as exc:
            logger.error("SES send failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail=f"Email send failed: {exc}") from exc

        logger.info("Report sent to %s for question: %s", body.to_email, body.question[:60])
        return {"status": "sent"}

    # Pool of example questions covering all Gold tables. The /examples endpoint
    # returns a random 4 so the UI refreshes on each page load.
    _EXAMPLE_POOL: list[str] = [
        "Which country has the highest total revenue?",
        "What are the top 10 best-selling products by revenue?",
        "Which carrier has the fastest average delivery time?",
        "Show me monthly revenue trends for the last year.",
        "Which payment method has the highest success rate?",
        "Who are the top 5 customers by lifetime value?",
        "What is the average order value by country?",
        "Which brand has the highest average revenue per unit?",
        "How many unique customers placed orders each month?",
        "What percentage of shipments were delivered successfully by each carrier?",
        "Which payment method processes the most failed transactions?",
        "Compare delivery speed across all carriers.",
        "What are the top 5 countries by number of orders?",
        "Which product category generates the most revenue per unit?",
    ]

    @app.get("/examples")
    def get_examples() -> dict[str, list[str]]:
        """Return a random selection of 4 example questions from the pool."""
        return {"questions": random.sample(_EXAMPLE_POOL, min(4, len(_EXAMPLE_POOL)))}

    @app.post("/ask/stream")
    def ask_stream(body: AskRequest) -> Any:
        """Stream the full pipeline response as newline-delimited JSON events.

        Each line is a JSON object with a 'type' field:
          {"type": "status", "text": "..."}   — pipeline progress update
          {"type": "token",  "text": "..."}   — insight text token (streamable)
          {"type": "error",  "text": "..."}   — fatal error, stream ends
          {"type": "done",   "data": {...}}   — full AskResponse payload

        Pass session_id from a prior response to enable multi-turn follow-ups.
        """
        from fastapi.responses import StreamingResponse as SR

        if not body.question.strip():

            def _err() -> Generator[str, None, None]:
                yield json.dumps({"type": "error", "text": "question must not be empty"}) + "\n"

            return SR(_err(), media_type="application/x-ndjson")

        assert _session is not None

        # Rate limit check before spawning the generator.
        sid_for_limit = body.session_id or "anonymous"
        if not _check_rate_limit(sid_for_limit):

            def _rate_err() -> Generator[str, None, None]:
                yield (
                    json.dumps(
                        {
                            "type": "error",
                            "text": "Rate limit exceeded: max 10 requests per minute per session.",
                        }
                    )
                    + "\n"
                )

            return SR(_rate_err(), media_type="application/x-ndjson")

        request_id = str(uuid.uuid4())
        request_start = time.monotonic()

        conversation: Conversation | None = None
        if body.session_id:
            conversation = _store.get(body.session_id)
        prior_context = conversation.context_summary() if conversation else ""

        def _generate() -> Generator[str, None, None]:
            lang = _detect_language(body.question)
            yield (
                json.dumps(
                    {"type": "status", "text": _status_msg("Analyzing your question...", lang)}
                )
                + "\n"
            )

            question_type = _session._client.classify_question(body.question, prior_context)

            if question_type == "conversational":
                yield (
                    json.dumps(
                        {"type": "status", "text": _status_msg("Answering from context...", lang)}
                    )
                    + "\n"
                )
                try:
                    insight = _session._client.answer_conversational(body.question, prior_context)
                except AgentError as exc:
                    yield json.dumps({"type": "error", "text": str(exc)}) + "\n"
                    return

                session_id: str = (
                    cast(str, body.session_id) if conversation is not None else _store.create()
                )
                _store.append_turn(
                    session_id,
                    Turn(
                        question=body.question,
                        sql="",
                        insight=insight,
                        assumptions=[],
                    ),
                )
                yield (
                    json.dumps(
                        {
                            "type": "done",
                            "data": {
                                "insight": insight,
                                "assumptions": [],
                                "validation_flags": [],
                                "execution_id": "",
                                "bytes_scanned": 0,
                                "cost_usd": 0.0,
                                "session_id": session_id,
                                "chart_type": "",
                                "presigned_url": None,
                                "html_chart": None,
                                "png_b64": None,
                                "sql": "",
                                "inferred_question": "",
                                "chart_height": 0,
                                "columns": [],
                                "rows": [],
                                "request_id": request_id,
                                "verdict": "No",
                                "discrepancy_detail": "None",
                            },
                        }
                    )
                    + "\n"
                )
                return

            # Analytical path
            system_prompt = _session._system_prompt
            if prior_context:
                system_prompt = f"{system_prompt}\n\n{prior_context}"

            yield (
                json.dumps({"type": "status", "text": _status_msg("Generating SQL query...", lang)})
                + "\n"
            )
            try:
                generated = _session._generator.generate(
                    question=body.question,
                    system_prompt=system_prompt,
                )
            except SQLGenerationError as exc:
                logger.error("SQL generation failed: %s", exc)
                yield json.dumps({"type": "error", "text": str(exc)}) + "\n"
                return

            yield (
                json.dumps(
                    {"type": "status", "text": _status_msg("Querying your data warehouse...", lang)}
                )
                + "\n"
            )
            try:
                query_result = _session._executor.execute(generated.sql)
            except AgentError as exc:
                logger.error("Athena execution failed: %s", exc)
                yield json.dumps({"type": "error", "text": str(exc)}) + "\n"
                return

            validation_report = validate(query_result)

            yield (
                json.dumps({"type": "status", "text": _status_msg("Generating insight...", lang)})
                + "\n"
            )
            if validation_report.zero_rows:
                result_markdown = "(no rows returned)"
            else:
                from agent.insight import InsightGenerator

                result_markdown = InsightGenerator._sample_markdown(query_result)

            result_container: dict[str, str] = {}
            token_iter, result_container = _session._client.stream_insight_tokens(
                question=body.question,
                sql=generated.sql,
                result_markdown=result_markdown,
            )
            for token in token_iter:
                yield json.dumps({"type": "token", "text": token}) + "\n"

            insight = result_container.get("insight", "")
            chart_title = result_container.get("chart_title", "")

            from agent.insight import InsightResponse

            response = InsightResponse(
                insight=insight,
                chart_title=chart_title,
                assumptions=generated.assumptions,
                validation_flags=validation_report.flags,
                execution_id=query_result.execution_id,
                bytes_scanned=query_result.bytes_scanned,
                cost_usd=query_result.cost_usd,
            )

            chart = _session._chart_generator.generate(
                result=query_result,
                question=body.question,
                title=chart_title,
            )
            _session._audit.write(
                question=body.question,
                sql=generated.sql,
                response=response,
            )
            inferred_question = _session._client.infer_question_from_sql(
                generated.sql, question=body.question
            )

            # Verdict: compare original question vs SQL interpretation.
            stream_verdict, stream_discrepancy = "No", "None"
            if inferred_question:
                stream_verdict, stream_discrepancy = _session._client.get_verdict(
                    body.question, inferred_question
                )

            session_id = cast(str, body.session_id) if conversation is not None else _store.create()
            _store.append_turn(
                session_id,
                Turn(
                    question=body.question,
                    sql=generated.sql,
                    insight=insight,
                    assumptions=generated.assumptions,
                ),
            )

            # Engineer audit log.
            _write_engineer_log(
                aws_config=_session._config.aws,
                session_id=session_id,
                request_id=request_id,
                timestamp_utc=datetime.now(UTC).isoformat(),
                question=body.question,
                sql=generated.sql,
                inferred_question=inferred_question,
                discrepancy_detail=stream_discrepancy,
                verdict=stream_verdict,
                bytes_scanned=query_result.bytes_scanned,
                athena_cost_usd=query_result.cost_usd,
                response_time_seconds=time.monotonic() - request_start,
                athena_query_execution_id=query_result.execution_id,
                sql_retry_count=max(0, generated.attempts - 1),
                row_count_returned=len(query_result.rows),
                chart_type_rendered=chart.chart_type,
                language=lang,
            )

            png_b64 = base64.b64encode(chart.png_bytes).decode() if chart.png_bytes else None

            yield (
                json.dumps(
                    {
                        "type": "done",
                        "data": {
                            "insight": insight,
                            "assumptions": generated.assumptions,
                            "validation_flags": validation_report.flags,
                            "execution_id": query_result.execution_id,
                            "bytes_scanned": query_result.bytes_scanned,
                            "cost_usd": query_result.cost_usd,
                            "session_id": session_id,
                            "chart_type": chart.chart_type,
                            "presigned_url": chart.presigned_url,
                            "html_chart": chart.html,
                            "png_b64": png_b64,
                            "sql": generated.sql,
                            "inferred_question": inferred_question,
                            "chart_height": chart.chart_height,
                            "columns": query_result.columns,
                            "rows": query_result.rows[:100],
                            "request_id": request_id,
                            "verdict": stream_verdict,
                            "discrepancy_detail": stream_discrepancy,
                        },
                    }
                )
                + "\n"
            )

        return SR(_generate(), media_type="application/x-ndjson")

    @app.get("/engineer-log")
    async def engineer_log(session_id: str) -> dict[str, Any]:
        """Return all engineer log rows for a session as a CSV string.

        Reads every per-request CSV file under
        metadata/engineer-log/date=*/session={session_id}/*.csv
        from the Bronze bucket, concatenates the rows (header once), and
        returns the result so the UI can offer a one-click download.

        Returns {"csv": "<csv text>", "row_count": n} on success.
        Returns {"csv": "", "row_count": 0} if no log exists yet.
        """
        if not session_id or not session_id.strip():
            raise HTTPException(status_code=400, detail="session_id is required")
        try:
            import boto3
            from botocore.exceptions import ClientError

            s3 = boto3.client("s3", region_name=_session._config.aws.region)
            prefix = f"{_ENGINEER_LOG_PREFIX}/"
            paginator = s3.get_paginator("list_objects_v2")
            session_suffix = f"/session={session_id}/"
            keys: list[str] = []
            for page in paginator.paginate(
                Bucket=_session._config.aws.bronze_bucket, Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if session_suffix in key and key.endswith(".csv"):
                        keys.append(key)

            if not keys:
                return {"csv": "", "row_count": 0}

            keys.sort()
            rows_out: list[str] = []
            header_written = False
            for key in keys:
                try:
                    obj = s3.get_object(Bucket=_session._config.aws.bronze_bucket, Key=key)
                    text = obj["Body"].read().decode("utf-8")
                    lines = text.splitlines()
                    if not lines:
                        continue
                    if not header_written:
                        rows_out.append(lines[0])  # header
                        header_written = True
                    if len(lines) > 1:
                        rows_out.extend(lines[1:])
                except ClientError:
                    pass

            csv_text = "\n".join(rows_out)
            return {"csv": csv_text, "row_count": len(rows_out) - (1 if header_written else 0)}
        except Exception as exc:  # noqa: BLE001
            logger.error("engineer-log fetch failed: %s", exc)
            raise HTTPException(status_code=500, detail="Could not retrieve engineer log") from exc

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Liveness check for the ALB target group health check."""
        return {"status": "ok"}

except ImportError:
    # FastAPI is not installed — CLI-only mode.
    app = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _cli_main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns exit code (0 = success, 1 = error).

    Usage:
        python -m agent.main "Which country has the highest revenue?"
    """
    configure_logging()
    args = argv if argv is not None else sys.argv[1:]

    if not args:
        print(
            'Usage: python -m agent.main "<question>"\n'
            'Example: python -m agent.main "Which country has the highest revenue?"',
            file=sys.stderr,
        )
        return 1

    question = " ".join(args)

    try:
        session = AgentSession()
    except (ConfigurationError, SchemaResolutionError) as exc:
        print(f"Startup error: {exc}", file=sys.stderr)
        return 1

    try:
        result = session.ask(question)
    except SQLGenerationError as exc:
        print(f"Could not generate SQL: {exc}", file=sys.stderr)
        return 1
    except AgentError as exc:
        print(f"Agent error: {exc}", file=sys.stderr)
        return 1

    print(result.response.format_for_display())
    if result.chart.presigned_url:
        print(f"\nChart: {result.chart.presigned_url}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli_main())
