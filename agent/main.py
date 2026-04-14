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
import logging
import os
import sys
import tempfile
from dataclasses import dataclass
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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


@dataclass
class AskResult:
    """The combined output of one question-answer pipeline run.

    Attributes:
        response: InsightResponse with insight text, assumptions, cost, flags.
        chart: ChartOutput with PNG bytes, Plotly HTML, and presigned S3 URL.
        sql: The final SQL query executed against Athena.
    """

    response: InsightResponse
    chart: ChartOutput
    sql: str


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

        chart = self._chart_generator.generate(result=query_result, question=question)

        self._audit.write(question=question, sql=generated.sql, response=response)

        return AskResult(response=response, chart=chart, sql=generated.sql)


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
                sql=result.response.execution_id,  # execution_id is the traceability key
                insight=result.response.insight,
                assumptions=result.response.assumptions,
            ),
        )

        png_b64 = (
            base64.b64encode(result.chart.png_bytes).decode()
            if result.chart.png_bytes
            else None
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
        )

    class SendReportRequest(BaseModel):
        to_email: str
        question: str
        insight: str
        png_b64: str | None = None

    @app.post("/send-report")
    async def send_report(body: SendReportRequest) -> dict[str, str]:
        """Generate a PDF report (chart + summary) and send it via AWS SES.

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

        # Generate PDF using fpdf2.
        try:
            from fpdf import FPDF

            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.add_page()

            # Title
            pdf.set_font("Helvetica", "B", 18)
            pdf.cell(0, 12, "EDP Analytics Report", ln=True)
            pdf.ln(4)

            # Question
            pdf.set_font("Helvetica", "B", 12)
            pdf.cell(0, 8, "Question", ln=True)
            pdf.set_font("Helvetica", "", 11)
            pdf.multi_cell(0, 7, body.question)
            pdf.ln(6)

            # Chart image
            if body.png_b64:
                png_bytes = base64.b64decode(body.png_b64)
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    tmp.write(png_bytes)
                    tmp_path = tmp.name
                pdf.image(tmp_path, x=10, w=190)
                os.unlink(tmp_path)
                pdf.ln(6)

            # Summary
            pdf.set_font("Helvetica", "B", 12)
            pdf.cell(0, 8, "Summary", ln=True)
            pdf.set_font("Helvetica", "", 11)
            pdf.multi_cell(0, 7, body.insight)

            pdf_bytes = bytes(pdf.output())
        except Exception as exc:
            logger.error("PDF generation failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail=f"PDF generation failed: {exc}") from exc

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
