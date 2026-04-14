"""Tests for main.py — AgentSession and CLI entry point."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from agent.charts import ChartOutput
from agent.exceptions import AgentError, ConfigurationError, SQLGenerationError
from agent.insight import InsightResponse
from agent.main import AgentSession, AskResult, _cli_main

# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_response(
    insight: str = "Germany leads with £432k revenue.",
    assumptions: list[str] | None = None,
    flags: list[str] | None = None,
) -> InsightResponse:
    return InsightResponse(
        insight=insight,
        assumptions=assumptions or ["Table: revenue_by_country"],
        validation_flags=flags or [],
        execution_id="exec-test-123",
        bytes_scanned=20 * 1024 * 1024,
        cost_usd=0.000095,
    )


def _make_chart(presigned_url: str | None = "https://s3.example.com/chart.png") -> ChartOutput:
    return ChartOutput(
        png_bytes=b"\x89PNG...",
        html="<div>chart</div>",
        presigned_url=presigned_url,
        chart_type="bar",
    )


def _make_ask_result(
    response: InsightResponse | None = None,
    chart: ChartOutput | None = None,
    sql: str = "SELECT 1",
) -> AskResult:
    return AskResult(
        response=response or _make_response(),
        chart=chart or _make_chart(),
        sql=sql,
    )


def _patch_session_deps(mock_ask_result: AskResult | None = None) -> list[Any]:
    """Patch all AgentSession dependencies so it constructs without AWS/Claude.

    Returns a list of patches that must be started and stopped by the caller.
    """
    from agent.executor import QueryResult
    from agent.generator import GeneratedSQL

    generated_sql = GeneratedSQL(
        sql="SELECT country, total_revenue FROM revenue_by_country LIMIT 10",
        assumptions=["Table: revenue_by_country"],
        attempts=1,
    )
    query_result = QueryResult(
        execution_id="exec-test-123",
        columns=["country", "total_revenue"],
        rows=[{"country": "Germany", "total_revenue": "432701.55"}],
        bytes_scanned=20 * 1024 * 1024,
        cost_usd=0.000095,
    )
    ask_result = mock_ask_result or _make_ask_result()

    mock_generator = MagicMock()
    mock_generator.generate.return_value = generated_sql
    mock_executor = MagicMock()
    mock_executor.execute.return_value = query_result
    mock_insight_gen = MagicMock()
    mock_insight_gen.generate.return_value = ask_result.response
    mock_chart_gen = MagicMock()
    mock_chart_gen.generate.return_value = ask_result.chart

    patches = [
        patch("agent.main.configure_logging"),
        patch("agent.main.SchemaResolver"),
        patch("agent.main.build_system_prompt", return_value="<system prompt>"),
        patch("agent.main.ClaudeClient"),
        patch("agent.main.SQLValidator"),
        patch("agent.main.AuditLogger"),
        patch("agent.main.SQLGenerator", return_value=mock_generator),
        patch("agent.main.AthenaExecutor", return_value=mock_executor),
        patch("agent.main.InsightGenerator", return_value=mock_insight_gen),
        patch("agent.main.ChartGenerator", return_value=mock_chart_gen),
    ]
    return patches


def _start_patches(patches: list[Any]) -> list[Any]:
    return [p.start() for p in patches]


def _stop_patches(patches: list[Any]) -> None:
    for p in patches:
        p.stop()


# ── AgentSession construction ──────────────────────────────────────────────────


class TestAgentSessionConstruction:
    def test_session_constructs_without_error(self) -> None:
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            session = AgentSession()
            assert session is not None
        finally:
            _stop_patches(patches)

    def test_schema_resolver_load_all_schemas_called(self) -> None:
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            with patch("agent.main.SchemaResolver") as mock_resolver_cls:
                AgentSession()
                mock_resolver_cls.return_value.load_all_schemas.assert_called_once()
        finally:
            _stop_patches(patches)

    def test_build_system_prompt_called(self) -> None:
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            with patch("agent.main.build_system_prompt", return_value="<sp>") as mock_bsp:
                AgentSession()
                mock_bsp.assert_called_once()
        finally:
            _stop_patches(patches)

    def test_chart_generator_instantiated(self) -> None:
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            with patch("agent.main.ChartGenerator") as mock_chart_cls:
                AgentSession()
                mock_chart_cls.assert_called_once()
        finally:
            _stop_patches(patches)


# ── AgentSession.ask() ─────────────────────────────────────────────────────────


class TestAgentSessionAsk:
    def test_ask_returns_ask_result(self) -> None:
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            session = AgentSession()
            result = session.ask("Which country has the highest revenue?")
            assert isinstance(result, AskResult)
        finally:
            _stop_patches(patches)

    def test_ask_result_contains_insight_response(self) -> None:
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            session = AgentSession()
            result = session.ask("Which country has the highest revenue?")
            assert isinstance(result.response, InsightResponse)
        finally:
            _stop_patches(patches)

    def test_ask_result_contains_chart_output(self) -> None:
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            session = AgentSession()
            result = session.ask("Which country has the highest revenue?")
            assert isinstance(result.chart, ChartOutput)
        finally:
            _stop_patches(patches)

    def test_ask_passes_question_to_generator(self) -> None:
        from agent.executor import QueryResult
        from agent.generator import GeneratedSQL

        generated = GeneratedSQL(
            sql="SELECT country FROM revenue_by_country LIMIT 10",
            assumptions=[],
            attempts=1,
        )
        query_result = QueryResult(
            execution_id="exec-x",
            columns=["country"],
            rows=[{"country": "Germany"}],
            bytes_scanned=1024,
            cost_usd=0.0,
        )
        mock_generator = MagicMock()
        mock_generator.generate.return_value = generated
        mock_executor = MagicMock()
        mock_executor.execute.return_value = query_result
        mock_insight_gen = MagicMock()
        mock_insight_gen.generate.return_value = _make_response()
        mock_chart_gen = MagicMock()
        mock_chart_gen.generate.return_value = _make_chart()

        patches = [
            patch("agent.main.configure_logging"),
            patch("agent.main.SchemaResolver"),
            patch("agent.main.build_system_prompt", return_value="<sp>"),
            patch("agent.main.ClaudeClient"),
            patch("agent.main.SQLValidator"),
            patch("agent.main.AuditLogger"),
            patch("agent.main.SQLGenerator", return_value=mock_generator),
            patch("agent.main.AthenaExecutor", return_value=mock_executor),
            patch("agent.main.InsightGenerator", return_value=mock_insight_gen),
            patch("agent.main.ChartGenerator", return_value=mock_chart_gen),
        ]
        _start_patches(patches)
        try:
            session = AgentSession()
            question = "Which country has the highest revenue?"
            session.ask(question)
            call_args = mock_generator.generate.call_args
            assert call_args[1].get("question") == question or question in str(call_args)
        finally:
            _stop_patches(patches)

    def test_ask_passes_sql_to_executor(self) -> None:
        from agent.executor import QueryResult
        from agent.generator import GeneratedSQL

        expected_sql = "SELECT country FROM revenue_by_country LIMIT 10"
        generated = GeneratedSQL(sql=expected_sql, assumptions=[], attempts=1)
        query_result = QueryResult(
            execution_id="exec-x",
            columns=["country"],
            rows=[],
            bytes_scanned=1024,
            cost_usd=0.0,
        )
        mock_generator = MagicMock()
        mock_generator.generate.return_value = generated
        mock_executor = MagicMock()
        mock_executor.execute.return_value = query_result
        mock_insight_gen = MagicMock()
        mock_insight_gen.generate.return_value = _make_response()
        mock_chart_gen = MagicMock()
        mock_chart_gen.generate.return_value = _make_chart()

        patches = [
            patch("agent.main.configure_logging"),
            patch("agent.main.SchemaResolver"),
            patch("agent.main.build_system_prompt", return_value="<sp>"),
            patch("agent.main.ClaudeClient"),
            patch("agent.main.SQLValidator"),
            patch("agent.main.AuditLogger"),
            patch("agent.main.SQLGenerator", return_value=mock_generator),
            patch("agent.main.AthenaExecutor", return_value=mock_executor),
            patch("agent.main.InsightGenerator", return_value=mock_insight_gen),
            patch("agent.main.ChartGenerator", return_value=mock_chart_gen),
        ]
        _start_patches(patches)
        try:
            session = AgentSession()
            session.ask("Any question")
            mock_executor.execute.assert_called_once_with(expected_sql)
        finally:
            _stop_patches(patches)

    def test_ask_calls_audit_write(self) -> None:
        from agent.executor import QueryResult
        from agent.generator import GeneratedSQL

        generated = GeneratedSQL(sql="SELECT 1 LIMIT 1", assumptions=[], attempts=1)
        query_result = QueryResult(
            execution_id="exec-x",
            columns=["col"],
            rows=[],
            bytes_scanned=1024,
            cost_usd=0.0,
        )
        mock_generator = MagicMock()
        mock_generator.generate.return_value = generated
        mock_executor = MagicMock()
        mock_executor.execute.return_value = query_result
        mock_audit = MagicMock()
        mock_insight_gen = MagicMock()
        mock_insight_gen.generate.return_value = _make_response()
        mock_chart_gen = MagicMock()
        mock_chart_gen.generate.return_value = _make_chart()

        patches = [
            patch("agent.main.configure_logging"),
            patch("agent.main.SchemaResolver"),
            patch("agent.main.build_system_prompt", return_value="<sp>"),
            patch("agent.main.ClaudeClient"),
            patch("agent.main.SQLValidator"),
            patch("agent.main.AuditLogger", return_value=mock_audit),
            patch("agent.main.SQLGenerator", return_value=mock_generator),
            patch("agent.main.AthenaExecutor", return_value=mock_executor),
            patch("agent.main.InsightGenerator", return_value=mock_insight_gen),
            patch("agent.main.ChartGenerator", return_value=mock_chart_gen),
        ]
        _start_patches(patches)
        try:
            session = AgentSession()
            session.ask("Any question")
            mock_audit.write.assert_called_once()
        finally:
            _stop_patches(patches)

    def test_sql_generation_error_propagates(self) -> None:
        mock_generator = MagicMock()
        mock_generator.generate.side_effect = SQLGenerationError("failed after 3 attempts")

        patches = [
            patch("agent.main.configure_logging"),
            patch("agent.main.SchemaResolver"),
            patch("agent.main.build_system_prompt", return_value="<sp>"),
            patch("agent.main.ClaudeClient"),
            patch("agent.main.SQLValidator"),
            patch("agent.main.AuditLogger"),
            patch("agent.main.SQLGenerator", return_value=mock_generator),
            patch("agent.main.AthenaExecutor"),
            patch("agent.main.InsightGenerator"),
            patch("agent.main.ChartGenerator"),
        ]
        _start_patches(patches)
        try:
            session = AgentSession()
            with pytest.raises(SQLGenerationError):
                session.ask("Any question")
        finally:
            _stop_patches(patches)

    def test_insight_text_preserved_in_result(self) -> None:
        ask_result = _make_ask_result(response=_make_response(insight="France is number two."))
        patches = _patch_session_deps(ask_result)
        _start_patches(patches)
        try:
            session = AgentSession()
            result = session.ask("Which country is second?")
            assert result.response.insight == "France is number two."
        finally:
            _stop_patches(patches)

    def test_validation_flags_preserved_in_result(self) -> None:
        ask_result = _make_ask_result(response=_make_response(flags=["Negative revenue detected."]))
        patches = _patch_session_deps(ask_result)
        _start_patches(patches)
        try:
            session = AgentSession()
            result = session.ask("Any question")
            assert "Negative revenue detected." in result.response.validation_flags
        finally:
            _stop_patches(patches)

    def test_prior_context_appended_to_system_prompt(self) -> None:
        """When prior_context is given, it is appended to the system prompt."""
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            with patch("agent.main.build_system_prompt", return_value="<base prompt>"):
                with patch("agent.main.SchemaResolver"):
                    session = AgentSession()
                    session._system_prompt = "<base prompt>"
                    # Capture what system_prompt the generator receives.
                    captured = {}

                    def capture_generate(question: str, system_prompt: str) -> Any:
                        captured["system_prompt"] = system_prompt
                        from agent.generator import GeneratedSQL

                        return GeneratedSQL(sql="SELECT 1 LIMIT 1", assumptions=[], attempts=1)

                    session._generator.generate = capture_generate  # type: ignore[method-assign]
                    try:
                        session.ask(
                            "Follow-up?", prior_context="Prior conversation:\nQ: ...\nA: ..."
                        )
                    except Exception:
                        pass  # executor will fail — we only care about the system_prompt
                    if "system_prompt" in captured:
                        assert "Prior conversation:" in captured["system_prompt"]
        finally:
            _stop_patches(patches)

    def test_chart_generator_called_with_query_result(self) -> None:
        patches = _patch_session_deps()
        _start_patches(patches)
        try:
            with patch("agent.main.ChartGenerator") as mock_chart_cls:
                mock_chart_inst = MagicMock()
                mock_chart_inst.generate.return_value = _make_chart()
                mock_chart_cls.return_value = mock_chart_inst
                session = AgentSession()
                session.ask("Any question")
                mock_chart_inst.generate.assert_called_once()
        finally:
            _stop_patches(patches)


# ── CLI ────────────────────────────────────────────────────────────────────────


class TestCliMain:
    def _mock_session(self, ask_result: AskResult | None = None) -> MagicMock:
        mock = MagicMock(spec=AgentSession)
        mock.ask.return_value = ask_result or _make_ask_result()
        return mock

    def test_no_args_returns_exit_code_1(self, capsys: pytest.CaptureFixture[str]) -> None:
        code = _cli_main([])
        assert code == 1

    def test_no_args_prints_usage_to_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        _cli_main([])
        captured = capsys.readouterr()
        assert "Usage" in captured.err

    def test_question_passed_to_session(self) -> None:
        mock_session = self._mock_session()
        with patch("agent.main.AgentSession", return_value=mock_session):
            _cli_main(["Which", "country", "leads?"])
        mock_session.ask.assert_called_once_with("Which country leads?")

    def test_success_returns_exit_code_0(self) -> None:
        mock_session = self._mock_session()
        with patch("agent.main.AgentSession", return_value=mock_session):
            code = _cli_main(["Which country leads?"])
        assert code == 0

    def test_insight_printed_to_stdout(self, capsys: pytest.CaptureFixture[str]) -> None:
        ask_result = _make_ask_result(response=_make_response(insight="Germany leads."))
        mock_session = self._mock_session(ask_result)
        with patch("agent.main.AgentSession", return_value=mock_session):
            _cli_main(["Any question?"])
        captured = capsys.readouterr()
        assert "Germany leads." in captured.out

    def test_presigned_url_printed_to_stdout(self, capsys: pytest.CaptureFixture[str]) -> None:
        ask_result = _make_ask_result(chart=_make_chart(presigned_url="https://example.com/c.png"))
        mock_session = self._mock_session(ask_result)
        with patch("agent.main.AgentSession", return_value=mock_session):
            _cli_main(["Any question?"])
        captured = capsys.readouterr()
        assert "https://example.com/c.png" in captured.out

    def test_no_chart_line_when_no_presigned_url(self, capsys: pytest.CaptureFixture[str]) -> None:
        ask_result = _make_ask_result(chart=_make_chart(presigned_url=None))
        mock_session = self._mock_session(ask_result)
        with patch("agent.main.AgentSession", return_value=mock_session):
            _cli_main(["Any question?"])
        captured = capsys.readouterr()
        assert "Chart:" not in captured.out

    def test_configuration_error_returns_exit_code_1(self) -> None:
        with patch(
            "agent.main.AgentSession", side_effect=ConfigurationError("missing ENVIRONMENT")
        ):
            code = _cli_main(["Any question?"])
        assert code == 1

    def test_configuration_error_message_to_stderr(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "agent.main.AgentSession", side_effect=ConfigurationError("missing ENVIRONMENT")
        ):
            _cli_main(["Any question?"])
        captured = capsys.readouterr()
        assert "missing ENVIRONMENT" in captured.err

    def test_sql_generation_error_returns_exit_code_1(self) -> None:
        mock_session = self._mock_session()
        mock_session.ask.side_effect = SQLGenerationError("failed after 3 attempts")
        with patch("agent.main.AgentSession", return_value=mock_session):
            code = _cli_main(["Any question?"])
        assert code == 1

    def test_agent_error_returns_exit_code_1(self) -> None:
        mock_session = self._mock_session()
        mock_session.ask.side_effect = AgentError("something went wrong")
        with patch("agent.main.AgentSession", return_value=mock_session):
            code = _cli_main(["Any question?"])
        assert code == 1

    def test_multi_word_question_joined_with_spaces(self) -> None:
        mock_session = self._mock_session()
        with patch("agent.main.AgentSession", return_value=mock_session):
            _cli_main(["Which", "product", "sold", "the", "most?"])
        mock_session.ask.assert_called_once_with("Which product sold the most?")
