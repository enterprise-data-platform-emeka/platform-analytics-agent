.PHONY: setup lint typecheck test test-integration run clean

# Set up local development environment
setup:
	pyenv install --skip-existing
	python -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements-dev.txt
	@echo "Setup complete. Activate with: source .venv/bin/activate"

# Lint all Python source and test files
lint:
	.venv/bin/ruff check agent/ tests/
	.venv/bin/ruff format --check agent/ tests/

# Run static type checks
typecheck:
	.venv/bin/mypy agent/ tests/

# Run unit tests only (no AWS calls)
test:
	.venv/bin/pytest tests/ -v

# Run integration tests against real AWS dev environment
# Requires active dev-admin credentials and deployed infrastructure
test-integration:
	.venv/bin/pytest tests/ -v -m integration

# Run the agent CLI against a question (requires AWS credentials and deployed infra)
# Usage: make run Q="Show total orders by country"
run:
	.venv/bin/python -m agent.main "$(Q)"

# Remove generated files
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	@echo "Clean complete."
