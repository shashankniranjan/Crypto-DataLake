.PHONY: install-dev lint format typecheck test run-once run-live

install-dev:
	python -m pip install -e .[dev]

lint:
	ruff check src tests

format:
	ruff format src tests

typecheck:
	mypy src tests

test:
	pytest

run-once:
	bml run-once

run-live:
	./scripts/run-live.sh
