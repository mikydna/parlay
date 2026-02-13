.PHONY: fmt lint type test

fmt:
	uv run ruff format .

lint:
	uv run ruff check .

type:
	uv run pyright

test:
	uv run pytest
