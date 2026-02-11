.PHONY: fmt lint type test ci

fmt:
	uv run ruff format .

lint:
	uv run ruff check .

type:
	uv run pyright

test:
	uv run pytest

ci:
	uv run ruff format --check .
	$(MAKE) lint
	$(MAKE) type
	$(MAKE) test
