# syntax=docker/dockerfile:1

FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv

# compile .pyc now so the first container start doesn't pay for it;
# copy (not hardlink) because the cache mount is a separate filesystem
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

# dependency layer: invalidated only when the lockfile or metadata changes
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-install-project --no-dev

# project layer: a source edit rebuilds only from this point on
COPY README.md ./
COPY src/ src/
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-dev --no-editable


FROM python:3.14-slim

RUN useradd --create-home --uid 1000 waltz \
  && mkdir /data && chown waltz:waltz /data

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
  WALTZ_CHECKPOINT=/data/waltz.lsn

USER waltz
WORKDIR /app

ENTRYPOINT ["waltz"]
CMD ["stream"]