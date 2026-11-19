# syntax=docker/dockerfile:1.7
#
# meili-connect: production image, one binary (`meili-connect`).
#
# Dependencies are compiled in a cargo-chef layer so that editing the
# sources does not rebuild them. The build needs cmake, a C/C++ compiler,
# perl and make: rdkafka is built from source with a vendored OpenSSL, and
# the AWS SDK uses aws-lc.

ARG RUST_VERSION=1.94

# ---------------------------------------------------------------------------
# Tooling
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-bookworm AS chef
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      cmake build-essential perl make pkg-config clang libclang-dev \
 && rm -rf /var/lib/apt/lists/* \
 && cargo install cargo-chef --locked
WORKDIR /app

# ---------------------------------------------------------------------------
# Dependency recipe
# ---------------------------------------------------------------------------
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------------------
# Dependencies, then the binary
# ---------------------------------------------------------------------------
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release -p meili-connect-server --recipe-path recipe.json
COPY . .
RUN cargo build --release -p meili-connect-server --bin meili-connect \
 && install -D target/release/meili-connect /out/meili-connect

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 connect \
 && useradd --system --uid 10001 --gid connect --home-dir /nonexistent --shell /usr/sbin/nologin connect

COPY --from=builder /out/meili-connect /usr/local/bin/meili-connect

USER connect
ENV LISTEN_ADDR=0.0.0.0:8080 \
    RUST_LOG=info
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/meili-connect"]
