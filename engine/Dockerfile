# syntax=docker/dockerfile:1
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY CMakeLists.txt ./
COPY engine/ engine/

# /src/build is a cache mount, so it never lands in the image layer; the
# binary is copied out to a real path before the mount is torn down.
RUN --mount=type=cache,target=/src/build \
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --target engine_main -j "$(nproc)" && \
    cp build/engine_main /usr/local/bin/engine-bin

FROM debian:bookworm-slim AS runtime

RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin engine

COPY --from=builder /usr/local/bin/engine-bin /usr/local/bin/engine

USER 10001
ENTRYPOINT ["engine"]
