FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release

# TLS to object stores needs CA roots, which scratch lacks. Arch-independent, so build natively.
FROM --platform=$BUILDPLATFORM alpine:3 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch AS base
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
EXPOSE 3000
VOLUME /data
ENTRYPOINT ["/objk"]

# CI: package prebuilt binaries from dist/, no compile per platform.
FROM base AS dist
ARG TARGETARCH
COPY --chmod=755 dist/objk-linux-${TARGETARCH} /objk

# Default: self-contained build.
FROM base
COPY --from=build /src/target/release/objk /objk
