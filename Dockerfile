# syntax=docker/dockerfile:1

# The build stage runs on the build platform and cross-compiles for the target platform, so a multi-platform build emulates nothing.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.work go.mod go.sum ./
COPY redis/go.mod redis/go.sum ./redis/
RUN cd redis && GOWORK=/src/go.work go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
WORKDIR /src/redis
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOWORK=/src/go.work go build -trimpath -ldflags "-s -w -X github.com/ok0/go-microgateway/internal/cli.version=${VERSION} -X github.com/ok0/go-microgateway/internal/cli.commit=${COMMIT} -X github.com/ok0/go-microgateway/internal/cli.date=${DATE}" -o /out/microgateway ./cmd/microgateway

# The proxy port is the listener address of the mounted config (8080 in the examples).
# The probe port is 9902: /livez and /readyz on all interfaces, without a token.
# The management port (admin.address, default 127.0.0.1:9901) binds to loopback and is not exposed; a non-loopback admin.address needs admin.auth.keys, or admin.auth.management: none.
# The configuration is mounted at /etc/microgateway/config.yaml.
# The user is the distroless nonroot user, given numerically so that a non-root check can verify it.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/microgateway /microgateway
USER 65532:65532
EXPOSE 8080 9902
ENTRYPOINT ["/microgateway"]
CMD ["run", "-config", "/etc/microgateway/config.yaml"]
