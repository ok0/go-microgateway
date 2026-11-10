# syntax=docker/dockerfile:1

FROM golang:1.27 AS build
WORKDIR /src
COPY go.work go.mod go.sum ./
COPY redis/go.mod redis/go.sum ./redis/
RUN cd redis && GOWORK=/src/go.work go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
WORKDIR /src/redis
RUN CGO_ENABLED=0 GOWORK=/src/go.work go build -trimpath -ldflags "-s -w -X github.com/ok0/go-microgateway/internal/cli.version=${VERSION} -X github.com/ok0/go-microgateway/internal/cli.commit=${COMMIT} -X github.com/ok0/go-microgateway/internal/cli.date=${DATE}" -o /out/microgateway ./cmd/microgateway

# The proxy port is the listener address of the mounted config (8080 in the examples).
# The admin port is 9901 and binds to loopback by default; a non-loopback admin.address such as 0.0.0.0:9901 needs admin.auth.keys, or admin.auth.management: none.
# The configuration is mounted at /etc/microgateway/config.yaml.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/microgateway /microgateway
USER nonroot
EXPOSE 8080 9901
ENTRYPOINT ["/microgateway"]
CMD ["run", "-config", "/etc/microgateway/config.yaml"]
