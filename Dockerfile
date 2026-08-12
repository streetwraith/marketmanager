# syntax=docker/dockerfile:1
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /marketmanager ./cmd/marketmanager

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /marketmanager /marketmanager
EXPOSE 8080
USER nonroot:nonroot
# The binary probes its own /healthz (no shell or curl in distroless). At least
# one flag is required for some deploy platforms to detect the healthcheck.
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
  CMD ["/marketmanager", "-healthcheck"]
ENTRYPOINT ["/marketmanager"]
