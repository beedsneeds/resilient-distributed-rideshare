FROM golang:1.25

WORKDIR /build

COPY services/ride/go.mod go.mod
COPY services/ride/go.sum go.sum
RUN go mod download

COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.38 /ko-app/grpc-health-probe /bin/grpc_health_probe

COPY services/ride/ .

RUN go build -o /ride .

EXPOSE 50051

CMD ["/ride"]
