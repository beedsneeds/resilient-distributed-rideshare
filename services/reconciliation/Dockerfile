FROM golang:1.25

WORKDIR /build

COPY services/reconciliation/go.mod go.mod
COPY services/reconciliation/go.sum go.sum
RUN go mod download

COPY services/reconciliation/ .

RUN go build -o /reconciliation .

CMD ["/reconciliation"]
