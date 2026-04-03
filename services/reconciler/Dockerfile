FROM golang:1.25

WORKDIR /build

COPY services/reconciler/go.mod go.mod
COPY services/reconciler/go.sum go.sum
RUN go mod download

COPY services/reconciler/ .

RUN go build -o /reconciler .

CMD ["/reconciler"]
