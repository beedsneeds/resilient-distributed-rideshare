FROM golang:1.25

WORKDIR /build

COPY services/reconciler/go.mod services/reconciler/go.sum services/reconciler/
COPY services/ride/go.mod services/ride/go.sum services/ride/
COPY services/matching/go.mod services/matching/go.sum services/matching/

RUN go work init ./services/reconciler ./services/ride ./services/matching

RUN cd services/ride && GOWORK=off go mod download
RUN cd services/matching && GOWORK=off go mod download
RUN cd services/reconciler && GOWORK=off go mod download

COPY services/reconciler/ services/reconciler/
COPY services/ride/ services/ride/
COPY services/matching/ services/matching/

RUN go build -o /reconciler ./services/reconciler/.

CMD ["/reconciler"]

# diverging from standard container copy because I always have problems with 
# stale pinned commits of its dependency services
