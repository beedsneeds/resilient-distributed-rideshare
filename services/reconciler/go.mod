module github.com/beedsneeds/resilient-distributed-rideshare/services/reconciler

go 1.25.8

require (
	github.com/beedsneeds/resilient-distributed-rideshare/services/matching v0.0.0-20260404012228-ced8cbd5f9d9
	github.com/beedsneeds/resilient-distributed-rideshare/services/ride v0.0.0-20260404012228-ced8cbd5f9d9
	github.com/jackc/pgx/v5 v5.9.1
	github.com/redis/go-redis/v9 v9.18.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.35.0 // indirect
)
