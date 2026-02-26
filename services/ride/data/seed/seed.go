package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:5432/ride",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOSTNAME"),
	)
	seedCSV := "driver.csv"
	tableName := "driver"

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := seedFromCSV(ctx, pool, seedCSV, tableName); err != nil {
		log.Fatal(err)
	}
}

func seedFromCSV(ctx context.Context, pool *pgxpool.Pool, filename string, tableName string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	// First row is headers — should match DB column name
	headers := records[0]
	dataRows := records[1:]

	rows := make([][]any, len(dataRows))
	for i, record := range dataRows {
		row := make([]any, len(record))
		for j, val := range record {
			row[j] = val
		}
		rows[i] = row
	}

	// Only specify the columns present in your CSV. DEFAULT for any omitted columns
	copyCount, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		headers,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return err
	}

	log.Printf("Inserted %d rows", copyCount)
	return nil
}
