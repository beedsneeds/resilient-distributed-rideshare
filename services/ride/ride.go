package ride

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func ride() {
	fmt.Println("Hello")
	DATABASE_URL = "postgres://postgres:postgres@localhost:5432/database_name"
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	// conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
}

func SayHello() {
	fmt.Println("hello from ride")
}
