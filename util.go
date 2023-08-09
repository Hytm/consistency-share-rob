package main

import (
	"context"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DB = "DB"
)

func connect() *pgxpool.Pool {
	conn, err := pgxpool.New(context.Background(), os.Getenv(DB))
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
	if err != nil {
		log.Fatal("failed to connect database", err)
	}
	return conn
}

func disconnect(conn *pgxpool.Pool) {
	conn.Close()
}

func prepare(conn *pgxpool.Pool) (uuid.UUID, error) {
	var id uuid.UUID
	_, err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS users")
	if err != nil {
		return id, err
	}

	_, err = conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT, age INT)")
	if err != nil {
		return id, err
	}

	err = conn.QueryRow(context.Background(), "INSERT INTO users (name, age) VALUES ('John Doe', 0) RETURNING id").Scan(&id)
	if err != nil {
		return id, err
	}
	return id, nil
}
