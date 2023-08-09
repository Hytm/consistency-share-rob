package main

import (
	"context"
	"log"
	"os"
	"strconv"

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
	return conn
}

func disconnect(conn *pgxpool.Pool) {
	conn.Close()
}

func prepare(conn *pgxpool.Pool) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0)
	_, err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS users")
	if err != nil {
		return ids, err
	}

	_, err = conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT, age INT)")
	if err != nil {
		return ids, err
	}

	//Create a user per worker
	workers, err := strconv.Atoi(os.Getenv(WRITERS))
	if err != nil {
		log.Fatal("writer: ", err)
	}
	if workers == 0 {
		workers = 1
	}

	var id uuid.UUID
	for i := 0; i < workers; i++ {
		name := "John Doe " + strconv.Itoa(i)
		err = conn.QueryRow(context.Background(), "INSERT INTO users (name, age) VALUES ($1, 0) RETURNING id", name).Scan(&id)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)

	}
	return ids, nil
}
