package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const (
	ITERATIONS = "ITERATIONS"
	WRITERS    = "WRITERS"
)

type check struct {
	id  uuid.UUID
	age int
}

func main() {
	_ = godotenv.Load()
	conn := connect()

	ids, err := prepare(conn)
	if err != nil {
		log.Fatal(err)
	}

	update := make(chan check)
	go write(conn, context.Background(), ids, update)
	go read(conn, context.Background(), update)

	runtime.Goexit()
	// wait for goroutines to finish
	close(update)
	disconnect(conn)
}

func write(conn *pgxpool.Pool, ctx context.Context, ids []uuid.UUID, update chan check) {
	var counter uint64
	iter, err := strconv.Atoi(os.Getenv(ITERATIONS))
	if err != nil {
		log.Fatal("writer: ", err)
	}
	workers, err := strconv.Atoi(os.Getenv(WRITERS))
	if err != nil {
		log.Fatal("writer: ", err)
	}
	if workers == 0 {
		workers = 1
	}

	if iter == 0 {
		log.Fatal("writer: ITERATIONS must be greater than 0")
		return
	}

	iterPerWorker := iter / workers
	remainder := iter % workers
	log.Printf("writer: %d iterations per worker, %d workers, %d remainder (nothing done)", iterPerWorker, workers, remainder)

	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			start := iterPerWorker * i
			limit := start + iterPerWorker
			for age := start; age <= limit; age++ {
				ctx := context.Background()
				tx, err := conn.Begin(ctx)
				if err != nil {
					log.Fatal("writer:Failed to start transactions: ", err)
				}
				_, err = tx.Exec(ctx, "UPDATE users SET age = $1 WHERE id = $2", age, ids[i])
				if err != nil {
					log.Fatal("writer: ", err)
					tx.Rollback(ctx)
				} else {
					tx.Commit(ctx)
				}
				atomic.AddUint64(&counter, 1)
				fmt.Printf("\r%d", atomic.LoadUint64(&counter))
				update <- check{age: age, id: ids[i]}
			}
		}(i)
	}
	wg.Wait()
}

func read(conn *pgxpool.Pool, ctx context.Context, update <-chan check) {
	for check := range update {
		go handleRead(conn, ctx, check)
	}
}

func handleRead(conn *pgxpool.Pool, ctx context.Context, check check) {
	var age int
	err := conn.QueryRow(ctx, "SELECT age FROM users WHERE id = $1", check.id).Scan(&age)
	if err != nil {
		log.Fatal("reader: ", err)
	}
	if age < check.age {
		log.Fatalf("expected %d, got %d on worker %d", check.age, age, check.id)
	}
}
