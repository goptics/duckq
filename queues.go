package duckq

import (
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type queues struct {
	client *sql.DB
}

type Queues interface {
	NewQueue(queueKey string, opts ...Option) (*Queue, error)
	NewPriorityQueue(queueKey string, opts ...Option) (*PriorityQueue, error)
	Close() error
}

func New(dbPath string) Queues {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		panic(fmt.Sprintf("failed to open database: %v", err))
	}

	// DuckDB auto-configures optimization settings
	// No need for WAL mode configuration as in SQLite

	return &queues{
		client: db,
	}
}

func (q *queues) NewQueue(queueKey string, opts ...Option) (*Queue, error) {
	return newQueue(q.client, queueKey, opts...)
}

func (q *queues) NewPriorityQueue(queueKey string, opts ...Option) (*PriorityQueue, error) {
	return newPriorityQueue(q.client, queueKey, opts...)
}

func (q *queues) Close() error {
	return q.client.Close()
}
