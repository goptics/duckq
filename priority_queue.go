package duckq

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/lucsky/cuid"
)

// PriorityQueue extends Queue with priority-based dequeuing
type PriorityQueue struct {
	*Queue
}

// createPriorityTable creates a table with a priority column for a priority queue
func createPriorityTable(db *sql.DB, tableName string) error {
	// First create a sequence for auto-incrementing IDs if it doesn't exist
	seqName := fmt.Sprintf("%s_id_seq", tableName)
	createSeqSQL := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s START 1;", seqName)
	_, err := db.Exec(createSeqSQL)
	if err != nil {
		return err
	}

	// Create the table with a priority column included from the start
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY DEFAULT nextval('%s'),
		data BLOB NOT NULL,
		status TEXT NOT NULL,
		ack_id TEXT UNIQUE,
		ack BOOLEAN DEFAULT 0,
		created_at TIMESTAMP,
		updated_at TIMESTAMP,
		priority INTEGER NOT NULL DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS %s_status_idx ON %s (status, created_at);
	CREATE INDEX IF NOT EXISTS %s_status_ack_idx ON %s (status, ack);
	CREATE INDEX IF NOT EXISTS %s_ack_id_idx ON %s (ack_id);
	CREATE INDEX IF NOT EXISTS %s_priority_idx ON %s (priority ASC, created_at ASC);
	`, tableName, seqName, tableName, tableName, tableName, tableName, tableName, tableName, tableName, tableName)

	_, err = db.Exec(createTableSQL)
	return err
}

// newPriorityQueue creates a new DuckDB-based priority queue
func newPriorityQueue(db *sql.DB, tableName string, opts ...Option) (*PriorityQueue, error) {
	// Create the queue with a priority column included
	q := &Queue{
		client:           db,
		tableName:        tableName,
		removeOnComplete: true, // Default to removing completed items
	}

	// Apply any provided options
	for _, opt := range opts {
		opt(q)
	}

	// Initialize the table with priority column
	if err := createPriorityTable(db, tableName); err != nil {
		return nil, fmt.Errorf("failed to initialize table: %w", err)
	}

	q.RequeueNoAckRows()

	pq := &PriorityQueue{
		Queue: q,
	}

	return pq, nil
}

// No need for initPriorityColumn anymore since we create the table with priority column from the start

// Enqueue adds an item to the queue with a specified priority
// Lower priority numbers will be dequeued first (0 is highest priority)
// Returns true if the operation was successful
func (pq *PriorityQueue) Enqueue(item any, priority int) bool {
	if pq.closed.Load() {
		return false
	}

	now := time.Now().UTC()
	tx, err := pq.client.Begin()
	if err != nil {
		return false
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(
		fmt.Sprintf("INSERT INTO %s (data, status, created_at, updated_at, priority) VALUES (?, ?, ?, ?, ?)", pq.tableName),
		item, "pending", now, now, priority,
	)
	if err != nil {
		tx.Rollback()
		return false
	}

	err = tx.Commit()
	return err == nil
}

// dequeueInternal overrides the base dequeueInternal method to consider priority
func (pq *PriorityQueue) dequeueInternal(withAckId bool) (any, bool, string) {
	if pq.closed.Load() {
		return nil, false, ""
	}

	tx, err := pq.client.Begin()
	if err != nil {
		return nil, false, ""
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Get the highest priority pending item (lower priority numbers come first)
	var id int64
	var data []byte
	row := tx.QueryRow(fmt.Sprintf(
		"SELECT id, data FROM %s WHERE status = 'pending' ORDER BY priority ASC, created_at ASC LIMIT 1",
		pq.tableName,
	))
	err = row.Scan(&id, &data)
	if err != nil {
		if err == sql.ErrNoRows {
			tx.Rollback()
			return nil, false, ""
		}
		tx.Rollback()
		return nil, false, ""
	}

	// Update the status to 'processing' with ack ID or remove directly if no ack ID
	now := time.Now().UTC()
	var ackID string

	if withAckId {
		ackID = cuid.New()

		_, err = tx.Exec(
			fmt.Sprintf("UPDATE %s SET status = 'processing', ack_id = ?, updated_at = ? WHERE id = ?", pq.tableName),
			ackID, now, id,
		)
	} else {
		// remove the row if there is no ack
		_, err = tx.Exec(
			fmt.Sprintf("DELETE FROM %s WHERE id = ?", pq.tableName),
			id,
		)
	}

	if err != nil {
		tx.Rollback()
		return nil, false, ""
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		return nil, false, ""
	}

	return data, true, ackID
}

// Dequeue overrides the base Dequeue method to use priority-based dequeuing
func (pq *PriorityQueue) Dequeue() (any, bool) {
	item, success, _ := pq.dequeueInternal(false)
	return item, success
}

// DequeueWithAckId overrides the base DequeueWithAckId method to use priority-based dequeuing
func (pq *PriorityQueue) DequeueWithAckId() (any, bool, string) {
	return pq.dequeueInternal(true)
}
