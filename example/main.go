package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/goptics/duckq"
)

// regularQueueExample demonstrates the basic queue functionality
func regularQueueExample() {
	// Create a new DuckDB queue
	dbPath := "./regular_queue_example.db"
	defer os.Remove(dbPath)

	queues := duckq.New(dbPath)
	queue, err := queues.NewQueue("tasks")
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer queues.Close()

	fmt.Println("DuckQ Example")
	fmt.Println("---------------")

	// Add some items to the queue
	fmt.Println("Adding items to the queue...")
	queue.Enqueue([]byte("Task 1"))
	queue.Enqueue([]byte("Task 2"))
	queue.Enqueue([]byte("Task 3"))

	// Print queue length
	fmt.Printf("Queue length: %d\n", queue.Len())

	// Print all values
	fmt.Println("\nAll pending items:")
	for i, item := range queue.Values() {
		fmt.Printf("  %d: %v\n", i+1, string(item.([]byte)))
	}

	// Dequeue an item with acknowledgment
	fmt.Println("\nDequeuing an item with acknowledgment...")
	item, success, ackID := queue.DequeueWithAckId()
	if success {
		fmt.Printf("Dequeued: %v\n", string(item.([]byte)))
		fmt.Printf("Acknowledgment ID: %s\n", ackID)

		// Simulate processing the item
		fmt.Println("Processing item...")
		time.Sleep(1 * time.Second)

		// Acknowledge the item
		if queue.Acknowledge(ackID) {
			fmt.Println("Item successfully acknowledged!")
		} else {
			fmt.Println("Failed to acknowledge item!")
		}
	}

	// Simple dequeue
	fmt.Println("\nPerforming a simple dequeue...")
	item, success = queue.Dequeue()
	if success {
		fmt.Printf("Dequeued: %v\n", string(item.([]byte)))
	}

	// Check queue length again
	fmt.Printf("\nQueue length after dequeuing: %d\n", queue.Len())

	// Purge the queue
	fmt.Println("\nPurging the queue...")
	queue.Purge()
	fmt.Printf("Queue length after purge: %d\n", queue.Len())

	fmt.Println("\nRegular queue example completed!")
}

func main() {
	// Show menu
	fmt.Println("DuckQ Examples")
	fmt.Println("---------------")
	fmt.Println("1. Regular Queue Example")
	fmt.Println("2. Priority Queue Example")

	// Default to regular queue example
	var choice string
	fmt.Print("Choose an example (default is 1): ")
	fmt.Scanln(&choice)

	fmt.Println()

	switch choice {
	case "2":
		priorityQueueExample()
	default:
		regularQueueExample()
	}
}
