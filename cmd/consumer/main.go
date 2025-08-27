package main

import (
	"context"
	"github.com/fandasy/kafka/consumer"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type demoHandler struct{}

func (demoHandler) Handle(ctx context.Context, key, value []byte) error {
	// RetryableErr(err), FatalErr(err)
	log.Printf("handle: key=%s value=%s", string(key), string(value))
	return nil
}

func DemoHandler() demoHandler {
	return demoHandler{}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// DLQ
	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP("kafka:9092"),
		Topic:    "test-dlq",
		Balancer: &kafka.LeastBytes{},
	}

	dlq := consumer.NewDLQ(dlqWriter,
		consumer.WithDLQTimeout(3*time.Second),
		consumer.WithDLQAttempts(3),
	)

	c := consumer.New(consumer.Config{
		Brokers:       []string{"kafka:9092"},
		Topic:         "test-topic",
		GroupID:       "test-group",
		Workers:       8,
		GraceTimeout:  20 * time.Second,
		CommitTimeout: 3 * time.Second,
		CommitRetry:   2,
		DLQ:           dlq,
	}, DemoHandler(), consumer.RetryConfig{
		MaxAttempts:       5,
		BaseBackoff:       200 * time.Millisecond,
		MaxBackoff:        10 * time.Second,
		PerMessageTimeout: 5 * time.Second,
	})

	log.Println("Starting")

	go func() {
		if err := c.Start(); err != nil {
			log.Fatalf("consumer error: %v", err)
		}
	}()

	<-ctx.Done()
	if err := c.Stop(context.TODO()); err != nil {
		log.Fatalf("consumer stop error: %v", err)
	}
}
