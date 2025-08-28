package consumer

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log"
	"math"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type MessageHandler interface {
	Handle(ctx context.Context, key, value []byte) error
}

type RetryConfig struct {
	MaxAttempts       int
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
	PerMessageTimeout time.Duration
}

type Config struct {
	Brokers       []string
	Topic         string
	GroupID       string
	Workers       int
	GraceTimeout  time.Duration
	CommitTimeout time.Duration
	CommitRetry   int
	DLQ           *DLQ
}

type Consumer struct {
	reader  *kafka.Reader
	handler MessageHandler
	retry   RetryConfig
	dlq     *DLQ
	om      *CommitManager
	workers int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(c Config, h MessageHandler, r RetryConfig) *Consumer {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = 3
	}
	if r.BaseBackoff <= 0 {
		r.BaseBackoff = 100 * time.Millisecond
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = 10 * time.Second
	}
	if r.PerMessageTimeout <= 0 {
		r.PerMessageTimeout = 5 * time.Second
	}
	if c.Workers <= 0 {
		c.Workers = 1
	}
	if c.GraceTimeout <= 0 {
		c.GraceTimeout = 15 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        c.GroupID,
		Topic:          c.Topic,
		CommitInterval: 0,
		MinBytes:       10e3,
		MaxBytes:       10e6,
	})

	om := NewOffsetManager(reader, c.CommitTimeout, c.CommitRetry)

	return &Consumer{
		reader:  reader,
		handler: h,
		retry:   r,
		dlq:     c.DLQ,
		om:      om,
		workers: c.Workers,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (c *Consumer) Start() error {
	sema := make(chan struct{}, c.workers)

	for {
		msg, err := c.reader.FetchMessage(c.ctx)
		if err != nil {
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				return nil
			default:
				return err
			}
		}

		log.Printf("Fetch new message")

		c.om.Track(msg)

		select {
		case sema <- struct{}{}:
			// ok
		case <-c.ctx.Done():
			return nil
		}

		c.wg.Add(1)
		go func(m kafka.Message) {
			defer func() {
				<-sema
				c.wg.Done()
			}()

			c.processOne(m)
		}(msg)
	}
}

func (c *Consumer) processOne(m kafka.Message) {

	var err error
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {

		ctx, cancel := context.WithTimeout(context.Background(), c.retry.PerMessageTimeout)
		err = c.handler.Handle(ctx, m.Key, m.Value)
		cancel()

		if err == nil { // success
			_ = c.om.Ack(c.ctx, m)
			return
		}

		var ce CErr
		if errors.As(err, &ce) && ce.Kind() == Fatal {
			c.handlePoison(m)
			return
		}

		if attempt < c.retry.MaxAttempts {
			sleep := backoffWithJitter(c.retry.BaseBackoff, c.retry.MaxBackoff, attempt)

			select {
			case <-time.After(sleep):
				// retry
			case <-c.ctx.Done():
				return
			}
		}
	}

	c.handlePoison(m)
}

func (c *Consumer) handlePoison(m kafka.Message) {
	if c.dlq.Enabled() {
		if err := c.dlq.Send(c.ctx, m); err != nil {
			log.Printf("[consumer] DLQ failed, skipping but NOT requeuing (commit anyway to avoid stall): %v", err)
		}
	}

	if err := c.om.AckPoison(c.ctx, m); err != nil {
		log.Printf("[consumer] poison ack failed: %v", err)
	}
}

func (c *Consumer) Stop(ctx context.Context) error {
	if err := c.reader.Close(); err != nil {
		return err
	}

	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-done:
		return nil
	}
}

// backoff + jitter

func backoffWithJitter(base, max time.Duration, attempt int) time.Duration {
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	if exp > float64(max) {
		exp = float64(max)
	}
	r := randUint64()
	j := time.Duration(r % uint64(time.Duration(exp)))
	return j
}

func randUint64() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])
}
