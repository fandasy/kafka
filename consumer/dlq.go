package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type DLQ struct {
	writer      *kafka.Writer
	timeout     time.Duration
	maxAttempts int
}

type DLQOption func(*DLQ)

func WithDLQTimeout(d time.Duration) DLQOption {
	return func(dq *DLQ) {
		dq.timeout = d
	}
}
func WithDLQAttempts(n int) DLQOption {
	return func(dq *DLQ) {
		dq.maxAttempts = n
	}
}

func NewDLQ(w *kafka.Writer, opts ...DLQOption) *DLQ {
	d := &DLQ{writer: w, timeout: 3 * time.Second, maxAttempts: 3}

	for _, o := range opts {
		o(d)
	}

	return d
}

func (d *DLQ) Enabled() bool {
	return d != nil && d.writer != nil
}

func (d *DLQ) Send(appCtx context.Context, src kafka.Message) error {
	if !d.Enabled() {
		return errors.New("dlq disabled")
	}

	msg := kafka.Message{
		Key:   src.Key,
		Value: src.Value,
		Headers: append(src.Headers,
			kafka.Header{Key: "source-topic", Value: []byte(src.Topic)},
			kafka.Header{Key: "source-partition", Value: []byte(fmt.Sprintf("%d", src.Partition))},
			kafka.Header{Key: "source-offset", Value: []byte(fmt.Sprintf("%d", src.Offset))},
		),
	}

	var err error
	for i := 1; i <= d.maxAttempts; i++ {
		ctx, cancel := context.WithTimeout(appCtx, d.timeout)
		err = d.writer.WriteMessages(ctx, msg)
		cancel()

		if err == nil {
			return nil
		}

		var ke kafka.Error
		if errors.As(err, &ke) && ke.Temporary() {
			select {
			case <-time.After(time.Duration(i+1) * 200 * time.Millisecond):
				// retry
			case <-appCtx.Done():
				return appCtx.Err()
			}

			continue
		}

		return err
	}

	return err
}
