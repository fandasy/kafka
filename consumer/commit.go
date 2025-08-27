package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type CommitManager struct {
	mu     sync.Mutex
	reader *kafka.Reader
	parts  map[int]*partState // partition → state
	ct     time.Duration      // commit timeout
	cr     int                // commit retry
}

type partState struct {
	nextCommit  int64
	initialized bool
	done        map[int64]struct{}
	lastTopic   string
}

func NewOffsetManager(r *kafka.Reader, commitTimeout time.Duration, commitRetry int) *CommitManager {
	if commitTimeout <= 0 {
		commitTimeout = 2 * time.Second
	}
	if commitRetry < 0 {
		commitRetry = 0
	}

	return &CommitManager{
		reader: r,
		parts:  make(map[int]*partState),
		ct:     commitTimeout,
		cr:     commitRetry,
	}
}

func (cm *CommitManager) Track(m kafka.Message) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	ps := cm.part(m)
	if !ps.initialized {
		ps.initialized = true
		ps.nextCommit = m.Offset
	}

	if m.Offset < ps.nextCommit {
		ps.nextCommit = m.Offset
	}
}

func (cm *CommitManager) Ack(appCtx context.Context, m kafka.Message) error {
	cm.mu.Lock()

	ps := cm.part(m)
	if !ps.initialized {
		ps.initialized = true
		ps.nextCommit = m.Offset
	}

	if ps.done == nil {
		ps.done = make(map[int64]struct{})
	}

	ps.done[m.Offset] = struct{}{}

	last := ps.nextCommit - 1
	for {
		if _, ok := ps.done[ps.nextCommit]; ok {
			last = ps.nextCommit
			delete(ps.done, ps.nextCommit)
			ps.nextCommit++

			continue
		}
		break
	}

	if last < 0 || last < ps.nextCommit-1 {
		cm.mu.Unlock()
		return nil
	}

	partition, topic := m.Partition, m.Topic
	if ps.lastTopic != "" {
		topic = ps.lastTopic
	} else {
		ps.lastTopic = topic
	}

	cm.mu.Unlock()

	return cm.commit(appCtx, topic, partition, last)
}

func (cm *CommitManager) AckPoison(appCtx context.Context, m kafka.Message) error {
	return cm.Ack(appCtx, m)
}

func (cm *CommitManager) part(m kafka.Message) *partState {
	ps, ok := cm.parts[m.Partition]
	if !ok {
		ps = &partState{
			done:      make(map[int64]struct{}),
			lastTopic: m.Topic,
		}

		cm.parts[m.Partition] = ps
	}

	return ps
}

func (cm *CommitManager) commit(appCtx context.Context, topic string, partition int, last int64) error {
	msg := kafka.Message{
		Topic:     topic,
		Partition: partition,
		Offset:    last,
	}

	var err error
	for i := 0; i <= cm.cr; i++ {
		ctx, cancel := context.WithTimeout(appCtx, cm.ct)
		err = cm.reader.CommitMessages(ctx, msg)
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

		log.Printf("[offset-manager] commit failed (p=%d last=%d): %v", partition, last, err)

		return fmt.Errorf("commit failed (p=%d last=%d): %w", partition, last, err)
	}

	return fmt.Errorf("commit retry exceeded for p=%d last=%d err=%w", partition, last, err)
}
