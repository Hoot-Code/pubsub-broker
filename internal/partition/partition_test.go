package partition_test

import (
	"sync"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/partition"
)

func TestHashPartitioner_KeyedRouting(t *testing.T) {
	p := partition.NewHashPartitioner()
	_ = p.Register("orders", 8)

	// Same key must always land on the same partition.
	got1, _ := p.Assign("orders", "customer-42")
	got2, _ := p.Assign("orders", "customer-42")
	if got1 != got2 {
		t.Errorf("keyed routing not deterministic: %d vs %d", got1, got2)
	}
	if got1 < 0 || got1 >= 8 {
		t.Errorf("partition %d out of range [0,8)", got1)
	}
}

func TestHashPartitioner_KeylessRoundRobin(t *testing.T) {
	p := partition.NewHashPartitioner()
	_ = p.Register("events", 4)

	seen := make(map[int32]int)
	for i := 0; i < 40; i++ {
		part, err := p.Assign("events", "")
		if err != nil {
			t.Fatalf("Assign: %v", err)
		}
		if part < 0 || part >= 4 {
			t.Fatalf("partition %d out of range [0,4)", part)
		}
		seen[part]++
	}
	// Each partition should receive ~10 messages (balanced).
	for id, count := range seen {
		if count < 5 || count > 15 {
			t.Errorf("partition %d got %d messages — not balanced", id, count)
		}
	}
}

func TestHashPartitioner_UnknownTopic(t *testing.T) {
	p := partition.NewHashPartitioner()
	_, err := p.Assign("nonexistent", "key")
	if err == nil {
		t.Error("expected error for unknown topic")
	}
}

func TestHashPartitioner_Deregister(t *testing.T) {
	p := partition.NewHashPartitioner()
	_ = p.Register("tmp", 4)
	_ = p.Register("tmp", 0) // deregister

	if c := p.TopicPartitionCount("tmp"); c != 0 {
		t.Errorf("count after deregister: want 0, got %d", c)
	}
}

func TestHashPartitioner_ConcurrentAssign(t *testing.T) {
	p := partition.NewHashPartitioner()
	_ = p.Register("concurrent", 16)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := p.Assign("concurrent", "")
				if err != nil {
					t.Errorf("concurrent Assign: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestOffsetStore_CommitAndLoad(t *testing.T) {
	s := partition.NewOffsetStore()
	s.Commit("grp1", "topic-a", 0, 42)
	s.Commit("grp1", "topic-a", 1, 17)

	if got := s.Load("grp1", "topic-a", 0); got != 42 {
		t.Errorf("offset[0]: want 42, got %d", got)
	}
	if got := s.Load("grp1", "topic-a", 1); got != 17 {
		t.Errorf("offset[1]: want 17, got %d", got)
	}
	if got := s.Load("grp1", "topic-a", 2); got != -1 {
		t.Errorf("missing offset: want -1, got %d", got)
	}
}

func TestOffsetStore_Snapshot(t *testing.T) {
	s1 := partition.NewOffsetStore()
	s1.Commit("g", "t", 0, 100)
	snap := s1.Snapshot()

	s2 := partition.NewOffsetStore()
	s2.Restore(snap)
	if got := s2.Load("g", "t", 0); got != 100 {
		t.Errorf("restore: want 100, got %d", got)
	}
}

func BenchmarkHashPartitioner_Assign(b *testing.B) {
	p := partition.NewHashPartitioner()
	_ = p.Register("bench", 32)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = p.Assign("bench", "some-routing-key")
		}
	})
}
