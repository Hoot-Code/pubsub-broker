package cluster

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

// PartitionMap stores the assignment of topic partitions to cluster nodes.
// All methods are safe for concurrent use.
type PartitionMap struct {
	mu    sync.RWMutex
	owner map[string]map[int32]string // topic → partition → nodeID
}

// Assign distributes the partitions for a topic across the given node list
// using a round-robin strategy. nodes must be sorted for determinism.
// Repeated calls with the same arguments produce identical results.
func (pm *PartitionMap) Assign(topic string, partitions int, nodes []string) {
	if len(nodes) == 0 || partitions <= 0 {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.owner == nil {
		pm.owner = make(map[string]map[int32]string)
	}
	assignment := make(map[int32]string, partitions)
	for i := 0; i < partitions; i++ {
		assignment[int32(i)] = nodes[i%len(nodes)]
	}
	pm.owner[topic] = assignment
}

// Owner returns the NodeID responsible for the given topic partition.
// Returns "" if the topic or partition is not in the map.
func (pm *PartitionMap) Owner(topic string, partition int32) string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if pm.owner == nil {
		return ""
	}
	return pm.owner[topic][partition]
}

// OwnedBy returns all (topic → []partitions) pairs assigned to nodeID.
// The partition slices are sorted ascending.
func (pm *PartitionMap) OwnedBy(nodeID string) map[string][]int32 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	result := make(map[string][]int32)
	for topic, assignment := range pm.owner {
		for partition, owner := range assignment {
			if owner == nodeID {
				result[topic] = append(result[topic], partition)
			}
		}
	}
	for topic := range result {
		sort.Slice(result[topic], func(i, j int) bool {
			return result[topic][i] < result[topic][j]
		})
	}
	return result
}

// ─── JSON serialisation ───────────────────────────────────────────────────────

// partitionMapJSON is the on-wire/on-disk representation of a PartitionMap.
// Partition numbers are stored as string keys because JSON requires string keys.
type partitionMapJSON struct {
	Owner map[string]map[string]string `json:"owner"`
}

// MarshalJSON encodes the PartitionMap into a portable JSON representation.
func (pm *PartitionMap) MarshalJSON() ([]byte, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	out := partitionMapJSON{
		Owner: make(map[string]map[string]string, len(pm.owner)),
	}
	for topic, partitions := range pm.owner {
		topicMap := make(map[string]string, len(partitions))
		for p, nodeID := range partitions {
			topicMap[strconv.Itoa(int(p))] = nodeID
		}
		out.Owner[topic] = topicMap
	}
	return json.Marshal(out)
}

// UnmarshalJSON decodes a previously marshalled PartitionMap.
func (pm *PartitionMap) UnmarshalJSON(data []byte) error {
	var in partitionMapJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return fmt.Errorf("cluster: partition map unmarshal: %w", err)
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.owner = make(map[string]map[int32]string, len(in.Owner))
	for topic, partitions := range in.Owner {
		assignment := make(map[int32]string, len(partitions))
		for pStr, nodeID := range partitions {
			p, err := strconv.Atoi(pStr)
			if err != nil {
				return fmt.Errorf("cluster: partition map unmarshal: invalid partition %q: %w", pStr, err)
			}
			assignment[int32(p)] = nodeID
		}
		pm.owner[topic] = assignment
	}
	return nil
}
