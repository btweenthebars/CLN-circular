package rebalance

import (
	"sort"
	"sync"
)

// DEFAULT_MAX_RUN_NODE_FAILS is how many payments of a parallel run a node
// in the middle of their routes may fail for lack of liquidity, across all
// its splits, before the rest of the run routes around it.
const DEFAULT_MAX_RUN_NODE_FAILS = 10

// RunFailures counts, for a whole parallel run, the payments each node
// failed for lack of liquidity in the middle of a route, and drops a node for
// the rest of the run at Limit. Its splits share it: maxnodefails starts
// over with each split, so a drained hub could fail dozens of a run's
// payments, five per split. Safe for the splits' goroutines.
type RunFailures struct {
	Limit   int // 0: DEFAULT_MAX_RUN_NODE_FAILS; below 0: never dropped
	mu      sync.Mutex
	counts  map[string]int
	dropped []string // in the order they were dropped
}

func (f *RunFailures) limit() int {
	if f.Limit == 0 {
		return DEFAULT_MAX_RUN_NODE_FAILS
	}
	return f.Limit
}

// fail counts one failure of node, and says whether that dropped it.
func (f *RunFailures) fail(node string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.counts == nil {
		f.counts = map[string]int{}
	}
	f.counts[node]++
	if f.counts[node] != f.limit() { // (never, below 0)
		return false
	}
	f.dropped = append(f.dropped, node)
	return true
}

// Dropped is a copy of the nodes dropped so far, in the order they were. A
// copy: the splits share f.dropped, and one appending to what it was given
// would write into its spare room, over the next node dropped.
func (f *RunFailures) Dropped() []string {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.dropped...)
}

// DroppedNode is a dropped node and the failures counted against it.
type DroppedNode struct {
	NodeId   string `json:"node_id"`
	Alias    string `json:"alias,omitempty"`
	Failures int    `json:"failed_payments"`
}

// DroppedNodes is Dropped with their counts, most first.
func (f *RunFailures) DroppedNodes() []DroppedNode {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]DroppedNode, 0, len(f.dropped))
	for _, n := range f.dropped {
		out = append(out, DroppedNode{NodeId: n, Failures: f.counts[n]})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Failures > out[j].Failures })
	return out
}

// FailForTest counts a failure of node (tests of other packages).
func FailForTest(f *RunFailures, node string) bool {
	return f.fail(node)
}
