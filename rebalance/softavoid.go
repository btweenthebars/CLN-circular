package rebalance

import (
	"circular/graph"
	"circular/util"
	"errors"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"sort"
	"sync"
)

// DEFAULT_SOFT_AVOID_STREAK is how many failed payments in a row let a run's
// soft-avoided nodes back in, when softavoidstreak is not given.
const DEFAULT_SOFT_AVOID_STREAK = 60

// SoftAvoid is what a run leaves out of its routes at first (softavoid):
// nodes known to fail. A split whose search finds no route without them
// searches again with them at once, and once the run's payments fail Limit
// times in a row they are let back for the rest of the run: a route through
// them is better than none. Unlike the avoid lists it never skips a candidate,
// and a split's own two peers are always used. Shared by the run's splits.
type SoftAvoid struct {
	Limit     int // failed payments in a row that let them back; below 0: never
	mu        sync.Mutex
	nodes     []string // node ids, or scid/direction
	streak    int
	lifted    string // why they were let back; "" while they are avoided
	fallbacks int    // splits that found no route without them and used them
}

// NewSoftAvoid makes a run's soft avoid of keys (node ids, short channel ids
// or scid/direction); nil when there are none. limit 0 is the default.
func NewSoftAvoid(keys []string, limit int) *SoftAvoid {
	if len(keys) == 0 {
		return nil
	}
	if limit == 0 {
		limit = DEFAULT_SOFT_AVOID_STREAK
	}
	return &SoftAvoid{Limit: limit, nodes: append([]string(nil), keys...)}
}

// Active is a copy of what is still avoided: none once they were let back.
func (s *SoftAvoid) Active() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifted != "" {
		return nil
	}
	return append([]string(nil), s.nodes...)
}

// paid counts a payment's (or probe's) outcome: a failure in the middle of
// its route (past our out-peer, not on our in channel: one that says the
// routes are hard to come by) adds to the streak, which at the limit lets the
// nodes back; one that went through ends it. Failures of our own channels or
// node, and errors before anything was sent, count for neither.
func (s *SoftAvoid) paid(ok bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok {
		s.streak = 0
		return
	}
	s.streak++
	if s.lifted == "" && s.Limit > 0 && s.streak >= s.Limit {
		s.lifted = fmt.Sprintf("after %d failed payments in a row", s.streak)
	}
}

func (s *SoftAvoid) fellBack() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fallbacks++
}

// SoftAvoidReport is what a result says of its soft avoid.
type SoftAvoidReport struct {
	Nodes     []SoftAvoidNode `json:"nodes"`
	Lifted    string          `json:"let_back,omitempty"`
	Fallbacks int             `json:"splits_through_them,omitempty"`
}

// SoftAvoidNode is one soft-avoided node or channel, and why.
type SoftAvoidNode struct {
	Key   string  `json:"key"`
	Alias string  `json:"alias,omitempty"`
	Score float64 `json:"memory_score,omitempty"` // circular-nodes' score, when that put it there
}

// Report says what was soft-avoided, whether and why it was let back, and
// how many splits went through it for lack of another route. alias and
// score name each key (nil: none).
func (s *SoftAvoid) Report(alias func(string) string, score func(string) float64) *SoftAvoidReport {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &SoftAvoidReport{Lifted: s.lifted, Fallbacks: s.fallbacks, Nodes: []SoftAvoidNode{}}
	for _, k := range s.nodes {
		n := SoftAvoidNode{Key: k}
		if alias != nil {
			n.Alias = alias(k)
		}
		if score != nil {
			n.Score = score(k)
		}
		out.Nodes = append(out.Nodes, n)
	}
	sort.SliceStable(out.Nodes, func(i, j int) bool { return out.Nodes[i].Score > out.Nodes[j].Score })
	return out
}

// softExclude is exclude with the soft-avoided nodes added, but never the
// split's own two peers; nil when nothing is soft-avoided.
func (r *Rebalance) softExclude(exclude map[string]bool) map[string]bool {
	active := r.Soft.Active()
	if len(active) == 0 {
		return nil
	}
	with := make(map[string]bool, len(exclude)+len(active))
	for k, v := range exclude {
		with[k] = v
	}
	for _, k := range active {
		if k != r.OutChannel.Destination && k != r.InChannel.Source {
			with[k] = true
		}
	}
	return with
}

// searchRoute is getRoute, leaving out the soft-avoided nodes first. With no
// route (or none within maxppm) without them, it searches again with them:
// they still cost their risk penalty (circular-node-memory) in that search.
// r.SoftFellBack says the route search went through them; from then on the
// split searches with them at once: what it leaves out only grows, so a
// search without them would fail again.
func (r *Rebalance) searchRoute(exclude map[string]bool) (*graph.Route, error) {
	if r.SoftFellBack {
		return r.getRoute(exclude)
	}
	soft := r.softExclude(exclude)
	if soft == nil {
		return r.getRoute(exclude)
	}
	route, err := r.getRoute(soft)
	if err == nil || !(errors.Is(err, util.ErrNoRoute) || errors.As(err, &util.ErrRouteTooExpensive{})) {
		return route, err
	}
	route, err = r.getRoute(exclude)
	if err == nil {
		r.Soft.fellBack()
		r.SoftFellBack = true
		r.Node.Logln(glightning.Debug, "no route without the soft-avoided nodes: going through them")
	}
	return route, err
}
