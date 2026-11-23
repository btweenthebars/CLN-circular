package parallel

import (
	"circular/graph"
	"circular/node"
	rebalance2 "circular/rebalance"
	"circular/util"
	"fmt"
	"sort"
	"strings"
)

// setAvoid sets the run's own avoid list (the call's `avoid`: node ids,
// short channel ids, scid/direction) and how many failures in the middle of
// a route drop a node for the rest of the run (maxrunnodefails: 0 the
// default, below 0 never). The saved list (circular-avoid) applies to every
// run; one that cannot be read refuses the run.
func (r *AbstractRebalance) setAvoid(keys []string, maxRunNodeFails int) error {
	if err := r.Node.AvoidListError(); err != nil {
		return err
	}
	r.callAvoid = nil
	for _, k := range keys {
		key, err := node.AvoidKey(k)
		if err != nil {
			return fmt.Errorf("avoid: %w", err)
		}
		r.callAvoid = append(r.callAvoid, key)
	}
	r.maxRunNodeFails = maxRunNodeFails
	r.runFailures = &rebalance2.RunFailures{Limit: maxRunNodeFails}
	return nil
}

// avoidsEnd refuses a call's avoid that names one of the run's fixed ends:
// the target channel (any direction) or its peer. The run goes through them,
// so such an entry could not be kept; it is refused rather than ignored.
func avoidsEnd(avoid []string, self string, ends ...*graph.Channel) error {
	for _, ch := range ends {
		peer := ch.Destination
		if ch.Source != self {
			peer = ch.Source
		}
		for _, k := range avoid {
			if k == peer || k == ch.ShortChannelId || strings.HasPrefix(k, ch.ShortChannelId+"/") {
				return fmt.Errorf("avoid: %s is this run's own channel %s or its peer, which the run goes "+
					"through: it cannot be avoided", k, ch.ShortChannelId)
			}
		}
	}
	return nil
}

// avoided says why candidate may not be used, when it or its peer is on the
// saved avoid list or the run's own: "" when it may.
func (r *AbstractRebalance) avoided(candidate *graph.Channel) string {
	peer := candidate.Destination
	if candidate.Source != r.Node.Id {
		peer = candidate.Source
	}
	saved, call := map[string]bool{}, map[string]bool{}
	for _, k := range r.Node.AvoidedKeys() {
		saved[k] = true
	}
	for _, k := range r.callAvoid {
		call[k] = true
	}
	for _, k := range []string{peer, candidate.ShortChannelId,
		candidate.ShortChannelId + "/" + util.GetDirection(candidate.Source, candidate.Destination)} {
		switch {
		case saved[k]:
			return fmt.Sprintf("avoided: %s is on the avoid list (circular-avoid)", k)
		case call[k]:
			return fmt.Sprintf("avoided: %s is on this run's avoid", k)
		}
	}
	return ""
}

// splitExclude is what a split's searches leave out besides the graph's
// avoid list and the saved one: the run's own avoid list, and a probe-only
// run's counted channels.
func (r *AbstractRebalance) splitExclude() []string {
	return append(node.ExcludeKeys(r.callAvoid), r.probeExclude()...) // (none unless probe-only)
}

// droppedNodes is the nodes this run dropped, in every round, with their
// aliases.
func (r *AbstractRebalance) droppedNodes() []rebalance2.DroppedNode {
	// a node dropped in several rounds once, with its failures in all of them
	var all []rebalance2.DroppedNode
	at := map[string]int{}
	for _, d := range append(append([]rebalance2.DroppedNode(nil), r.droppedBefore...), r.runFailures.DroppedNodes()...) {
		if i, ok := at[d.NodeId]; ok {
			all[i].Failures += d.Failures
			continue
		}
		at[d.NodeId] = len(all)
		all = append(all, d)
	}
	// most failures first, as within a round
	sort.SliceStable(all, func(i, j int) bool { return all[i].Failures > all[j].Failures })
	for i := range all {
		all[i].Alias = r.Node.Graph.GetAlias(all[i].NodeId)
		if all[i].Alias == all[i].NodeId {
			all[i].Alias = ""
		}
	}
	return all
}

// SOFT_AVOID_REMEMBERED_MAX: at most this many nodes from circular-node-memory
// (softavoidscore) are soft-avoided, the most failing.
const SOFT_AVOID_REMEMBERED_MAX = 50

// setSoftAvoid sets what the run's routes leave out at first (softAvoidOf).
func (r *AbstractRebalance) setSoftAvoid(keys []string, score float64, streak int) error {
	soft, scores, err := softAvoidOf(r.Node, keys, score, streak)
	if err != nil {
		return err
	}
	r.softAvoid, r.softScores = soft, scores
	return nil
}

// softAvoidOf is a run's soft avoid: keys (node ids, short channel ids or
// scid/direction: the call's softavoid) and the nodes circular-node-memory
// scores at score or more (0, or below: none), most failing first. Its
// splits leave them out of their routes until a search finds no route
// without them, or until streak failed payments in a row (0: the default;
// below 0: never) let them back for the rest of the run. Also the scores
// that put nodes on it.
func softAvoidOf(n *node.Node, keys []string, score float64, streak int) (*rebalance2.SoftAvoid, map[string]float64, error) {
	var ids []string
	seen := map[string]bool{n.Id: true}
	for _, k := range keys {
		key, err := node.AvoidKey(k)
		if err != nil {
			return nil, nil, fmt.Errorf("softavoid: %w", err)
		}
		for _, id := range node.ExcludeKeys([]string{key}) {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	scores := map[string]float64{}
	if score > 0 {
		var remembered []string
		for id, s := range n.Graph.NodeScoresNow() {
			if s.Score >= score {
				scores[id] = s.Score
				if !seen[id] {
					seen[id] = true
					remembered = append(remembered, id)
				}
			}
		}
		sort.Slice(remembered, func(i, j int) bool {
			if scores[remembered[i]] != scores[remembered[j]] {
				return scores[remembered[i]] > scores[remembered[j]]
			}
			return remembered[i] < remembered[j]
		})
		if len(remembered) > SOFT_AVOID_REMEMBERED_MAX {
			remembered = remembered[:SOFT_AVOID_REMEMBERED_MAX]
		}
		ids = append(ids, remembered...)
	}
	return rebalance2.NewSoftAvoid(ids, streak), scores, nil
}

// softReport is what a result or quote says of a soft avoid: nil for none.
func softReport(n *node.Node, soft *rebalance2.SoftAvoid, scores map[string]float64) *rebalance2.SoftAvoidReport {
	alias := func(k string) string {
		if a := n.Graph.GetAlias(k); a != k {
			return a
		}
		return ""
	}
	return soft.Report(alias, func(k string) float64 { return scores[k] })
}
