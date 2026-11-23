package parallel

import (
	"circular/graph"
	"circular/node"
	rebalance2 "circular/rebalance"
	"circular/util"
	"fmt"
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
	for i := range all {
		all[i].Alias = r.Node.Graph.GetAlias(all[i].NodeId)
		if all[i].Alias == all[i].NodeId {
			all[i].Alias = ""
		}
	}
	return all
}
