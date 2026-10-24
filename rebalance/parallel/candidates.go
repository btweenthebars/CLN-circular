package parallel

import (
	"circular/graph"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/gammazero/deque"
	"sort"
)

func (r *AbstractRebalance) FindCandidates(exclude string) error {
	r.Node.Logln(glightning.Debug, "Looking for candidates")

	r.Node.PeersLock.RLock()
	peers := r.GetCandidatesList()
	r.Node.PeersLock.RUnlock()

	r.Candidates = deque.New[*graph.Channel]()
	found := make(map[string]*graph.Channel)
	var ids []string
	for _, p := range peers {
		if p.Id == exclude {
			continue
		}

		for _, peerChannel := range p.Channels {
			// let's see if this channel is a candidate
			if r.IsGoodCandidate(peerChannel) {
				direction := r.GetCandidateDirection(p.Id)
				candidate, err := r.Node.GetGraphChannelFromPeerChannel(peerChannel, direction)
				if err != nil {
					r.Node.Logln(glightning.Unusual, err)
					continue
				}

				id := candidateId(candidate)
				if _, ok := found[id]; !ok {
					found[id] = candidate
					ids = append(ids, id)
				}
			}
		}
	}
	// best first: by how each did in earlier splits, and what moving
	// liquidity on it costs us
	sort.Strings(ids) // a stable starting order: peers come from a map
	cost := make(map[string]float64, len(ids))
	for _, id := range ids {
		cost[id] = r.ownFeeCost(found[id])
	}
	for _, id := range r.Node.RankCandidates(ids, cost) {
		r.Node.Logln(glightning.Debug, "adding candidate to candidates:", id)
		r.Candidates.PushBack(found[id])
	}
	if r.Candidates.Len() == 0 {
		return util.ErrNoCandidates
	}

	r.Node.Logln(glightning.Debug, "found ", r.Candidates.Len(), " candidates")
	return nil
}

func (r *AbstractRebalance) GetCandidatesList() []*glightning.Peer {
	if r.CandidatesList == nil {
		// if no CandidatesList was supplied, consider all peers as potential candidates
		return util.GetMapValues(r.Node.Peers)
	} else {
		// if a CandidatesList was supplied, consider only the peers in the CandidatesList as potential candidates
		result := make([]*glightning.Peer, 0)
		for _, peer := range r.CandidatesList {
			if _, ok := r.Node.Peers[peer]; ok {
				result = append(result, r.Node.Peers[peer])
			} else {
				r.Node.Logln(glightning.Unusual, "peer in CandidatesList does not exist: ", peer)
			}
		}

		r.Node.Logln(glightning.Debug, "using CandidatesList: ", r.CandidatesList)

		return result
	}
}

func (r *AbstractRebalance) GetNextCandidate() (*graph.Channel, error) {
	var candidate *graph.Channel
	r.Node.Logln(glightning.Debug, "getting next candidate")
	for {
		r.QueueLock.Lock()
		if r.Candidates.Len() == 0 {
			r.QueueLock.Unlock()
			break
		}
		candidate = r.Candidates.PopFront()
		r.QueueLock.Unlock()
		r.Node.Logln(glightning.Debug, "got candidate:", candidate.ShortChannelId)

		peerChannel, err := r.Node.GetPeerChannelFromGraphChannel(candidate)
		if err != nil {
			r.Node.Logln(glightning.Unusual, "error getting peer channel from graph channel:", err)
			r.leftQueue(candidateId(candidate), err.Error())
			continue
		}

		// check if we can use the channel
		if err := r.CanUseChannel(peerChannel); err != nil {
			r.Node.Logln(glightning.Debug, "channel not usable:", err)
			r.leftQueue(candidateId(candidate), err.Error())
			continue
		}
		r.Node.Logln(glightning.Debug, "channel usable")
		return candidate, nil
	}
	return nil, util.ErrNoCandidates
}

func (r *AbstractRebalance) FireCandidates() {
	r.AmountLock.Lock()
	defer r.AmountLock.Unlock()

	r.Node.Logln(glightning.Debug, "Firing candidates")
	r.Node.Logln(glightning.Debug, "AmountRebalanced: ", r.AmountRebalanced, ", InFlightAmount: ", r.InFlightAmount, ", Total amount:", r.amount)
	// Splits may deliver less than they ask for, so each asks for what is
	// left, up to the split amount.
	for r.AmountRebalanced+r.InFlightAmount < r.amount && r.splitsInFlight < r.splits &&
		!r.targetStopped && !r.streakStopped && !r.timeStopped && (r.targetCap == 0 || r.splitsInFlight < r.targetCap) {
		if r.Node.Stopped.Load() {
			// circular-stop: until circular-resume
			r.heldByStop = true
			break
		}
		candidate, err := r.GetNextCandidate()
		if err != nil {
			// no candidate left
			r.Node.Logln(glightning.Debug, err)
			break
		}
		amount := r.amount - r.AmountRebalanced - r.InFlightAmount
		if amount > r.splitAmount {
			amount = r.splitAmount
		}
		// which split this is, to tell what the target's limit was when it
		// was fired
		r.noteFired(candidate)
		r.heldByStop = false
		r.Fire(candidate, amount)

		r.InFlightAmount += amount
		r.splitsInFlight++
		r.Node.Logln(glightning.Debug, "splits in flight: ", r.splitsInFlight)
	}
}

// ownFeeCost is what moving liquidity on candidate costs us beyond the
// rebalance's fee, in ppm of the split: draining one of our out channels
// (circular-pull) gives up the fee we charge on it, so a dearer channel costs
// more; filling one (circular-push) gives it more to earn us, so a dearer
// channel costs less. 0 if our fee on it is unknown.
func (r *AbstractRebalance) ownFeeCost(candidate *graph.Channel) float64 {
	sign, peer := 1.0, candidate.Destination
	if candidate.Source != r.Node.Id {
		sign, peer = -1, candidate.Source
	}
	ours, err := r.Node.Graph.GetChannel(candidate.ShortChannelId + "/" + util.GetDirection(r.Node.Id, peer))
	if err != nil {
		return 0
	}
	return sign * float64(ours.ComputeFeePPM(r.splitAmount))
}
