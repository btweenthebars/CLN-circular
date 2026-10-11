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
	// best first: by how each did in earlier splits
	sort.Strings(ids) // a stable starting order: peers come from a map
	for _, id := range r.Node.RankCandidates(ids) {
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
			continue
		}

		// check if we can use the channel
		if err := r.CanUseChannel(peerChannel); err != nil {
			r.Node.Logln(glightning.Debug, "channel not usable:", err)
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
	for r.AmountRebalanced+r.InFlightAmount < r.amount && r.splitsInFlight < r.splits {
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
		r.Fire(candidate, amount)

		r.InFlightAmount += amount
		r.splitsInFlight++
		r.Node.Logln(glightning.Debug, "splits in flight: ", r.splitsInFlight)
	}
}
