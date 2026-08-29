package parallel

import (
	"circular/graph"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/gammazero/deque"
)

func (r *AbstractRebalance) FindCandidates(exclude string) error {
	r.Node.PeersLock.RLock()
	peers := r.GetCandidatesList()
	r.Node.PeersLock.RUnlock()

	r.Node.Logf(glightning.Info, "FindCandidates: evaluating %d peers (excluding target node %s)", len(peers), exclude)

	r.Candidates = deque.New[*graph.Channel]()
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
					r.Node.Logf(glightning.Unusual, "FindCandidates: GetGraphChannelFromPeerChannel(%s) error: %v", peerChannel.ShortChannelId, err)
					continue
				}

				r.Node.Logf(glightning.Info, "FindCandidates: adding candidate %s", candidate.ShortChannelId)
				r.Candidates.PushBack(candidate)
			}
		}
	}
	if r.Candidates.Len() == 0 {
		r.Node.Logf(glightning.Unusual, "FindCandidates: 0 candidate channels found")
		return util.ErrNoCandidates
	}

	r.Node.Logf(glightning.Info, "FindCandidates: found %d candidate channels", r.Candidates.Len())
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

		r.Node.Logln(glightning.Info, "using CandidatesList: ", r.CandidatesList)

		return result
	}
}

func (r *AbstractRebalance) GetNextCandidate() (*graph.Channel, error) {
	var candidate *graph.Channel
	for {
		r.QueueLock.Lock()
		if r.Candidates.Len() == 0 {
			r.QueueLock.Unlock()
			r.Node.Logf(glightning.Info, "GetNextCandidate: queue is empty (0 candidates left)")
			break
		}
		candidate = r.Candidates.PopFront()
		r.QueueLock.Unlock()
		r.Node.Logf(glightning.Info, "GetNextCandidate: popped candidate %s (remaining: %d)", candidate.ShortChannelId, r.Candidates.Len())

		peerChannel, err := r.Node.GetPeerChannelFromGraphChannel(candidate)
		if err != nil {
			r.Node.Logf(glightning.Unusual, "GetNextCandidate: error getting peer channel for %s: %v", candidate.ShortChannelId, err)
			continue
		}

		// check if we can use the channel
		if err := r.CanUseChannel(peerChannel); err != nil {
			r.Node.Logf(glightning.Info, "GetNextCandidate: channel %s not usable: %v", candidate.ShortChannelId, err)
			continue
		}
		r.Node.Logf(glightning.Info, "GetNextCandidate: channel %s is usable!", candidate.ShortChannelId)
		return candidate, nil
	}
	return nil, util.ErrNoCandidates
}

func (r *AbstractRebalance) FireCandidates() {
	r.AmountLock.Lock()
	defer r.AmountLock.Unlock()

	carryOn := r.AmountRebalanced+r.InFlightAmount < r.amount
	splitsInFlight := int(r.InFlightAmount / r.splitAmount)

	r.Node.Logf(glightning.Info, "FireCandidates: InFlight=%d, Rebalanced=%d, Target=%d, CarryOn=%v, SplitsInFlight=%d, MaxSplits=%d",
		r.InFlightAmount, r.AmountRebalanced, r.amount, carryOn, splitsInFlight, r.splits)

	for carryOn && splitsInFlight < r.splits {
		candidate, err := r.GetNextCandidate()
		if err != nil {
			r.Node.Logf(glightning.Info, "FireCandidates: GetNextCandidate stopped: %v", err)
			break
		}
		r.Node.Logf(glightning.Info, "FireCandidates: firing candidate %s", candidate.ShortChannelId)
		r.Fire(candidate)

		r.InFlightAmount += r.splitAmount
		carryOn = r.AmountRebalanced+r.InFlightAmount < r.amount
		splitsInFlight = int(r.InFlightAmount / r.splitAmount)
	}
}
