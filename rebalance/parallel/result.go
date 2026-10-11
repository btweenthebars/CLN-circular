package parallel

import (
	"bytes"
	"circular/rebalance"
	"encoding/json"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
	"sort"
	"time"
)

// Success is what one candidate channel moved: the peer's alias and node id,
// and for each fee rate paid (ppm), the sats rebalanced at that rate. It is
// written as one flat JSON object:
//
//	{"alias": "Bcash", "node_id": "02...", "756": 640136}
type Success struct {
	Alias  string
	NodeId string
	ByPPM  map[uint64]uint64
}

// MarshalJSON writes the alias and node id first, then the rates in
// increasing order.
func (s *Success) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	if s.Alias != "" {
		alias, err := json.Marshal(s.Alias)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`"alias":`)
		buf.Write(alias)
		buf.WriteByte(',')
	}
	nodeId, err := json.Marshal(s.NodeId)
	if err != nil {
		return nil, err
	}
	buf.WriteString(`"node_id":`)
	buf.Write(nodeId)
	ppms := make([]uint64, 0, len(s.ByPPM))
	for ppm := range s.ByPPM {
		ppms = append(ppms, ppm)
	}
	sort.Slice(ppms, func(i, j int) bool { return ppms[i] < ppms[j] })
	for _, ppm := range ppms {
		fmt.Fprintf(&buf, `,"%d":%d`, ppm, s.ByPPM[ppm])
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

type Result struct {
	RebalanceTarget  uint64 `json:"rebalance_target"`
	RebalancedAmount uint64 `json:"rebalanced_amount"`
	Attempts         uint64 `json:"attempts"`
	Time             string `json:"time"`
	// Successes are keyed by the short channel id of the candidate channel:
	// the one drained by circular-pull, or filled by circular-push
	Successes map[string]*Success `json:"successes"`
	LastError string              `json:"last_error,omitempty"`
}

func NewResult(target uint64) *Result {
	return &Result{
		RebalanceTarget:  target / 1000,
		RebalancedAmount: 0,
		Successes:        make(map[string]*Success),
	}
}

// AddSuccessGeneric records amount sats moved at ppm through candidate
// channel scid, whose peer is nodeId.
func (r *AbstractRebalance) AddSuccessGeneric(scid, nodeId string, ppm, amount uint64) {
	r.Result.RebalancedAmount += amount
	success, ok := r.Result.Successes[scid]
	if !ok {
		r.Node.Graph.LockAliases()
		alias := r.Node.Graph.Aliases[nodeId]
		r.Node.Graph.UnlockAliases()
		success = &Success{Alias: alias, NodeId: nodeId, ByPPM: make(map[uint64]uint64)}
		r.Result.Successes[scid] = success
	}
	success.ByPPM[ppm] += amount
}

func (r *AbstractRebalance) WaitForResult() (jrpc2.Result, error) {
	start := time.Now()
	r.Result = NewResult(r.amount)

	// while there's something inflight, wait for results
	for r.splitsInFlight > 0 {
		r.Node.Logln(glightning.Debug, "Waiting for result, InFlightAmount:", r.InFlightAmount)
		rebalanceResult := <-r.RebalanceResultChan

		r.TotalAttempts += rebalanceResult.Attempts
		r.judgeCandidate(rebalanceResult)

		if rebalanceResult.Status == "success" {
			r.Node.Logln(glightning.Info, rebalanceResult.Message)
			if rebalanceResult.Route != nil {
				r.Node.Logln(glightning.Info, "\n"+rebalanceResult.Route.String())
			}
			// update results data
			r.AddSuccess(rebalanceResult)

			// put the candidate back in front of the queue
			r.EnqueueCandidate(rebalanceResult)
		} else {
			r.Node.Logf(glightning.Debug, "Failed rebalance attempt: %s", rebalanceResult.Message)
			if rebalanceResult.Message != "" {
				r.Result.LastError = rebalanceResult.Message
			}
		}

		// update inflight and rebalanced amount
		r.UpdateAmounts(rebalanceResult)

		// now that we had a result, we can fire more candidates
		r.FireCandidates()
	}

	// rebalance is over
	r.Result.Attempts = r.TotalAttempts
	r.Result.Time = fmt.Sprintf("%.3fs", float64(time.Since(start).Milliseconds())/1000)
	r.Node.Logf(glightning.Info, "Parallel rebalance finished. Total attempts: %d, time: %s, amount rebalanced: %d sats",
		r.Result.Attempts, r.Result.Time, r.Result.RebalancedAmount)
	return r.Result, nil
}

// judgeCandidate records how the split's candidate did, and adapts the run
// to what the target channel (the in channel for circular-pull, the out
// channel for circular-push) can take. It runs before the split is counted
// out of splitsInFlight.
//
//   - A failure counts against the candidate only if it happened on the
//     candidate's own channel or along the route; not on the target channel,
//     and not for a reason on neither, such as circular-stop or a timeout.
//   - A failure due to a policy just read again (RetryNow) gives the
//     candidate another try, once per run, and counts against nobody.
//   - When the target refused because other splits hold its HTLC slots or
//     balance (RetryWhenFree), the run keeps at most as many splits in
//     flight as the target held then, and the candidate gets another try
//     once one of them finishes. Retrying at once only met the same
//     refusal: in circular-push every split leaves through the target.
//   - When the target refused with nothing else in flight, or for good, no
//     more splits are fired: every candidate would meet the same refusal.
func (r *AbstractRebalance) judgeCandidate(result *rebalance.Result) {
	if result.Candidate == "" {
		return
	}
	if result.Status == "success" {
		r.Node.RecordCandidate(result.Candidate, true, result.AmountMsat, result.Fee)
		return
	}
	if result.Retry == rebalance.RetryNow && result.FailedAt != rebalance.SideNone && result.FailedAt != rebalance.SideRoute {
		r.retryCandidate(result.Candidate)
		return
	}
	switch result.FailedAt {
	case r.candidateSide, rebalance.SideRoute:
		r.Node.RecordCandidate(result.Candidate, false, 0, 0)
	case rebalance.SideNone:
		// neither channel's fault
	default: // the target channel
		others := r.splitsInFlight - 1
		if result.Retry == rebalance.RetryWhenFree && others > 0 {
			if r.targetCap == 0 || others < r.targetCap {
				r.targetCap = others
				r.Node.Logf(glightning.Debug, "the target channel takes %d splits at a time", others)
			}
			r.retryCandidate(result.Candidate)
			return
		}
		r.targetStopped = true
		r.Node.Logln(glightning.Info, "the target channel cannot take more for now: firing no more splits. ", result.Message)
	}
}

// retryCandidate puts candidate id back at the front of the queue, once per
// run.
func (r *AbstractRebalance) retryCandidate(id string) {
	candidate, ok := r.launched[id]
	if !ok || r.retried[id] {
		return
	}
	r.retried[id] = true
	r.QueueLock.Lock()
	r.Candidates.PushFront(candidate)
	r.QueueLock.Unlock()
	r.Node.Logln(glightning.Debug, "candidate ", id, " will be tried again")
}

func (r *AbstractRebalance) UpdateAmounts(result *rebalance.Result) {
	if result.Status == "success" {
		// Read both channels again, so that the deplete and fill checks of the
		// next splits see the new balances rather than those of the last peer
		// refresh. Adjusting the cached balances instead counted a payment
		// twice when a peer refresh had already seen it.
		if err := r.Node.RefreshPeerChannels(result.Out, result.In); err != nil {
			r.Node.Logln(glightning.Unusual, "unable to refresh the rebalanced channels: ", err)
		}
	}

	r.AmountLock.Lock()
	defer r.AmountLock.Unlock()

	r.InFlightAmount -= result.Requested
	r.splitsInFlight--
	if result.Status == "success" {
		r.AmountRebalanced += result.AmountMsat
	}
}
