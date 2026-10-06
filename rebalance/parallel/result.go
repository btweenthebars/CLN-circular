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
	for r.InFlightAmount > 0 {
		r.Node.Logln(glightning.Debug, "Waiting for result, InFlightAmount:", r.InFlightAmount)
		rebalanceResult := <-r.RebalanceResultChan

		r.TotalAttempts += rebalanceResult.Attempts

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

	r.InFlightAmount -= r.splitAmount
	if result.Status == "success" {
		r.AmountRebalanced += r.splitAmount
	}
}
