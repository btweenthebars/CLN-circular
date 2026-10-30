package parallel

import (
	"bytes"
	"circular/graph"
	"circular/rebalance"
	"encoding/json"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
	"math"
	"math/bits"
	"sort"
	"strings"
	"time"
)

// Success is what one candidate channel moved: the peer's alias and node id,
// for each fee rate paid (ppm), the sats rebalanced at that rate, and the
// other nodes its successful routes went through. It is written as one flat
// JSON object:
//
//	{"alias": "Bcash", "node_id": "02...", "756": 640136, "involving": ["03...", "02..."]}
type Success struct {
	Alias  string
	NodeId string
	ByPPM  map[uint64]uint64
	// Involving: the nodes of the successful routes, in the order first
	// seen, without our node and the candidate's peer
	Involving []string
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
	if len(s.Involving) > 0 {
		involving, err := json.Marshal(s.Involving)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"involving":`)
		buf.Write(involving)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

type Result struct {
	RebalanceTarget  uint64 `json:"rebalance_target"`
	RebalancedAmount uint64 `json:"rebalanced_amount"`
	Attempts         uint64 `json:"attempts"`
	Time             string `json:"time"`
	// FeesPaidMsat is what the successful splits paid in fees, and AvgPPM
	// that as a rate of what they moved
	FeesPaidMsat    uint64 `json:"fees_paid_msat"`
	AvgPPM          uint64 `json:"avg_ppm"`
	SplitsSucceeded int    `json:"splits_succeeded"`
	// SplitsFailed leaves out the splits the run's stop ended
	SplitsFailed int `json:"splits_failed"`
	// Successes are keyed by the short channel id of the candidate channel:
	// the one drained by circular-pull, or filled by circular-push
	Successes map[string]*Success `json:"successes"`
	LastError string              `json:"last_error,omitempty"`
	// CheapestPPMOverMax is the lowest rate of the cheapest routes found by
	// splits that failed because every route cost more than maxppm: the
	// maxppm their search would have needed
	CheapestPPMOverMax uint64 `json:"cheapest_ppm_over_max,omitempty"`
	// With feeratio: FeeratioSkipped counts the candidates left out before
	// trying (not worth it), FeeratioHeld the splits that failed on their
	// own limit where maxppm would have let them go on
	FeeratioSkipped int `json:"feeratio_skipped,omitempty"`
	FeeratioHeld    int `json:"feeratio_held,omitempty"`
	// Rounds, in slow mode (retryevery), is how many rounds the run fired,
	// and LastRound why the last one that fell short did
	Rounds    int    `json:"rounds,omitempty"`
	LastRound string `json:"last_round,omitempty"`
	// WorstNodes are the nodes that failed the most of the run's payments,
	// most first, at most WORST_NODES of them; failures on our own channels
	// are not counted
	WorstNodes []*FailingNode `json:"worst_nodes,omitempty"`
	// Stopped says why the run fired no more splits, when it did not reach
	// its target: the target channel took no more, too many payments failed
	// in a row (maxfailstreak), or no candidate was left (and why).
	Stopped string `json:"stopped,omitempty"`
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

// addInvolving adds to candidate channel scid's success the nodes route
// went through, other than ours and peer, the candidate's.
func (r *AbstractRebalance) addInvolving(scid, peer string, route *graph.PrettyRoute) {
	success, ok := r.Result.Successes[scid]
	if !ok || route == nil {
		return
	}
	for _, hop := range route.Hops {
		if hop.Id == "" || hop.Id == r.Node.Id || hop.Id == peer || contains(success.Involving, hop.Id) {
			continue
		}
		success.Involving = append(success.Involving, hop.Id)
	}
}

func contains(ids []string, id string) bool {
	for _, i := range ids {
		if i == id {
			return true
		}
	}
	return false
}

func (r *AbstractRebalance) WaitForResult() (jrpc2.Result, error) {
	start := time.Now()
	r.Result = NewResult(r.amount)

	every := r.progressEvery
	if every <= 0 {
		every = PROGRESS_EVERY
	}
	progress := time.NewTicker(every)
	defer progress.Stop()
	var deadline <-chan time.Time // never, without a time limit
	if r.maxTime > 0 {
		timer := time.NewTimer(r.maxTime)
		defer timer.Stop()
		deadline = timer.C
	}

	for r.rounds = 1; ; r.rounds++ {
		r.waitForRound(start, progress.C, deadline)
		if !r.anotherRound(start, progress.C, deadline) {
			break
		}
	}
	return r.endRun(start)
}

// waitForRound waits for the results of a round's splits, firing more as
// they come back, until none is in flight.
func (r *AbstractRebalance) waitForRound(start time.Time, tick, deadline <-chan time.Time) {
	// while there's something inflight, wait for results
	for r.splitsInFlight > 0 {
		r.Node.Logln(glightning.Debug, "Waiting for result, InFlightAmount:", r.InFlightAmount)
		rebalanceResult := r.nextResult(start, tick, deadline)

		r.TotalAttempts += rebalanceResult.Attempts
		r.countFailedNodes(rebalanceResult)
		r.countFailStreak(rebalanceResult)
		r.judgeCandidate(rebalanceResult)
		if rebalanceResult.Status != "success" && rebalanceResult.Candidate != "" {
			// a candidate tried again leaves the queue again, for its own reason
			r.leftQueue(rebalanceResult.Candidate, r.howItFailed(rebalanceResult))
		}

		if rebalanceResult.Status == "success" {
			r.Node.Logln(glightning.Info, rebalanceResult.Message)
			if rebalanceResult.Route != nil {
				r.Node.Logln(glightning.Info, "\n"+rebalanceResult.Route.String())
			}
			// update results data
			r.AddSuccess(rebalanceResult)
			r.Result.SplitsSucceeded++
			r.Result.FeesPaidMsat += rebalanceResult.Fee
			r.movedMsat += rebalanceResult.AmountMsat

			// put the candidate back in front of the queue
			r.EnqueueCandidate(rebalanceResult)
		} else {
			r.Node.Logf(glightning.Debug, "Failed rebalance attempt: %s", rebalanceResult.Message)
			if !rebalanceResult.Halted {
				r.Result.SplitsFailed++
			}
			if rebalanceResult.HeldByOwnLimit {
				r.Result.FeeratioHeld++
			}
			if p := rebalanceResult.CheapestPPM; p > 0 && (r.Result.CheapestPPMOverMax == 0 || p < r.Result.CheapestPPMOverMax) {
				r.Result.CheapestPPMOverMax = p
			}
			if rebalanceResult.Message != "" && !rebalanceResult.Halted {
				// a split the run's stop ended says nothing new: the last real
				// failure stays
				// one split's failure, not the run's: say which
				r.Result.LastError = r.splitName(rebalanceResult) + ": " + rebalanceResult.Message
			}
		}

		// update inflight and rebalanced amount
		r.UpdateAmounts(rebalanceResult)

		// now that we had a result, we can fire more candidates
		r.FireCandidates()
	}
}

// endRun completes the run's result once nothing is in flight and no other
// round follows.
func (r *AbstractRebalance) endRun(start time.Time) (jrpc2.Result, error) {
	// rebalance is over
	r.Result.Attempts = r.TotalAttempts
	if r.retryEvery > 0 {
		r.Result.Rounds = r.rounds
	}
	r.Result.AvgPPM = avgPPM(r.Result.FeesPaidMsat, r.movedMsat)
	r.Result.WorstNodes = r.worstNodes()
	r.Result.FeeratioSkipped = len(r.feeratioSkipped)
	if r.AmountRebalanced < r.amount && r.Result.Stopped == "" {
		switch {
		case r.held():
			// circular-stop came first: its splits ended at their next
			// payment, and no more were fired
			r.Result.Stopped = "circular was stopped (circular-stop): firing no more splits"
		case r.firstStop != "":
			// a stop during a hold that circular-resume then ended
			r.Result.Stopped = r.firstStop
		case !r.targetStopped && !r.streakStopped && !r.timeStopped && !r.feeStopped:
			// nothing stopped it: no candidate was left to fire
			r.Result.Stopped = r.ranOut()
		}
		if r.Result.Stopped != "" {
			r.Node.Logln(glightning.Info, r.Result.Stopped)
		}
	}
	if r.AmountRebalanced >= r.amount {
		// the splits in flight when it stopped still reached the target
		r.Result.Stopped = ""
	}
	r.Result.Time = fmt.Sprintf("%.3fs", float64(time.Since(start).Milliseconds())/1000)
	r.Node.Logf(glightning.Info, "Parallel rebalance finished. Total payments: %d, time: %s, amount rebalanced: %d sats",
		r.Result.Attempts, r.Result.Time, r.Result.RebalancedAmount)
	return r.Result, nil
}

// DEFAULT_MAX_FAIL_STREAK is how many failed payments in a row, with no
// split succeeding, end a run by default: a run whose target nothing can
// reach within maxppm used to go on through every candidate's attempts.
const DEFAULT_MAX_FAIL_STREAK = 300

// countFailStreak counts the failed payments of result toward the run's
// streak, which a success ends, and stops firing splits once the streak
// reaches its limit.
func (r *AbstractRebalance) countFailStreak(result *rebalance.Result) {
	if result.Status == "success" {
		r.failStreak = 0
		return
	}
	r.failStreak += int(result.Attempts)
	limit := r.maxFailStreak
	if limit == 0 {
		// the node's default, which may be below 0 (no limit)
		limit = r.Node.GetDefaults().MaxFailStreak
	}
	if limit == 0 {
		limit = DEFAULT_MAX_FAIL_STREAK
	}
	if limit < 0 || r.failStreak < limit || r.streakStopped {
		return
	}
	r.streakStopped = true
	r.halt.Store(true)
	r.noteStop(fmt.Sprintf("%d payments failed in a row without a split succeeding (maxfailstreak %d): firing no more splits",
		r.failStreak, limit))
}

// noteStop logs why the run stopped firing splits, and keeps the first such
// reason, in Result.Stopped too unless circular-stop is holding the run back
// (said at the end of the run, or this one if circular was resumed).
func (r *AbstractRebalance) noteStop(why string) {
	r.Node.Logln(glightning.Info, why)
	if r.firstStop == "" {
		r.firstStop = why
	}
	if r.Result != nil && r.Result.Stopped == "" && !r.held() {
		r.Result.Stopped = r.firstStop
	}
}

// held says whether circular-stop is holding the run back: it did the last
// time the run could fire a split, and circular has not been resumed since.
func (r *AbstractRebalance) held() bool {
	return r.heldByStop && r.Node.Stopped.Load()
}

// splitName names the split behind result by its candidate channel and that
// channel's peer, as in "last failed split, via 123x1x1 (alias)".
func (r *AbstractRebalance) splitName(result *rebalance.Result) string {
	peer := result.In
	if r.candidateSide == rebalance.SideOut {
		peer = result.Out
	}
	scid := result.Candidate
	if i := strings.IndexByte(scid, '/'); i >= 0 {
		scid = scid[:i]
	}
	if scid == "" {
		return "last failed split"
	}
	return fmt.Sprintf("last failed split, via %s (%s)", scid, r.Node.Graph.GetAlias(peer))
}

// judgeCandidate records how the split's candidate did, and adapts the run
// to what the target channel (the in channel for circular-pull, the out
// channel for circular-push) can take. It runs before the split is counted
// out of splitsInFlight.
//
//   - A failure counts against the candidate only if it happened on the
//     candidate's own channel or along the route; not on the target channel,
//     and not for a reason on neither, such as circular-stop or a timeout.
//   - A failure due to a policy just read again and found changed (RetryNow)
//     gives the candidate another try, once per run, counted against nobody.
//     A second one is judged like any other failure.
//   - When the target refused because other splits hold its HTLC slots or
//     balance (RetryWhenFree), the run keeps at most as many splits in
//     flight as the target held then, and the candidate gets another try
//     once one of them finishes. Retrying at once only met the same
//     refusal: in circular-push every split leaves through the target. The
//     limit grows back by one once that many splits succeed, in case the
//     refusal had another cause. Each time one more at once is refused, it
//     waits twice as long to try again, and half as long when one more held.
//     A candidate refused more than maxBusyRefusals times for no reason the
//     target's limit explains (see targetBusy) goes to the back of the
//     queue: the target may refuse it alone, for the fees its route adds to
//     the HTLC, say.
//   - When the target refused with nothing else in flight, see
//     targetRefusedAlone: one candidate may be left out, but when it then
//     refuses another one alone too, or refuses for good, no more splits are
//     fired: every candidate would meet the same refusal.
func (r *AbstractRebalance) judgeCandidate(result *rebalance.Result) {
	if result.Candidate == "" {
		return
	}
	if result.Status == "success" {
		r.Node.RecordCandidate(result.Candidate, true, result.AmountMsat, result.Fee)
		r.targetRechecked = false
		r.refusedAlone = ""
		r.targetSucceeded(result.Candidate)
		return
	}
	if r.isProbe(result.Candidate) {
		// unless the target refused it as busy, the probe tells nothing: the
		// next split fired up to the limit probes instead
		defer func() {
			if r.isProbe(result.Candidate) {
				r.probeSeq = 0
			}
		}()
	}
	if result.Retry == rebalance.RetryNow && result.FailedAt != rebalance.SideNone && result.FailedAt != rebalance.SideRoute {
		if !r.retried[result.Candidate] {
			r.retried[result.Candidate] = true
			r.requeue(result.Candidate, true)
			return
		}
		// it had its try: judge it as any other failure
	}
	switch result.FailedAt {
	case r.candidateSide, rebalance.SideRoute:
		r.Node.RecordCandidate(result.Candidate, false, 0, 0)
	case rebalance.SideNone:
		// neither channel's fault
	default: // the target channel
		others := r.splitsInFlight - 1
		if result.Retry == rebalance.RetryWhenFree && others > 0 {
			r.targetBusy(result.Candidate, others)
			return
		}
		if result.Retry == rebalance.RetryWhenFree && r.targetRefusedAlone(result) {
			return
		}
		r.targetStopped = true
		r.halt.Store(true)
		r.noteStop("the target channel cannot take more for now: firing no more splits. " + result.Message)
	}
}

// targetSucceeded lets the limit on splits at once grow back after a
// success of candidate id: by one once as many splits as the limit succeed,
// times the backoff. A growth is judged by its probe, the split fired up to
// the new limit: no other growth until that split succeeds, which halves the
// backoff, or is refused.
func (r *AbstractRebalance) targetSucceeded(id string) {
	if r.targetCap == 0 {
		return
	}
	r.capSuccesses++
	if r.isProbe(id) {
		// one more at once was not too many
		r.probing, r.probeSeq = false, 0
		if r.capBackoff > 1 {
			r.capBackoff /= 2
		}
	}
	if !r.probing && r.targetCap < r.splits && r.capSuccesses >= r.targetCap*r.backoff() {
		r.setTargetCap(r.targetCap + 1)
		r.probing, r.probeSeq = true, 0
	}
}

// isProbe reports whether the split in flight for candidate id is the probe
// of the last growth.
func (r *AbstractRebalance) isProbe(id string) bool {
	return r.probing && r.probeSeq != 0 && r.firedAt[id] == r.probeSeq
}

// noteFired numbers the split about to be fired for candidate, and makes it
// the probe of the last growth if it brings the splits in flight up to the
// new limit. It runs before the split is counted in splitsInFlight. Splits
// whose results wait in RebalanceResultChan have already left the target:
// counting them, a probe found room they had freed and seemed to hold.
func (r *AbstractRebalance) noteFired(candidate *graph.Channel) {
	r.fireSeq++
	if r.firedAt == nil {
		r.firedAt = make(map[string]int)
	}
	r.firedAt[candidateId(candidate)] = r.fireSeq
	atTarget := r.splitsInFlight - len(r.RebalanceResultChan)
	if r.probing && r.probeSeq == 0 && atTarget+1 >= r.targetCap {
		r.probeSeq = r.fireSeq
	}
}

// setTargetCap changes the limit on splits at once to cap, as of the splits
// fired so far.
func (r *AbstractRebalance) setTargetCap(cap int) {
	r.targetCap = cap
	r.capSetAt = r.fireSeq
	r.capSuccesses = 0
}

// targetBusy handles a busy refusal of candidate id by the target, with
// others splits still in flight.
func (r *AbstractRebalance) targetBusy(id string, others int) {
	// The target's limit explains it if there was none yet (our own splits
	// filled the target), if the split was fired before the limit took its
	// current value, or if the limit grew and has not proved itself yet: one
	// more at once may be too many.
	explained := r.targetCap == 0 || r.firedAt[id] <= r.capSetAt || r.probing
	if r.targetCap == 0 || others < r.targetCap {
		if r.probing {
			// the last growth was one too many: wait longer before trying
			// one more again
			r.probing, r.probeSeq = false, 0
			if r.capBackoff < maxCapBackoff {
				r.capBackoff = r.backoff() * 2
			}
		}
		r.setTargetCap(others)
		r.Node.Logf(glightning.Debug, "the target channel takes %d splits at a time", others)
	}
	if !explained {
		if r.busyRefusals == nil {
			r.busyRefusals = make(map[string]int)
		}
		if r.busyRefusals[id]++; r.busyRefusals[id] > maxBusyRefusals {
			r.Node.Logln(glightning.Debug, "candidate ", id, " is often refused by the target: trying the others first")
			r.requeue(id, false)
			return
		}
	}
	r.requeue(id, true)
}

// targetRefusedAlone handles a busy refusal by the target with nothing else
// in flight, and reports whether the run can go on.
//
// A refusal by our copy of the target's balance (a precheck) may come from a
// read taken while other splits held the target: the first since the last
// success reads the target again and retries the candidate. A refusal by
// lightningd or a peer would not change with a new read.
//
// After that, the target may refuse this candidate alone (for the fees its
// route adds to the HTLC, say): it is left out, the target read again if it
// was a precheck, and the next candidate tried. A refusal of another
// candidate alone after that stops the run.
func (r *AbstractRebalance) targetRefusedAlone(result *rebalance.Result) bool {
	peer := result.In
	if result.FailedAt == rebalance.SideOut {
		peer = result.Out
	}
	if result.Precheck && !r.targetRechecked {
		r.targetRechecked = true
		if err := r.refreshPeers(peer); err != nil {
			r.Node.Logln(glightning.Unusual, "unable to read the target channel again: ", err)
			return false
		}
		r.Node.Logln(glightning.Debug, "the target channel refused with nothing else in flight: read it again and try once more")
		r.requeue(result.Candidate, true)
		return true
	}
	if r.refusedAlone != "" && r.refusedAlone != result.Candidate {
		return false
	}
	r.refusedAlone = result.Candidate
	if result.Precheck {
		// the next candidate's precheck sees the target as it is now
		if err := r.refreshPeers(peer); err != nil {
			r.Node.Logln(glightning.Unusual, "unable to read the target channel again: ", err)
			return false
		}
	}
	r.Node.Logln(glightning.Debug, "the target channel refused candidate ", result.Candidate, " alone: trying the others")
	return true
}

// maxBusyRefusals is how many unexplained busy refusals by the target a
// candidate may have before it goes to the back of the queue; maxCapBackoff
// bounds how much longer the run waits before giving the target one more
// split at once after such a try was refused.
const (
	maxBusyRefusals = 3
	maxCapBackoff   = 64
)

// backoff is capBackoff, 1 when unset.
func (r *AbstractRebalance) backoff() int {
	if r.capBackoff < 1 {
		return 1
	}
	return r.capBackoff
}

// requeue puts candidate id back at the front of the queue, or at its back.
// Each requeue follows a busy refusal that lowered the target's limit or
// came from a split fired before it was lowered, or a re-read or retry
// allowed once since the last success: the limit only grows after
// successes, so candidates cannot come back forever.
func (r *AbstractRebalance) requeue(id string, front bool) {
	candidate, ok := r.launched[id]
	if !ok {
		return
	}
	r.QueueLock.Lock()
	if front {
		r.Candidates.PushFront(candidate)
	} else {
		r.Candidates.PushBack(candidate)
	}
	r.QueueLock.Unlock()
	r.Node.Logln(glightning.Debug, "candidate ", id, " will be tried again")
}

// refreshPeers reads our channels with the given peers again.
func (r *AbstractRebalance) refreshPeers(peerIds ...string) error {
	if r.refresh != nil {
		return r.refresh(peerIds...)
	}
	return r.Node.RefreshPeerChannels(peerIds...)
}

func (r *AbstractRebalance) UpdateAmounts(result *rebalance.Result) {
	if result.Status == "success" {
		// Read both channels again, so that the deplete and fill checks of the
		// next splits see the new balances rather than those of the last peer
		// refresh. Adjusting the cached balances instead counted a payment
		// twice when a peer refresh had already seen it.
		if err := r.refreshPeers(result.Out, result.In); err != nil {
			r.Node.Logln(glightning.Unusual, "unable to refresh the rebalanced channels: ", err)
		}
	}

	r.AmountLock.Lock()
	defer r.AmountLock.Unlock()

	r.InFlightAmount -= result.Requested
	r.releaseFee(result.Requested)
	r.splitsInFlight--
	if result.Status == "success" {
		r.AmountRebalanced += result.AmountMsat
	}
}

// avgPPM is fee as a rate of moved, in ppm, without overflowing fee*1e6.
func avgPPM(fee, moved uint64) uint64 {
	if moved == 0 {
		return 0
	}
	hi, lo := bits.Mul64(fee, 1000000)
	if hi >= moved {
		return math.MaxUint64
	}
	ppm, _ := bits.Div64(hi, lo, moved)
	return ppm
}

// WORST_NODES is how many nodes Result.WorstNodes lists.
const WORST_NODES = 5

// FailingNode is a node that failed some of a run's payments.
type FailingNode struct {
	Alias          string `json:"alias,omitempty"`
	NodeId         string `json:"node_id"`
	FailedPayments int    `json:"failed_payments"`
}

// countFailedNodes adds up, per node, the payments a split's node failed,
// whatever the split's outcome.
func (r *AbstractRebalance) countFailedNodes(result *rebalance.Result) {
	for id, n := range result.FailedNodes {
		if r.failedAt == nil {
			r.failedAt = map[string]int{}
		}
		r.failedAt[id] += n
	}
}

// worstNodes lists the WORST_NODES nodes that failed the most payments, most
// first, then by node id.
func (r *AbstractRebalance) worstNodes() []*FailingNode {
	nodes := make([]*FailingNode, 0, len(r.failedAt))
	for id, n := range r.failedAt {
		nodes = append(nodes, &FailingNode{NodeId: id, FailedPayments: n})
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].FailedPayments != nodes[j].FailedPayments {
			return nodes[i].FailedPayments > nodes[j].FailedPayments
		}
		return nodes[i].NodeId < nodes[j].NodeId
	})
	if len(nodes) > WORST_NODES {
		nodes = nodes[:WORST_NODES]
	}
	if len(nodes) == 0 {
		return nil
	}
	r.Node.Graph.LockAliases()
	for _, node := range nodes {
		node.Alias = r.Node.Graph.Aliases[node.NodeId]
	}
	r.Node.Graph.UnlockAliases()
	return nodes
}

// How a candidate's last split failed, for Result.Stopped.
const (
	goneFailed  = "failed"                // on its channel, the route, or neither's fault
	goneRefused = "refused by the target" // the target took no split from it alone
)

// leftQueue notes why candidate id left the queue.
func (r *AbstractRebalance) leftQueue(id, why string) {
	if r.gone == nil {
		r.gone = map[string]string{}
	}
	r.gone[id] = why
}

// howItFailed says whose a failed split's failure was.
func (r *AbstractRebalance) howItFailed(result *rebalance.Result) string {
	switch result.FailedAt {
	case r.candidateSide, rebalance.SideRoute, rebalance.SideNone:
		return goneFailed
	}
	return goneRefused
}

// ranOut says why no candidate was left: how many failed, how many the
// target refused, and how many were skipped, and why.
func (r *AbstractRebalance) ranOut() string {
	var failed, refused, skipped int
	reasons := map[string]int{}
	for _, why := range r.gone {
		switch why {
		case goneFailed:
			failed++
		case goneRefused:
			refused++
		default:
			skipped++
			reasons[why]++
		}
	}
	var parts []string
	if failed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", failed))
	}
	if refused > 0 {
		parts = append(parts, fmt.Sprintf("%d refused by the target", refused))
	}
	if skipped > 0 {
		whys := make([]string, 0, len(reasons))
		for why := range reasons {
			whys = append(whys, why)
		}
		sort.Slice(whys, func(i, j int) bool {
			if reasons[whys[i]] != reasons[whys[j]] {
				return reasons[whys[i]] > reasons[whys[j]]
			}
			return whys[i] < whys[j]
		})
		if len(whys) == 1 {
			parts = append(parts, fmt.Sprintf("%d skipped (%s)", skipped, whys[0]))
		} else {
			for i, why := range whys {
				whys[i] = fmt.Sprintf("%s: %d", why, reasons[why])
			}
			parts = append(parts, fmt.Sprintf("%d skipped (%s)", skipped, strings.Join(whys, ", ")))
		}
	}
	if len(parts) == 0 {
		return "no candidates to try"
	}
	return "no candidates left: " + strings.Join(parts, ", ")
}

// PROGRESS_EVERY is how often a run in progress logs how it is going.
const PROGRESS_EVERY = 30 * time.Second

// nextResult waits for a split's result, logging how the run is going each
// time tick fires meanwhile, and stopping the run if deadline fires.
func (r *AbstractRebalance) nextResult(start time.Time, tick, deadline <-chan time.Time) *rebalance.Result {
	for {
		select {
		case result := <-r.RebalanceResultChan:
			return result
		case <-tick:
			r.Node.Logln(glightning.Info, r.progress(start))
		case <-deadline:
			r.timeUp()
		}
	}
}

// setMaxTime sets the run's time limit to seconds: 0, the node's default;
// below 0, none.
func (r *AbstractRebalance) setMaxTime(seconds int) {
	if seconds == 0 {
		seconds = r.Node.GetDefaults().MaxTime
	}
	r.maxTime = 0
	if seconds > 0 {
		r.maxTime = time.Duration(seconds) * time.Second
	}
}

// timeUp stops the run at its time limit, as countFailStreak does at
// maxfailstreak: no more splits, and those in flight end before their next
// payment.
func (r *AbstractRebalance) timeUp() {
	if r.timeStopped {
		return
	}
	r.timeStopped = true
	r.halt.Store(true)
	r.noteStop(fmt.Sprintf("the run reached its time limit (maxtime %.0fs): firing no more splits", r.maxTime.Seconds()))
}

// progress says how the run is going: for example
//
//	circular-pull 965538x2935x4: 1200000/3000000 sats moved in 312s, 143 payments, avg 280 ppm, 2 splits in flight
//
// and, when the last payments failed, how many in a row.
func (r *AbstractRebalance) progress(start time.Time) string {
	name := "rebalance"
	if named, ok := r.RebalanceMethods.(interface{ Name() string }); ok {
		name = named.Name()
	}
	target := ""
	if r.TargetChannel != nil {
		target = " " + r.TargetChannel.ShortChannelId
	}
	r.AmountLock.Lock()
	moved, inFlight := r.AmountRebalanced, r.splitsInFlight
	r.AmountLock.Unlock()
	// the splits count their payments as they send them; their results
	// count them too, once they finish
	payments := uint64(r.sent.Load())
	if payments < r.TotalAttempts {
		payments = r.TotalAttempts
	}
	line := fmt.Sprintf("%s%s: %d/%d sats moved in %ds, %d payments", name, target, moved/1000, r.amount/1000,
		int(time.Since(start).Seconds()), payments)
	if r.movedMsat > 0 {
		line += fmt.Sprintf(", avg %d ppm", avgPPM(r.Result.FeesPaidMsat, r.movedMsat))
	}
	line += fmt.Sprintf(", %d splits in flight", inFlight)
	if r.retryEvery > 0 {
		line += fmt.Sprintf(", round %d", r.rounds)
	}
	if r.failStreak > 0 {
		line += fmt.Sprintf(", %d payments failed in a row", r.failStreak)
	}
	return line
}

// DEFAULT_SLOW_MAXTIME is the time limit of a run in slow mode that has none
// of its own; MIN_RETRY_EVERY the shortest wait between its rounds.
const (
	DEFAULT_SLOW_MAXTIME = 6 * time.Hour
	MIN_RETRY_EVERY      = time.Minute
)

// setRetryEvery turns slow mode on with a wait of seconds between rounds
// (0 or less: off; at least MIN_RETRY_EVERY), with a time limit of
// DEFAULT_SLOW_MAXTIME if the run has none. Call after setMaxTime.
func (r *AbstractRebalance) setRetryEvery(seconds int) {
	r.retryEvery = 0
	if seconds <= 0 {
		return
	}
	r.retryEvery = time.Duration(seconds) * time.Second
	if r.retryEvery < MIN_RETRY_EVERY {
		r.retryEvery = MIN_RETRY_EVERY
	}
	if r.maxTime <= 0 {
		r.maxTime = DEFAULT_SLOW_MAXTIME
	}
}

// anotherRound, in slow mode, decides whether a round that fell short is
// followed by another: not if the target was reached, or the run hit its
// time limit or fee budget, or circular-stop holds it. If so, it waits
// retryEvery (logging progress; the time limit or circular-stop during the
// wait end the run instead), then starts the next round.
func (r *AbstractRebalance) anotherRound(start time.Time, tick, deadline <-chan time.Time) bool {
	if r.retryEvery <= 0 || r.AmountRebalanced >= r.amount || r.timeStopped || r.feeStopped ||
		r.held() || r.Node.Stopped.Load() {
		return false
	}
	why := r.firstStop
	if why == "" {
		why = r.ranOut()
	}
	r.Result.LastRound = why
	r.Node.Logf(glightning.Info, "%s: round %d: %s; next round in %s",
		r.progress(start), r.rounds, why, r.retryEvery)
	if !r.waitRetry(start, tick, deadline) {
		return false
	}
	r.newRound()
	return true
}

// waitRetry waits retryEvery before another round: false if the run's time
// limit or circular-stop comes first.
func (r *AbstractRebalance) waitRetry(start time.Time, tick, deadline <-chan time.Time) bool {
	next := time.NewTimer(r.retryEvery)
	defer next.Stop()
	poll := r.retryEvery / 20
	if poll > time.Second {
		poll = time.Second
	}
	stopped := time.NewTicker(poll)
	defer stopped.Stop()
	for {
		select {
		case <-next.C:
			return true
		case <-tick:
			r.Node.Logln(glightning.Info, r.progress(start)+", waiting for the next round")
		case <-deadline:
			// that, not the last round's stop, is why the run ends (the
			// latter is in last_round)
			r.firstStop, r.Result.Stopped = "", ""
			r.timeUp()
			return false
		case <-stopped.C:
			if r.Node.Stopped.Load() {
				r.firstStop, r.Result.Stopped = "", ""
				r.heldByStop = true // said at the end of the run
				return false
			}
		}
	}
}

// newRound clears what a round decided about the run, finds the candidates
// again (their balances may have changed), and fires the next round's
// splits. What moved, was paid and was learned is kept.
func (r *AbstractRebalance) newRound() {
	r.failStreak, r.streakStopped, r.targetStopped = 0, false, false
	r.halt.Store(false)
	r.firstStop, r.Result.Stopped = "", ""
	r.gone = nil
	r.retried = map[string]bool{}
	r.targetCap, r.capSuccesses, r.capSetAt, r.probing, r.probeSeq, r.capBackoff = 0, 0, 0, false, 0, 0
	r.busyRefusals = nil
	r.targetRechecked, r.refusedAlone = false, ""
	r.earnings = nil // what channels earned may have changed: read again (the node caches it)
	find := r.refind
	if find == nil {
		find = func() error { return r.FindCandidates(r.excludePeer) }
	}
	if err := find(); err != nil {
		r.Node.Logln(glightning.Unusual, "round ", r.rounds+1, ": unable to find the candidates again: ", err)
	}
	r.prefetchEarnings()
	r.FireCandidates()
}
