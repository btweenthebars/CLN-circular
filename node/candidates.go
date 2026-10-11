package node

import (
	"sort"
	"sync"
	"time"
)

const (
	// candidateBackoff is how long a candidate that just failed, and has
	// not succeeded since, is tried after all the others.
	candidateBackoff = 30 * time.Minute
	// candidateMemory caps the attempts a candidate's record counts: past
	// it, the counts are halved, so recent results weigh more.
	candidateMemory = 50
)

// CandidateRecord is how one of our channels has done as a circular-pull or
// circular-push candidate.
type CandidateRecord struct {
	Successes   uint64 `json:"successes"`
	Failures    uint64 `json:"failures"`
	LastSuccess int64  `json:"last_success,omitempty"` // Unix time
	LastFailure int64  `json:"last_failure,omitempty"`
	Failing     bool   `json:"failing,omitempty"` // the last result was a failure
	AmountMsat  uint64 `json:"amount_msat"` // delivered by the successes
	FeeMsat     uint64 `json:"fee_msat"`    // paid for them
}

// candidateRecords holds the records by channel id ("scid/direction"),
// safe for concurrent use.
type candidateRecords struct {
	lock    sync.Mutex
	records map[string]*CandidateRecord
}

func (c *candidateRecords) get(id string) CandidateRecord {
	c.lock.Lock()
	defer c.lock.Unlock()
	if r, ok := c.records[id]; ok {
		return *r
	}
	return CandidateRecord{}
}

// CandidateRecord returns how candidate channel id has done.
func (n *Node) CandidateRecord(id string) CandidateRecord {
	return n.candidates.get(id)
}

// RecordCandidate notes the result of a split through candidate channel id.
func (n *Node) RecordCandidate(id string, success bool, amountMsat, feeMsat uint64) {
	c := &n.candidates
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.records == nil {
		c.records = make(map[string]*CandidateRecord)
	}
	r, ok := c.records[id]
	if !ok {
		r = &CandidateRecord{}
		c.records[id] = r
	}
	if r.Successes+r.Failures >= candidateMemory {
		r.Successes /= 2
		r.Failures /= 2
		r.AmountMsat /= 2
		r.FeeMsat /= 2
	}
	now := time.Now().Unix()
	if success {
		r.Successes++
		r.LastSuccess = now
		r.Failing = false
		r.AmountMsat += amountMsat
		r.FeeMsat += feeMsat
	} else {
		r.Failures++
		r.LastFailure = now
		r.Failing = true
	}
}

// candidateList returns a copy of the records, to save.
func (n *Node) candidateList() map[string]CandidateRecord {
	c := &n.candidates
	c.lock.Lock()
	defer c.lock.Unlock()

	list := make(map[string]CandidateRecord, len(c.records))
	for id, r := range c.records {
		list[id] = *r
	}
	return list
}

// restoreCandidates puts back the records saved by an earlier run.
func (n *Node) restoreCandidates(list map[string]CandidateRecord) {
	c := &n.candidates
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.records == nil {
		c.records = make(map[string]*CandidateRecord)
	}
	for id, r := range list {
		if _, ok := c.records[id]; ok {
			continue // recorded since the start: newer
		}
		r := r
		c.records[id] = &r
	}
}

// RankCandidates orders candidate channel ids best first:
//   - those that failed within the last 30 minutes, and have not succeeded
//     since, come last;
//   - then by success rate, (successes+1)/(attempts+2), so a channel never
//     tried counts as an even chance;
//   - then by the fee rate their successes paid, cheapest first, with
//     channels never tried after those that were;
//   - ties keep the order they came in.
func (n *Node) RankCandidates(ids []string) []string {
	now := time.Now().Unix()
	type ranked struct {
		id       string
		backoff  bool
		rate     float64
		ppm      float64
		hasPrice bool
	}
	list := make([]ranked, len(ids))
	for i, id := range ids {
		r := n.candidates.get(id)
		list[i] = ranked{
			id:      id,
			backoff: r.Failing && now-r.LastFailure < int64(candidateBackoff.Seconds()),
			rate:    float64(r.Successes+1) / float64(r.Successes+r.Failures+2),
		}
		if r.AmountMsat > 0 {
			list[i].ppm = float64(r.FeeMsat) * 1e6 / float64(r.AmountMsat)
			list[i].hasPrice = true
		}
	}
	sort.SliceStable(list, func(i, j int) bool {
		a, b := list[i], list[j]
		if a.backoff != b.backoff {
			return !a.backoff
		}
		if a.rate != b.rate {
			return a.rate > b.rate
		}
		if a.hasPrice != b.hasPrice {
			return a.hasPrice
		}
		return a.ppm < b.ppm
	})
	result := make([]string, len(list))
	for i, r := range list {
		result[i] = r.id
	}
	return result
}
