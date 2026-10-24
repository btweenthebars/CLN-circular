package rebalance

import "circular/graph"

type Result struct {
	Status     string             `json:"status"`
	Message    string             `json:"message"`
	Amount     uint64             `json:"amount"`
	Out        string             `json:"out"`
	In         string             `json:"in"`
	Attempts   uint64             `json:"attempts"`
	Fee        uint64             `json:"fee,omitempty"`
	PPM        uint64             `json:"ppm,omitempty"`
	Route      *graph.PrettyRoute `json:"route,omitempty"`
	FormatHint string             `json:"format-hint,omitempty"`

	AmountMsat uint64   `json:"-"` // what a success delivered, in msat
	Channels   []string `json:"-"` // a success's route, as channel ids, to send over again
	Requested  uint64   `json:"-"` // what the caller asked for, in msat (set by the parallel commands)
	Candidate  string   `json:"-"` // the candidate channel's id (set by the parallel commands)
	FailedAt   Side     `json:"-"` // where a failure happened
	Retry      Retry    `json:"-"` // whether a failure on one of our channels may pass
	// Precheck: the failure is our copy of a channel's balance being too
	// low, found before paying; reading the channel again may change it
	Precheck bool `json:"-"`
	// Halted: the split ended because its run stopped (Rebalance.Halt), not
	// for a failure of its own
	Halted bool `json:"-"`
	// CheapestPPM: the rate of the cheapest route found, when every route
	// cost more than maxppm
	CheapestPPM uint64 `json:"-"`
	// FailedNodes counts, per node, the payments it failed, leaving out
	// failures on our own channels
	FailedNodes map[string]int `json:"-"`
}

func NewResult(status string, amount uint64, src, dst string) *Result {
	return &Result{
		Status: status,
		Amount: amount,
		Out:    src,
		In:     dst,
	}
}
