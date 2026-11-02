package parallel

import (
	"sync"
	"time"

	"github.com/elementsproject/glightning/jrpc2"
)

// RUNS_KEPT is how many finished runs circular-results keeps (in memory:
// a plugin restart forgets them)
const RUNS_KEPT = 20

// Run is one circular-pull or circular-push call, from when it fires its
// first split to its result. A caller that lost its connection meanwhile (a
// lightning-cli killed with its parent) can read the result here.
type Run struct {
	Kind    string `json:"kind"`   // pull / push
	Target  string `json:"target"` // inscid / outscid
	Amount  uint64 `json:"amount"` // sats asked for
	MaxPPM  uint64 `json:"maxppm"`
	Started int64  `json:"started"` // unix seconds
	Ended   int64  `json:"ended,omitempty"`
	Status  string `json:"status"` // running / done / failed
	Error   string `json:"error,omitempty"`
	// Result is what the call returned (circular-pull's result)
	Result jrpc2.Result `json:"result,omitempty"`
}

var runs struct {
	sync.Mutex
	list []*Run // oldest first: the running ones and the last RUNS_KEPT finished
}

var now = func() time.Time { return time.Now() }

// startRun notes a run that is starting.
func startRun(kind, target string, amount, maxPPM uint64) *Run {
	r := &Run{Kind: kind, Target: target, Amount: amount, MaxPPM: maxPPM, Started: now().Unix(), Status: "running"}
	runs.Lock()
	runs.list = append(runs.list, r)
	runs.Unlock()
	return r
}

// endRun notes its result, and keeps only the last RUNS_KEPT finished runs.
func endRun(r *Run, res jrpc2.Result, err error) {
	runs.Lock()
	defer runs.Unlock()
	r.Ended = now().Unix()
	if err != nil {
		r.Status, r.Error = "failed", err.Error()
	} else {
		r.Status, r.Result = "done", res
	}
	finished := 0
	for _, x := range runs.list {
		if x.Status != "running" {
			finished++
		}
	}
	kept := runs.list[:0]
	for _, x := range runs.list {
		if x.Status != "running" && finished > RUNS_KEPT {
			finished--
			continue
		}
		kept = append(kept, x)
	}
	runs.list = kept
}

// ResultsCmd is circular-results: the runs circular knows, newest first;
// target: only that channel's.
type ResultsCmd struct {
	Target string `json:"target,omitempty"`
	Runs   []Run  `json:"runs"`
}

func (c *ResultsCmd) Name() string {
	return "circular-results"
}

func (c *ResultsCmd) New() interface{} {
	return &ResultsCmd{}
}

func (c *ResultsCmd) Call() (jrpc2.Result, error) {
	return &ResultsCmd{Target: c.Target, Runs: listRuns(c.Target)}, nil
}

func listRuns(target string) []Run {
	runs.Lock()
	defer runs.Unlock()
	out := []Run{}
	for i := len(runs.list) - 1; i >= 0; i-- {
		if target == "" || runs.list[i].Target == target {
			out = append(out, *runs.list[i])
		}
	}
	return out
}
