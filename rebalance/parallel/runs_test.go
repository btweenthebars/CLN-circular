package parallel

import (
	"errors"
	"testing"
	"time"
)

func resetRuns(t *testing.T) {
	runs.Lock()
	runs.list = nil
	runs.Unlock()
	saved := now
	t.Cleanup(func() { now = saved; runs.Lock(); runs.list = nil; runs.Unlock() })
}

func TestRunsKeepTheResultForALaterCaller(t *testing.T) {
	resetRuns(t)
	clock := time.Unix(1000, 0)
	now = func() time.Time { return clock }
	a := startRun("pull", "1x1x1", 500000, 800)
	b := startRun("push", "2x1x1", 100000, 300)
	got := listRuns("")
	if len(got) != 2 || got[0].Target != "2x1x1" || got[1].Status != "running" || got[1].Started != 1000 {
		t.Fatalf("newest first, running: %+v", got)
	}
	clock = time.Unix(1300, 0)
	res := &Result{RebalanceTarget: 500000, RebalancedAmount: 200000}
	endRun(a, res, nil)
	endRun(b, nil, errors.New("no candidates"))
	got = listRuns("1x1x1")
	if len(got) != 1 || got[0].Status != "done" || got[0].Ended != 1300 || got[0].Result.(*Result).RebalancedAmount != 200000 {
		t.Fatalf("its result: %+v", got)
	}
	if got := listRuns("2x1x1"); got[0].Status != "failed" || got[0].Error != "no candidates" || got[0].Result != nil {
		t.Fatalf("a failed run: %+v", got)
	}
	if got := listRuns("9x1x1"); len(got) != 0 {
		t.Fatalf("another channel: %+v", got)
	}
}

func TestRunsKeepTheLastFinished(t *testing.T) {
	resetRuns(t)
	running := startRun("pull", "0x1x1", 1, 1)
	for i := 0; i < RUNS_KEPT+5; i++ {
		endRun(startRun("pull", "1x1x1", uint64(i), 1), &Result{}, nil)
	}
	got := listRuns("")
	if len(got) != RUNS_KEPT+1 {
		t.Fatalf("%d kept, want the last %d finished and the running one", len(got), RUNS_KEPT)
	}
	if got[0].Amount != RUNS_KEPT+4 || got[RUNS_KEPT-1].Amount != 5 || got[RUNS_KEPT] != *running {
		t.Fatalf("the newest finished, the running one kept: %+v", got)
	}
}

func TestResultsCmd(t *testing.T) {
	resetRuns(t)
	endRun(startRun("pull", "1x1x1", 1, 1), &Result{}, nil)
	startRun("pull", "2x1x1", 1, 1)
	res, err := (&ResultsCmd{Target: "2x1x1"}).Call()
	if err != nil || len(res.(*ResultsCmd).Runs) != 1 || res.(*ResultsCmd).Runs[0].Status != "running" {
		t.Fatalf("%v %+v", err, res)
	}
	if (&ResultsCmd{}).Name() != "circular-results" {
		t.Fatal("name")
	}
}
