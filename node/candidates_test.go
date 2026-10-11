package node

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Candidates were tried in the order the peer map gave them.
func TestRankCandidates(t *testing.T) {
	n := &Node{}
	// a: 3 successes at 100 ppm; b: 3 successes at 10 ppm; c: never tried;
	// d: 1 success, 3 failures; e: 5 successes, then just failed
	for i := 0; i < 3; i++ {
		n.RecordCandidate("a", true, 1000000, 100)
		n.RecordCandidate("b", true, 1000000, 10)
		n.RecordCandidate("d", false, 0, 0)
	}
	n.RecordCandidate("d", true, 1000000, 1)
	for i := 0; i < 5; i++ {
		n.RecordCandidate("e", true, 1000000, 1)
	}
	n.RecordCandidate("e", false, 0, 0)

	assert.Equal(t, []string{"b", "a", "c", "d", "e"}, n.RankCandidates([]string{"a", "b", "c", "d", "e"}))

	// past the backoff, e's record counts again: (5+1)/(6+2), below a and b's (3+1)/(3+2)
	n.candidates.records["e"].LastFailure = time.Now().Add(-candidateBackoff - time.Minute).Unix()
	assert.Equal(t, []string{"b", "a", "e", "c", "d"}, n.RankCandidates([]string{"a", "b", "c", "d", "e"}))

	// ties keep their order
	assert.Equal(t, []string{"x", "y"}, n.RankCandidates([]string{"x", "y"}))
}

func TestCandidateRecordsFavourRecentResults(t *testing.T) {
	n := &Node{}
	for i := 0; i < candidateMemory; i++ {
		n.RecordCandidate("a", false, 0, 0)
	}
	n.RecordCandidate("a", true, 10, 1)
	r := n.candidates.get("a")
	assert.Equal(t, uint64(candidateMemory/2), r.Failures, "halved once the record is full")
	assert.Equal(t, uint64(1), r.Successes)
}

func TestCandidateRecordsAreSaved(t *testing.T) {
	dir := t.TempDir()
	n := &Node{Graph: graphWithChannel("1x1x1", 500000, 0), dataDir: dir, liquidityRefresh: time.Hour}
	n.RecordCandidate("9x9x9/0", true, 5000, 2)
	n.applyLearned()

	restarted := &Node{Graph: graphWithChannel("1x1x1", 500000, 0), dataDir: dir, liquidityRefresh: time.Hour}
	restarted.loadLearned()
	restarted.applyLearned()
	r := restarted.candidates.get("9x9x9/0")
	assert.Equal(t, uint64(1), r.Successes)
	assert.Equal(t, uint64(5000), r.AmountMsat)
	assert.Equal(t, uint64(2), r.FeeMsat)
}
