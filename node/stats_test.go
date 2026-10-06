package node

import (
	"circular/graph"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"testing"
)

// The stats total added up msatoshi, which CLN 23.05 removed: it was always 0.
func TestSuccessAmount(t *testing.T) {
	for text, want := range map[string]uint64{
		"100000000msat":      100000000, // older CLN
		"1e+08":              100000000, // a number, as glightning keeps it
		"1.23456789e+08":     123456789,
		"4.294967295123e+12": 4294967295123,
		"1000":               1000,
		"":                   0,
		"garbage":            0,
		"-5":                 0,
	} {
		assert.Equal(t, want, successAmount(glightning.SendPaySuccess{AmountMilliSatoshi: text}), text)
	}
	// records saved before CLN 23.05 have msatoshi
	assert.Equal(t, uint64(42), successAmount(glightning.SendPaySuccess{MilliSatoshi: 42, AmountMilliSatoshi: "1e+08"}))

	stats := &Stats{GraphStats: &graph.Stats{}, RebalancedMsat: 250000000}
	assert.Contains(t, stats.String(), "rebalanced: 250000sats")
}

// Badger was never closed: writes still in memory were lost, and its
// directory lock stayed held.
func TestCloseReleasesTheDatabase(t *testing.T) {
	dir := t.TempDir()
	n := &Node{DB: NewDB(dir)}
	assert.NoError(t, n.DB.Set("key", []byte("value")))
	n.Close()
	n.Close() // a shutdown notification, then stdin closing
	assert.True(t, n.Stopped.Load())

	reopened := NewDB(dir) // fails while another handle holds the lock
	defer reopened.Close()
	value, err := reopened.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "value", string(value))
}
