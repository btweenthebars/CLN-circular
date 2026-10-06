package node

import (
	"circular/graph"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
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

// Badger panics on a read that starts while it is closing, which a shutdown
// during a payment notification or circular-stats could hit.
func TestStoreIsSafeToUseWhileClosing(t *testing.T) {
	store := NewDB(t.TempDir())
	for i := 0; i < 100; i++ {
		assert.NoError(t, store.Set(fmt.Sprintf("%s%d", SUCCESS_PREFIX, i), []byte(`{"amount_msat":"1e+08"}`)))
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				var err error
				switch i % 3 {
				case 0:
					_, err = store.Get(SUCCESS_PREFIX + "1")
				case 1:
					_, err = store.ListSuccesses()
				default:
					err = store.Set(fmt.Sprintf("key%d-%d", g, i), []byte("v"))
				}
				if err == badger.ErrDBClosed {
					return
				}
				assert.NoError(t, err)
			}
		}(g)
	}
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, store.Close())
	wg.Wait()
	assert.Equal(t, badger.ErrDBClosed, store.DropPrefix([]byte(SUCCESS_PREFIX)))
}
