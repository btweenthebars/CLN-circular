package node

import (
	"circular/graph"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const nodeA = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestAvoidKey(t *testing.T) {
	for in, want := range map[string]string{nodeA: nodeA, " " + strings.ToUpper(nodeA) + " ": nodeA,
		"123x4x5": "123x4x5", "123x0x0": "123x0x0", "123x0x10/1": "123x0x10/1", "123x4x5/1": "123x4x5/1", "123X4X5/0": "123x4x5/0"} {
		got, err := AvoidKey(in)
		assert.NoError(t, err, in)
		assert.Equal(t, want, got)
	}
	for _, bad := range []string{"", "CoinGate", "04" + nodeA[2:], nodeA[:65], "1x2", "1x2x3/2", "1x2x3/", "a1x2x3",
		"0812345x12x0", "812345x012x0", "812345x12x00/1"} { // leading zeros never match the graph's keys
		_, err := AvoidKey(bad)
		assert.Error(t, err, bad)
	}
	assert.Equal(t, []string{nodeA, "1x2x3/0", "1x2x3/1", "4x5x6/1"}, ExcludeKeys([]string{nodeA, "1x2x3", "4x5x6/1"}))
}

func TestTheAvoidList(t *testing.T) {
	dir := t.TempDir()
	n := &Node{}
	assert.NoError(t, n.LoadAvoidList(dir), "no file: an empty list")
	assert.Empty(t, n.AvoidedKeys())
	res, err := n.AvoidCall(dir, &AvoidCmd{})
	assert.NoError(t, err)
	assert.Equal(t, 0, res.Count)
	assert.NotNil(t, res.Avoid)
	assert.Empty(t, res.Error)

	res, err = n.AvoidCall(dir, &AvoidCmd{Add: strings.ToUpper(nodeA), Note: " drained hub "})
	assert.NoError(t, err)
	assert.Equal(t, "added "+nodeA, res.Changed)
	assert.Equal(t, 1, res.Count)
	assert.Equal(t, "drained hub", res.Avoid[0].Note)
	added := res.Avoid[0].Added
	assert.NotZero(t, added)
	_, err = n.AvoidCall(dir, &AvoidCmd{Add: "9x9x9"})
	assert.NoError(t, err)
	assert.Equal(t, []string{nodeA, "9x9x9"}, n.AvoidedKeys())
	// added again without a note: the note and the time kept
	e := n.avoidList.entries[nodeA]
	e.Added, added = 1000, 1000
	n.avoidList.entries[nodeA] = e
	res, _ = n.AvoidCall(dir, &AvoidCmd{Add: nodeA})
	assert.Equal(t, "drained hub", res.Avoid[0].Note)
	assert.Equal(t, added, res.Avoid[0].Added)
	res, _ = n.AvoidCall(dir, &AvoidCmd{Add: nodeA, Note: "new"})
	assert.Equal(t, "new", res.Avoid[0].Note)

	// kept across a restart
	again := &Node{}
	assert.NoError(t, again.LoadAvoidList(dir))
	assert.Equal(t, []string{nodeA, "9x9x9"}, again.AvoidedKeys())

	res, err = n.AvoidCall(dir, &AvoidCmd{Remove: "9x9x9"})
	assert.NoError(t, err)
	assert.Equal(t, "removed 9x9x9", res.Changed)
	assert.Equal(t, []string{nodeA}, n.AvoidedKeys())
	_, err = n.AvoidCall(dir, &AvoidCmd{Remove: "9x9x9"})
	assert.EqualError(t, err, "9x9x9 is not on the list (remove an entry by its key as the list shows it)")
	_, err = n.AvoidCall(dir, &AvoidCmd{Add: "CoinGate"})
	assert.Error(t, err)
	_, err = n.AvoidCall(dir, &AvoidCmd{Add: nodeA, Remove: nodeA})
	assert.EqualError(t, err, "add or remove, not both")
	assert.Equal(t, []string{nodeA}, n.AvoidedKeys())
	assert.NoError(t, n.AvoidListError())

	// a change that cannot be saved changes nothing
	_, err = n.AvoidCall(filepath.Join(dir, "missing", "dir"), &AvoidCmd{Add: "7x7x7"})
	assert.Error(t, err)
	_, err = n.AvoidCall("", &AvoidCmd{Add: "7x7x7"})
	assert.Error(t, err)
	assert.Equal(t, []string{nodeA}, n.AvoidedKeys())

	// a file that cannot be read: rebalances refused, and not written over
	path := filepath.Join(dir, AVOID_FILE)
	for _, broken := range []string{"{not json", `[{"key": "CoinGate"}]`} {
		assert.NoError(t, os.WriteFile(path, []byte(broken), 0o600))
		b := &Node{}
		assert.Error(t, b.LoadAvoidList(dir))
		assert.Contains(t, b.AvoidListError().Error(), "the avoid list cannot be read")
		assert.NotContains(t, b.AvoidedKeys(), "", "a bad key is not kept")
		res, err = b.AvoidCall(dir, &AvoidCmd{}) // the list says it is broken, not just shorter
		assert.NoError(t, err)
		assert.Contains(t, res.Error, "the avoid list cannot be read")
		_, err = b.AvoidCall(dir, &AvoidCmd{Add: "7x7x7"})
		assert.Contains(t, err.Error(), "not written over")
		data, _ := os.ReadFile(path)
		assert.Equal(t, broken, string(data))
	}
}

// A node the graph knows by its alias; none (not its id) when it does not.
func TestAvoidListAliases(t *testing.T) {
	dir := t.TempDir()
	g := graph.NewGraph()
	g.Aliases[nodeA] = "Hub"
	n := &Node{Graph: g}
	nodeB := "03" + nodeA[2:]
	for _, k := range []string{nodeA, nodeB, "1x2x3"} {
		_, err := n.AvoidCall(dir, &AvoidCmd{Add: k})
		assert.NoError(t, err)
	}
	res, _ := n.AvoidCall(dir, &AvoidCmd{})
	assert.Equal(t, []string{"Hub", "", ""}, []string{res.Avoid[0].Alias, res.Avoid[1].Alias, res.Avoid[2].Alias})
}
