package node

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProvenRoutes(t *testing.T) {
	n := &Node{}
	assert.Nil(t, n.ProvenRoute("1x1x1", "9x1x1", false))
	route := []string{"1x1x1/0", "5x1x1/1", "9x1x1/0"}
	n.RememberRoute("1x1x1", "9x1x1", route)
	route[1] = "changed by the caller"
	want := []string{"1x1x1/0", "5x1x1/1", "9x1x1/0"}
	assert.Equal(t, want, n.ProvenRoute("1x1x1", "9x1x1", false))
	assert.Equal(t, want, n.ProvenRoute("1x1x1", "9x1x1", false), "a look leaves it")
	assert.Nil(t, n.ProvenRoute("9x1x1", "1x1x1", false), "the other way round is another pair")
	assert.Nil(t, n.ProvenRoute("1x1x1", "8x1x1", false))
	assert.Equal(t, want, n.ProvenRoute("1x1x1", "9x1x1", true))
	assert.Nil(t, n.ProvenRoute("1x1x1", "9x1x1", false), "taken")

	// too old: forgotten
	n.RememberRoute("1x1x1", "9x1x1", want)
	r := n.proven.routes["1x1x1>9x1x1"]
	r.at = time.Now().Add(-ROUTE_MEMORY - time.Second)
	n.proven.routes["1x1x1>9x1x1"] = r
	assert.Nil(t, n.ProvenRoute("1x1x1", "9x1x1", false))
	assert.Empty(t, n.proven.routes)
	r.at = time.Now().Add(-ROUTE_MEMORY + time.Minute)
	n.proven.routes["1x1x1>9x1x1"] = r
	assert.Equal(t, want, n.ProvenRoute("1x1x1", "9x1x1", false))
}
