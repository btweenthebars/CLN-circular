package graph

import (
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"math"
	"sync"
	"time"
)

const (
	FILE                                = "graph.json"
	DEFAULT_GRAPH_REFRESH_INTERVAL      = 10      // minutes
	PRUNING_INTERVAL               uint = 1209600 // 14 days
)

// Edge contains All the SCIDs of the channels going from nodeA to nodeB
type Edge []string

type InboundFee struct {
	BaseFee int32 `json:"base_fee"`
	FeeRate int32 `json:"fee_rate"`
}

// Graph is the lightning network graph from the perspective of our node
// It has been built from the gossip received by lightningd.
// To access the edges flowing into a node, use: g.Inbound[node]
// To access an edge into nodeA from nodeB, use: g.Inbound[nodeA][nodeB]
// * an edge consists of an array of SCIDs between nodeA and nodeB
// To access a channel via channelId (scid/direction). use: g.Channels[channelId]
type Graph struct {
	Channels          map[string]*Channel        `json:"channels"`
	Inbound           map[string]map[string]Edge `json:"-"`
	Aliases           map[string]string          `json:"-"`
	InboundFees       map[string]*InboundFee     `json:"inbound_fees"`
	adjacencyListLock *sync.RWMutex
	channelsLock      *sync.RWMutex
	aliasesLock       *sync.RWMutex
	inboundFeesLock   *sync.RWMutex
}

func NewGraph() *Graph {
	return &Graph{
		Channels:          make(map[string]*Channel),
		Inbound:           make(map[string]map[string]Edge),
		Aliases:           make(map[string]string),
		InboundFees:       make(map[string]*InboundFee),
		adjacencyListLock: &sync.RWMutex{},
		channelsLock:      &sync.RWMutex{},
		aliasesLock:       &sync.RWMutex{},
		inboundFeesLock:   &sync.RWMutex{},
	}
}

func (g *Graph) Lock() {
	g.channelsLock.Lock()
	g.adjacencyListLock.Lock()
	g.aliasesLock.Lock()
	g.inboundFeesLock.Lock()
}

func (g *Graph) Unlock() {
	g.inboundFeesLock.Unlock()
	g.aliasesLock.Unlock()
	g.adjacencyListLock.Unlock()
	g.channelsLock.Unlock()
}

func (g *Graph) LockAliases() {
	g.aliasesLock.Lock()
}

func (g *Graph) UnlockAliases() {
	g.aliasesLock.Unlock()
}

func allocate(links *map[string]map[string]Edge, from, to string) {
	if (*links)[from] == nil {
		(*links)[from] = make(map[string]Edge)
	}
	if (*links)[from][to] == nil {
		(*links)[from][to] = make([]string, 0)
	}
}

// AddChannel lists c in the adjacency list, once: it is called again for the
// same channel on every peer refresh.
func (g *Graph) AddChannel(c *Channel) {
	allocate(&g.Inbound, c.Destination, c.Source)
	edge := g.Inbound[c.Destination][c.Source]
	listed := false
	for _, scid := range edge {
		if scid == c.ShortChannelId {
			listed = true
			break
		}
	}
	if !listed {
		g.Inbound[c.Destination][c.Source] = append(edge, c.ShortChannelId)
	}

	if c.maxHtlcMsat == 0 {
		c.maxHtlcMsat = c.HtlcMaximumMilliSatoshis.MSat()
	}
	if c.minHtlcMsat == 0 {
		c.minHtlcMsat = c.HtlcMinimumMilliSatoshis.MSat()
	}
}

// RefreshChannels adds or refreshes the channels in channelList, keeping what
// was learnt about their liquidity.
func (g *Graph) RefreshChannels(channelList []*glightning.Channel) {
	g.channelsLock.Lock()
	g.adjacencyListLock.Lock()
	defer g.channelsLock.Unlock()
	defer g.adjacencyListLock.Unlock()

	g.refreshChannels(channelList, nil)
}

// SyncChannels makes the graph match channelList, a full listchannels
// snapshot taken at asOf (Unix time): it refreshes the listed channels and
// removes the others, which lightningd forgets once they close. The channels
// in keep, our own, stay: unannounced ones are never listed. So do channels
// updated at asOf or later, such as ours added after keep was read. It
// returns the number removed, or -1 when the snapshot holds less than half of
// the other channels in the graph: such a snapshot is likely incomplete, so
// nothing is removed.
func (g *Graph) SyncChannels(channelList []*glightning.Channel, keep map[string]bool, asOf uint) int {
	g.channelsLock.Lock()
	g.adjacencyListLock.Lock()
	defer g.channelsLock.Unlock()
	defer g.adjacencyListLock.Unlock()

	others := 0
	for channelId := range g.Channels {
		if !keep[channelId] {
			others++
		}
	}
	if len(channelList) < others/2 {
		g.refreshChannels(channelList, nil)
		return -1
	}

	listed := make(map[string]struct{}, len(channelList))
	g.refreshChannels(channelList, listed)
	removed := 0
	for channelId, c := range g.Channels {
		if _, ok := listed[channelId]; ok || keep[channelId] || c.LastUpdate >= asOf {
			continue
		}
		g.DeleteChannel(c)
		removed++
	}
	return removed
}

// refreshChannels does RefreshChannels with channelsLock and adjacencyListLock
// held, and adds the id of each listed channel to listed, if not nil.
func (g *Graph) refreshChannels(channelList []*glightning.Channel, listed map[string]struct{}) {
	// we need to do NewChannel and not only update the liquidity because of gossip updates
	for _, c := range channelList {
		var channel *Channel
		channelId := c.ShortChannelId + "/" + util.GetDirection(c.Source, c.Destination)
		if listed != nil {
			listed[channelId] = struct{}{}
		}
		// if the channel did not exist prior to this refresh estimate its initial liquidity to be 50/50
		if existing, ok := g.Channels[channelId]; !ok {
			channel = NewChannel(c, c.AmountMsat.MSat()/2, 0)
			g.AddChannel(channel)
		} else if existing.LastUpdate > c.LastUpdate {
			// newer than the list: from a gossip update applied since, or one
			// of our channels, refreshed from listpeerchannels
			continue
		} else {
			channel = NewChannel(c, existing.Liquidity, existing.Timestamp)
		}
		g.Channels[channelId] = channel
	}
}

// ChannelUpdate is the policy a channel_update announces for one direction of
// a channel.
type ChannelUpdate struct {
	ChannelId       string // "scid/direction"
	Timestamp       uint
	MessageFlags    byte
	ChannelFlags    byte
	CltvDelta       uint
	HtlcMinimumMsat uint64
	HtlcMaximumMsat uint64 // present when MessageFlags&1 is set
	BaseFeeMsat     uint32
	FeePPM          uint32
}

// ApplyChannelUpdate gives a known channel the policy of a newer
// channel_update from gossip, so that fee changes and disables take effect
// without waiting for the next listchannels refresh. The channel is replaced,
// not changed in place: routes built earlier keep the policy they were priced
// with. It reports whether the channel was updated.
func (g *Graph) ApplyChannelUpdate(u *ChannelUpdate) bool {
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	old, ok := g.Channels[u.ChannelId]
	if !ok || u.Timestamp <= old.LastUpdate {
		return false
	}
	updated := *old.Channel
	updated.LastUpdate = u.Timestamp
	updated.MessageFlags = uint(u.MessageFlags)
	updated.ChannelFlags = uint(u.ChannelFlags)
	updated.IsActive = u.ChannelFlags&2 == 0 // the disable bit
	updated.Delay = u.CltvDelta
	updated.BaseFeeMillisatoshi = uint64(u.BaseFeeMsat)
	updated.FeePerMillionth = uint64(u.FeePPM)
	updated.HtlcMinimumMilliSatoshis = glightning.AmountFromMSat(u.HtlcMinimumMsat)
	if u.MessageFlags&1 != 0 {
		updated.HtlcMaximumMilliSatoshis = glightning.AmountFromMSat(u.HtlcMaximumMsat)
	}
	g.Channels[u.ChannelId] = NewChannel(&updated, old.Liquidity, old.Timestamp)
	return true
}

func (g *Graph) RefreshAliases(nodes []*glightning.Node) {
	g.aliasesLock.Lock()
	defer g.aliasesLock.Unlock()

	for _, n := range nodes {
		g.Aliases[n.Id] = n.Alias
	}
}

func (g *Graph) PruneChannels() {
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()
	g.adjacencyListLock.Lock()
	defer g.adjacencyListLock.Unlock()

	// get current time in seconds
	now := uint(time.Now().Unix())

	// prune channels that are older than PRUNING_INTERVAL
	// TODO: remove closed channels, but might be worth waiting for glightning to implement channel_state_changed
	for _, c := range g.Channels {
		if c.LastUpdate+PRUNING_INTERVAL < now {
			g.DeleteChannel(c)
		}
	}

	// prune inbound fees for SCIDs no longer in the channel map
	g.pruneInboundFees()
}

// pruneInboundFees removes inbound fee entries whose SCID is no longer present
// in g.Channels. Must be called with channelsLock and inboundFeesLock both held.
func (g *Graph) pruneInboundFees() {
	g.inboundFeesLock.Lock()
	defer g.inboundFeesLock.Unlock()

	// build a set of SCIDs that still exist in the graph
	existing := make(map[string]bool, len(g.Channels))
	for channelId := range g.Channels {
		// channelId is "scid/direction"; extract scid
		for i := len(channelId) - 1; i >= 0; i-- {
			if channelId[i] == '/' {
				existing[channelId[:i]] = true
				break
			}
		}
	}

	for key := range g.InboundFees {
		// key is "scid/direction"; extract scid
		for i := len(key) - 1; i >= 0; i-- {
			if key[i] == '/' {
				if !existing[key[:i]] {
					delete(g.InboundFees, key)
				}
				break
			}
		}
	}
}

func (g *Graph) DeleteChannel(c *Channel) {
	// delete from channel map
	delete(g.Channels, c.ShortChannelId+"/"+util.GetDirection(c.Source, c.Destination))

	// delete from adjacency list
	for i, edge := range g.Inbound[c.Destination][c.Source] {
		if edge == c.ShortChannelId {
			g.Inbound[c.Destination][c.Source] = remove(g.Inbound[c.Destination][c.Source], i)
			break
		}
	}
}

// assumes valid input
func remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func (g *Graph) GetAlias(id string) string {
	g.aliasesLock.RLock()
	defer g.aliasesLock.RUnlock()

	if alias, ok := g.Aliases[id]; ok {
		return alias
	}
	return id
}

// OppositeChannelId returns the id of the other direction of channelId ("scid/direction").
func OppositeChannelId(channelId string) string {
	n := len(channelId)
	if n < 2 || channelId[n-2] != '/' {
		return channelId
	}
	if channelId[n-1] == '0' {
		return channelId[:n-1] + "1"
	}
	return channelId[:n-1] + "0"
}

// LearnUpperBound records that channelId failed to forward amount for lack of
// liquidity: it holds less than amount, so the other direction holds at least
// the rest of the capacity. Beliefs are only ever tightened.
func (g *Graph) LearnUpperBound(channelId string, amount uint64) {
	if amount == 0 {
		return
	}
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	now := time.Now().Unix()
	if c, ok := g.Channels[channelId]; ok {
		if c.Liquidity >= amount {
			c.Liquidity = amount - 1
		}
		c.Timestamp = now
	}
	if c, ok := g.Channels[OppositeChannelId(channelId)]; ok {
		capacity := c.AmountMsat.MSat()
		if capacity > amount && c.Liquidity < capacity-amount {
			c.Liquidity = capacity - amount
		}
		c.Timestamp = now
	}
}

// LearnLowerBound records that channelId forwarded amount: it holds at least
// amount, so the other direction holds at most the rest of the capacity.
func (g *Graph) LearnLowerBound(channelId string, amount uint64) {
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	now := time.Now().Unix()
	if c, ok := g.Channels[channelId]; ok {
		if c.Liquidity < amount {
			c.Liquidity = amount
		}
		c.Timestamp = now
	}
	if c, ok := g.Channels[OppositeChannelId(channelId)]; ok {
		capacity := c.AmountMsat.MSat()
		rest := uint64(0)
		if capacity > amount {
			rest = capacity - amount
		}
		if c.Liquidity > rest {
			c.Liquidity = rest
		}
		c.Timestamp = now
	}
}

// MarkUnusable records that channelId cannot forward anything (disabled,
// closed or unknown to its node) until its liquidity belief is reset. The
// other direction is left alone.
func (g *Graph) MarkUnusable(channelId string) {
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	if c, ok := g.Channels[channelId]; ok {
		c.Liquidity = 0
		c.Timestamp = time.Now().Unix()
	}
}

func (g *Graph) GetChannel(id string) (*Channel, error) {
	g.channelsLock.RLock()
	defer g.channelsLock.RUnlock()

	if _, ok := g.Channels[id]; !ok {
		return nil, util.ErrNoChannel
	}
	return g.Channels[id], nil
}

func (g *Graph) RefreshLiquidity(refreshThreshold time.Duration) int {
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	// get current time in seconds
	now := time.Now().Unix()
	hits := 0

	for _, c := range g.Channels {
		if c.Timestamp+int64(refreshThreshold.Seconds()) < now {
			c.ResetLiquidity()
			hits++
		}
	}

	return hits
}

func (g *Graph) SetInboundFee(channelId string, baseFee, feeRate int32) {
	g.inboundFeesLock.Lock()
	defer g.inboundFeesLock.Unlock()
	g.InboundFees[channelId] = &InboundFee{
		BaseFee: baseFee,
		FeeRate: feeRate,
	}
}

// DeleteInboundFee forgets the inbound fee of channelId and reports whether
// there was one.
func (g *Graph) DeleteInboundFee(channelId string) bool {
	g.inboundFeesLock.Lock()
	defer g.inboundFeesLock.Unlock()

	if _, ok := g.InboundFees[channelId]; !ok {
		return false
	}
	delete(g.InboundFees, channelId)
	return true
}

// GetInboundFee returns the inbound fee that the node c leads to charges for
// a payment arriving over c. As in LND, amount is what the node forwards plus
// its outbound fee for forwarding it.
func (g *Graph) GetInboundFee(c *Channel, amount uint64) int64 {
	g.inboundFeesLock.RLock()
	defer g.inboundFeesLock.RUnlock()

	return g.inboundFee(c.ShortChannelId+"/"+util.GetDirection(c.Source, c.Destination), amount)
}

// maxInboundFeeRate is the rate LND caps inbound fee rates to, either way.
const maxInboundFeeRate = 10 * 1000000

// inboundFee is GetInboundFee for callers that hold inboundFeesLock and know
// the channel id. It computes the fee as LND's InboundFee.CalcFee does: the
// rate is capped, and the proportional part rounds toward zero.
func (g *Graph) inboundFee(channelId string, amount uint64) int64 {
	fee, ok := g.InboundFees[channelId]
	if !ok {
		return 0
	}
	rate := int64(fee.FeeRate)
	if rate > maxInboundFeeRate {
		rate = maxInboundFeeRate
	} else if rate < -maxInboundFeeRate {
		rate = -maxInboundFeeRate
	}
	// whole millions of msat, then the rest: rate x amount overflows int64
	// from about 9.2 BTC at the capped rate
	millions, rest := amount/1000000, int64(amount%1000000)
	if rate != 0 && millions > uint64(math.MaxInt64/2/maxInboundFeeRate) {
		// beyond any real amount (about 4.6M BTC): saturate rather than wrap
		if rate > 0 {
			return math.MaxInt64
		}
		return math.MinInt64
	}
	return int64(fee.BaseFee) + int64(millions)*rate + rest*rate/1000000
}
