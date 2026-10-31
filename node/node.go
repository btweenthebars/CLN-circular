package node

import (
	"circular/graph"
	"circular/util"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/robfig/cron/v3"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CIRCULAR_DIR                     = "circular"
	DEFAULT_PEER_REFRESH_INTERVAL    = 30  // seconds
	DEFAULT_LIQUIDITY_RESET_INTERVAL = 300 // minutes
	DEFAULT_LIQUIDITY_HALF_LIFE      = 60  // minutes
	DEFAULT_RISK_PPM                 = 200 // ppm per unit of -ln(chance)
	DEFAULT_NODE_MEMORY              = 24  // hours: nodes' failure scores halve this often
	DEFAULT_RPC_TIMEOUT              = 60  // seconds
)

var (
	singleton *Node
	once      sync.Once
)

type Node struct {
	lightning         *glightning.Lightning
	plugin            *glightning.Plugin
	liquidityRefresh  time.Duration
	liquidityHalfLife time.Duration
	nodeMemory        time.Duration // circular-node-memory: nodes' failure scores halve this often; 0 off
	// RiskPPM is how much a route's chance of success weighs against its
	// fees: each unit of -ln(chance) of a channel costs this many ppm of its
	// amount (circular-risk-ppm; 0: routes are chosen by fees and hops alone)
	RiskPPM uint64
	// Defaults are what the rebalance commands use for the parameters a
	// call leaves out (the circular-attempts ... circular-max-fee startup
	// options)
	Defaults       Defaults
	initLock       *sync.Mutex
	saveStats      bool
	PeersLock      *sync.RWMutex
	Id             string
	Peers          map[string]*glightning.Peer
	scidToPeer     map[string]*glightning.Peer // reverse index: scid → peer, guarded by PeersLock
	peersReadAt    time.Time                   // when refreshPeers read the channels in Peers, guarded by PeersLock
	channelsReadAt map[string]time.Time        // later reads of one peer's channels, guarded by PeersLock
	connectedAt    map[string]time.Time        // when a notification last changed a peer's connection, guarded by PeersLock
	Graph          *graph.Graph
	DB             *Store
	dataDir        string        // lightning-dir/circular
	learnedLock    sync.Mutex    // guards pendingBeliefs and migrateLegacy, and serializes saves
	pendingBeliefs *learnedState // read at start, applied after the first graph refresh
	migrateLegacy  bool          // pendingBeliefs came from an earlier version's graph.json
	learnedApplied atomic.Bool   // the first graph refresh applied what was saved
	cron           *cron.Cron
	candidates     candidateRecords // how our channels did as pull/push candidates
	Stopped        atomic.Bool      // circular-stop: no new rebalances
	gossipStopped  atomic.Bool
	// earned caches what Earned read, per channel (EARN_CACHE_TTL)
	earned             map[string]earnedEntry
	earnedLock         sync.Mutex
	ActivePayments     map[string]*ActivePayment
	activePaymentsLock *sync.RWMutex
}

func GetNode() *Node {
	once.Do(func() {
		singleton = &Node{
			initLock:           &sync.Mutex{},
			PeersLock:          &sync.RWMutex{},
			Peers:              make(map[string]*glightning.Peer),
			scidToPeer:         make(map[string]*glightning.Peer),
			ActivePayments:     make(map[string]*ActivePayment),
			activePaymentsLock: &sync.RWMutex{},
		}
	})
	// This makes sure the node is not used until it is initialized or refreshed
	singleton.initLock.Lock()
	// unlock it right away.
	singleton.initLock.Unlock()
	return singleton
}

func (n *Node) Init(lightning *glightning.Lightning, plugin *glightning.Plugin, options map[string]glightning.Option, config *glightning.Config) {
	defer util.TimeTrack(time.Now(), "node.Init", n.Logf)
	n.initLock.Lock()
	defer n.initLock.Unlock()

	n.setOptions(lightning, plugin, options)
	n.Logf(glightning.Info, "circular %s starting", BuildVersion())

	n.Logln(glightning.Debug, "getting ID")
	info, err := n.lightning.GetInfo()
	if err != nil {
		log.Fatalln("GetInfo failed in init, exiting")
	}
	n.Id = info.Id

	n.Logln(glightning.Debug, "loading from file")
	n.Graph = graph.NewGraph()
	n.Graph.SetNodeMemory(n.nodeMemory)
	n.dataDir = config.LightningDir + "/" + CIRCULAR_DIR
	n.loadLearned()

	n.Logln(glightning.Debug, "refreshing peers")
	if err = n.refreshPeers(); err != nil {
		n.Logln(glightning.Unusual, "initial refreshPeers failed: ", err)
	}

	n.Logln(glightning.Debug, "refreshing graph")
	if err = n.refreshGraph(); err != nil {
		n.Logln(glightning.Unusual, "initial refreshGraph failed: ", err)
	}

	n.Logln(glightning.Debug, "opening database")
	n.DB = NewDB(config.LightningDir + "/" + CIRCULAR_DIR)

	n.Logln(glightning.Debug, "setting up cronjobs")
	n.setupCronJobs(options)

	n.Logln(glightning.Debug, "starting gossip store parser")
	go n.StartGossipParser(info.LightningDir, info.Network)

	n.Logln(glightning.Info, "node initialized")
}

// Close stops rebalancing and the gossip parser, and closes the database. It
// runs when lightningd shuts the plugin down.
func (n *Node) Close() {
	n.Stopped.Store(true)
	n.StopGossipParser()
	if n.cron != nil {
		// let a running refresh finish, for a little while
		select {
		case <-n.cron.Stop().Done():
		case <-time.After(5 * time.Second):
		}
	}
	// keep what was learned since the last graph refresh, unless what the
	// previous run saved has not been applied yet: saving now would lose it
	if n.Graph != nil && n.dataDir != "" && n.learnedApplied.Load() {
		n.learnedLock.Lock()
		if err := n.SaveLearned(n.dataDir); err != nil {
			n.Logf(glightning.Unusual, "error saving liquidity beliefs: %+v", err)
		}
		n.learnedLock.Unlock()
	}
	if n.DB != nil {
		if err := n.DB.Close(); err != nil {
			n.Logln(glightning.Unusual, "error closing the database: ", err)
		}
	}
}

// loadLearned reads what earlier runs learned. The beliefs are applied once
// the first graph refresh has built the graph from listchannels.
func (n *Node) loadLearned() {
	state, legacy, err := n.LoadLearned(n.dataDir)
	if err != nil {
		n.Logln(glightning.Info, "no saved liquidity beliefs: starting from the defaults")
		return
	}
	n.learnedLock.Lock()
	n.pendingBeliefs = state
	n.migrateLegacy = legacy
	n.learnedLock.Unlock()
}

// applyLearned applies the beliefs loadLearned read, once the graph has its
// channels, and saves what the graph knows.
func (n *Node) applyLearned() {
	n.learnedLock.Lock()
	defer n.learnedLock.Unlock()

	if n.pendingBeliefs != nil {
		since := time.Now().Add(-n.liquidityRefresh).Unix()
		applied := n.Graph.ApplyBeliefs(n.pendingBeliefs.Liquidity, since)
		n.Logf(glightning.Info, "applied %d of %d saved liquidity beliefs", applied, len(n.pendingBeliefs.Liquidity))
		for key, until := range n.pendingBeliefs.Avoid {
			n.Graph.Avoid(key, until)
		}
		n.restoreCandidates(n.pendingBeliefs.Candidates)
		if len(n.pendingBeliefs.Nodes) > 0 {
			n.Logf(glightning.Info, "remembered %d of %d nodes that failed payments",
				n.Graph.ApplyNodeScores(n.pendingBeliefs.Nodes), len(n.pendingBeliefs.Nodes))
		}
		n.pendingBeliefs = nil
	}
	// records of channels that closed would otherwise stay forever; with no
	// peers read yet, there is nothing to compare with
	if n.PeersLock != nil {
		if open := n.localChannelIds(); len(open) > 0 {
			if removed := n.pruneCandidates(open); removed > 0 {
				n.Logf(glightning.Debug, "forgot the candidate records of %d closed channels", removed)
			}
		}
	}
	n.learnedApplied.Store(true)
	if err := n.SaveLearned(n.dataDir); err != nil {
		n.Logf(glightning.Unusual, "error saving liquidity beliefs: %+v", err)
		return
	}
	if n.migrateLegacy {
		n.RemoveLegacyGraph(n.dataDir)
		n.migrateLegacy = false
	}
}

func (n *Node) setOptions(lightning *glightning.Lightning, plugin *glightning.Plugin, options map[string]glightning.Option) {
	n.lightning = lightning
	n.plugin = plugin
	n.Logln(glightning.Info, "initializing node")

	n.liquidityRefresh = time.Duration(options["circular-liquidity-refresh"].GetValue().(int)) * time.Minute
	n.Logln(glightning.Debug, "liquidity refresh interval: ", int(n.liquidityRefresh.Minutes()), " minutes")

	n.liquidityHalfLife = time.Duration(options["circular-liquidity-halflife"].GetValue().(int)) * time.Minute
	n.Logln(glightning.Debug, "liquidity half-life: ", int(n.liquidityHalfLife.Minutes()), " minutes")

	if hours := options["circular-node-memory"].GetValue().(int); hours > 0 {
		n.nodeMemory = time.Duration(hours) * time.Hour
	}
	n.Logln(glightning.Debug, "node memory half-life: ", int(n.nodeMemory.Hours()), " hours")

	if risk := options["circular-risk-ppm"].GetValue().(int); risk > 0 {
		n.RiskPPM = uint64(risk)
	}
	n.Logln(glightning.Debug, "risk ppm: ", n.RiskPPM)

	n.Defaults = Defaults{
		Attempts:      options["circular-attempts"].GetValue().(int),
		MaxHops:       options["circular-max-hops"].GetValue().(int),
		MaxNodeFails:  options["circular-max-node-fails"].GetValue().(int),
		MaxFailStreak: options["circular-max-fail-streak"].GetValue().(int),
		MaxTime:       options["circular-max-time"].GetValue().(int),
		MaxFee:        options["circular-max-fee"].GetValue().(int),
		EarnDays:      options["circular-earn-days"].GetValue().(int),
	}
	if ratio := options["circular-fee-ratio"].GetValue().(string); ratio != "" {
		if v, err := strconv.ParseFloat(ratio, 64); err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
			n.Logln(glightning.Unusual, "circular-fee-ratio is not a number: ", ratio)
		} else {
			n.Defaults.FeeRatio = v
		}
	}
	n.Logf(glightning.Debug, "defaults: %+v", n.Defaults)

	n.saveStats = options["circular-save-stats"].GetValue().(bool)
	n.Logln(glightning.Debug, "save stats: ", n.saveStats)

	n.lightning.SetTimeout(DEFAULT_RPC_TIMEOUT)
}

func (n *Node) Logf(level glightning.LogLevel, format string, v ...any) {
	if n.plugin != nil {
		n.plugin.Log(util.GetCallInfo()+fmt.Sprintf(format, v...), level)
	} else {
		log.Printf(format, v...)
	}
}

func (n *Node) Logln(level glightning.LogLevel, v ...any) {
	if n.plugin != nil {
		n.plugin.Log(util.GetCallInfo()+fmt.Sprint(v...), level)
	} else {
		log.Println(v...)
	}
}

func (n *Node) RefreshChannel(channel *graph.Channel) {
	// this is needed to get up-to-date fees and channel info such as state
	channels, err := n.lightning.GetChannel(channel.ShortChannelId)
	if err != nil {
		n.Logln(glightning.Unusual, err)
		return
	}
	n.Graph.RefreshChannels(channels)
}

// Defaults are the startup options for the parameters a rebalance call
// leaves out. A field at 0 leaves the command's built-in default.
type Defaults struct {
	Attempts      int // payments per split (circular-attempts)
	MaxHops       int // channels per route (circular-max-hops)
	MaxNodeFails  int // per node and split (circular-max-node-fails)
	MaxFailStreak int // failed payments in a row per run; below 0: no limit (circular-max-fail-streak)
	MaxTime       int // seconds per run; 0 or less: no limit (circular-max-time)
	MaxFee        int // sats per run; 0 or less: no budget (circular-max-fee)
	// FeeRatio: circular-pull/push pay at most this share of what the
	// filled channel earns, less our fee on the drained one; 0 or less: off
	// (circular-fee-ratio)
	FeeRatio float64
	EarnDays int // days of forwards for what a channel earns; 0: DEFAULT_EARN_DAYS (circular-earn-days)
}

// GetDefaults returns n.Defaults, or none for a nil node.
func (n *Node) GetDefaults() Defaults {
	if n == nil {
		return Defaults{}
	}
	return n.Defaults
}
