package node

import (
	"circular/graph"
	"circular/util"
	"encoding/json"
	"errors"
	"github.com/elementsproject/glightning/glightning"
	"os"
	"path/filepath"
	"time"
)

const (
	// LEARNED_FILE holds what rebalances learned about channels' liquidity.
	LEARNED_FILE = "learned.json"
	// legacyGraphFile is the full graph earlier versions rewrote every graph
	// refresh: tens of MB each time. Its beliefs are migrated once, then it
	// is removed.
	legacyGraphFile = "graph.json"
	learnedVersion  = 1
)

// learnedState is the content of LEARNED_FILE.
type learnedState struct {
	Version   int                     `json:"version"`
	Liquidity map[string]graph.Belief `json:"liquidity"`
	// Avoid lists the nodes and channels rebalances avoid, until when (Unix time)
	Avoid map[string]int64 `json:"avoid,omitempty"`
	// Candidates records how our channels did as pull/push candidates
	Candidates map[string]CandidateRecord `json:"candidates,omitempty"`
}

// LoadLearned reads what earlier runs learned from dir/learned.json, falling
// back to its previous copy when it is missing or cannot be decoded (for
// example after a crash), then to an earlier version's graph.json. It
// reports whether the beliefs came from graph.json, which RemoveLegacyGraph
// can then remove. With nothing to read, it returns util.ErrNoGraphToLoad.
func (n *Node) LoadLearned(dir string) (state *learnedState, legacy bool, err error) {
	defer util.TimeTrack(time.Now(), "node.LoadLearned", n.Logf)

	path := filepath.Join(dir, LEARNED_FILE)
	for _, candidate := range []string{path, path + ".old"} {
		state, err := readLearned(candidate)
		if err != nil {
			n.logLoadError(candidate, err)
			continue
		}
		n.Logf(glightning.Info, "loaded %d liquidity beliefs from %s", len(state.Liquidity), candidate)
		return state, false, nil
	}

	for _, candidate := range []string{filepath.Join(dir, legacyGraphFile), filepath.Join(dir, legacyGraphFile+".old")} {
		beliefs, err := readLegacyGraph(candidate)
		if err != nil {
			n.logLoadError(candidate, err)
			continue
		}
		n.Logf(glightning.Info, "migrated %d liquidity beliefs from %s", len(beliefs), candidate)
		return &learnedState{Version: learnedVersion, Liquidity: beliefs}, true, nil
	}
	return nil, false, util.ErrNoGraphToLoad
}

func (n *Node) logLoadError(path string, err error) {
	level := glightning.Unusual
	if os.IsNotExist(err) {
		level = glightning.Debug
	}
	n.Logln(level, "unable to load ", path, ": ", err)
}

func readLearned(path string) (*learnedState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state learnedState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Version != learnedVersion {
		return nil, errors.New("unknown version")
	}
	return &state, nil
}

// readLegacyGraph reads the beliefs out of an earlier version's graph.json,
// without keeping the rest of the graph in memory.
func readLegacyGraph(path string) (map[string]graph.Belief, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var legacy struct {
		Channels map[string]struct {
			Channel struct {
				AmountMsat glightning.Amount `json:"amount_msat"`
			} `json:"channel"`
			Liquidity uint64 `json:"liquidity"`
			Timestamp int64  `json:"timestamp"`
		} `json:"channels"`
	}
	if err := json.NewDecoder(file).Decode(&legacy); err != nil {
		return nil, err
	}
	beliefs := make(map[string]graph.Belief)
	for id, c := range legacy.Channels {
		if c.Timestamp != 0 && c.Liquidity != c.Channel.AmountMsat.MSat()/2 {
			beliefs[id] = graph.Belief{Liquidity: c.Liquidity, Timestamp: c.Timestamp}
		}
	}
	return beliefs, nil
}

// RemoveLegacyGraph removes an earlier version's graph.json and its copies,
// once their beliefs are saved in learned.json.
func (n *Node) RemoveLegacyGraph(dir string) {
	for _, name := range []string{legacyGraphFile, legacyGraphFile + ".old", legacyGraphFile + ".tmp"} {
		path := filepath.Join(dir, name)
		if err := os.Remove(path); err == nil {
			n.Logln(glightning.Info, "removed ", path, ", replaced by ", LEARNED_FILE)
		}
	}
}

// SaveLearned writes what the graph has learned to dir/learned.json, keeping
// the previous copy as learned.json.old.
func (n *Node) SaveLearned(dir string) error {
	defer util.TimeTrack(time.Now(), "node.SaveLearned", n.Logf)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			return err
		}
	}
	data, err := json.Marshal(learnedState{Version: learnedVersion, Liquidity: n.Graph.Beliefs(),
		Avoid: n.Graph.AvoidList(), Candidates: n.candidateList()})
	if err != nil {
		return err
	}

	path := filepath.Join(dir, LEARNED_FILE)
	if err := writeSynced(path+".tmp", data); err != nil {
		return err
	}
	// Rotate: current → .old, then .tmp → current. A crash in between leaves
	// .old, which LoadLearned falls back to.
	if _, err := os.Stat(path); err == nil {
		if err := os.Rename(path, path+".old"); err != nil {
			return err
		}
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	syncDir(dir)
	return nil
}

// writeSynced writes data to path and flushes it to disk, so that a crash
// cannot leave a truncated file.
func writeSynced(path string, data []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(path)
	}
	return err
}

// syncDir flushes a directory's entries, making renames in it durable. Not
// every platform supports it, so failures are ignored.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}
