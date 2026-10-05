package node

import (
	"circular/graph"
	"circular/util"
	"encoding/json"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"os"
	"time"
)

// LoadGraphFromFile loads the graph saved by SaveGraphToFile. If that copy is
// missing or cannot be decoded, for example because a crash truncated it, it
// falls back to the previous copy. If neither loads, it returns
// util.ErrNoGraphToLoad and the graph is rebuilt from gossip.
func (n *Node) LoadGraphFromFile(dir, filename string) error {
	defer util.TimeTrack(time.Now(), "graph.LoadGraphFromFile", n.Logf)

	path := dir + "/" + filename
	for _, candidate := range []string{path, path + ".old"} {
		g, err := loadGraph(candidate)
		if err != nil {
			level := glightning.Unusual
			if os.IsNotExist(err) {
				level = glightning.Debug
			}
			n.Logln(level, "unable to load graph data from ", candidate, ": ", err)
			continue
		}
		n.Graph = g
		n.Logln(glightning.Info, "graph loaded successfully from ", candidate)
		return nil
	}
	return util.ErrNoGraphToLoad
}

func loadGraph(path string) (*graph.Graph, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	g := graph.NewGraph()
	if err := json.NewDecoder(file).Decode(g); err != nil {
		return nil, err
	}
	for id, c := range g.Channels {
		if c == nil || c.Channel == nil {
			return nil, fmt.Errorf("channel %s has no data", id)
		}
		g.AddChannel(c)
	}
	return g, nil
}

func (n *Node) SaveGraphToFile(dir, filename string) error {
	defer util.TimeTrack(time.Now(), "graph.SaveGraphToFile", n.Logf)

	// check if dir exists, otherwise create it
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			return err
		}
	}

	filename = dir + "/" + filename
	if err := n.serializeToFile(filename); err != nil {
		return err
	}

	// Rotate: current → .old, then .tmp → current. A crash in between leaves
	// .old, which LoadGraphFromFile falls back to.
	if _, err := os.Stat(filename); err == nil {
		if err := os.Rename(filename, filename+".old"); err != nil {
			return err
		}
	}
	if err := os.Rename(filename+".tmp", filename); err != nil {
		return err
	}
	syncDir(dir)

	return nil
}

// serializeToFile writes the graph to filename.tmp and flushes it to disk, so
// that a crash cannot leave a truncated file in place of the current copy.
func (n *Node) serializeToFile(filename string) error {
	tmp := filename + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}

	n.Graph.Lock()
	err = json.NewEncoder(file).Encode(n.Graph)
	n.Graph.Unlock()

	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// syncDir flushes a directory's entries, making renames in it durable. Not
// every platform supports it, so failures are ignored.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}
