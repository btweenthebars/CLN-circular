package main

import (
	"circular/node"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/virtuald/go-paniclog"
	"log"
	"os"
	"path/filepath"
	"time"
)

var (
	lightning *glightning.Lightning
	plugin    *glightning.Plugin
)

// This is called after the plugin starts up successfully
func onInit(plugin *glightning.Plugin, options map[string]glightning.Option, config *glightning.Config) {
	circularDir := config.LightningDir + "/" + node.CIRCULAR_DIR
	// check if dir exists, otherwise create it
	if _, err := os.Stat(circularDir); os.IsNotExist(err) {
		if err := os.Mkdir(circularDir, 0755); err != nil {
			log.Fatalln(err)
		}
	}

	// we redirect stderr to a file, so that we can have panic logs logged in a file
	if err := redirectStderr(circularDir); err != nil {
		log.Fatalln(err)
	}

	lightning = glightning.NewLightning()
	if err := lightning.StartUp(config.RpcFile, config.LightningDir); err != nil {
		log.Fatalln("error starting plugin: ", err)
	}

	node.GetNode().Init(lightning, plugin, options, config)
	log.Printf("circular successfully init'd!\n")
}

func main() {
	plugin = glightning.NewPlugin(onInit)
	registerOptions(plugin)
	registerMethods(plugin)
	registerSubscriptions(plugin)
	// No htlc_accepted hook: rebalances pay a real invoice, so lightningd settles
	// the incoming HTLC itself after checking its payment secret and amount.

	err := plugin.Start(os.Stdin, os.Stdout)
	// lightningd closed the connection without a shutdown notification
	node.GetNode().Close()
	if err != nil {
		log.Fatalln(err)
	}
}

const (
	stderrLog        = "stderr.log"
	stderrLogMaxSize = 10 << 20 // bytes: a larger log is moved to stderr.log.old at start
)

func redirectStderr(dir string) error {
	f, err := openStderrLog(dir, time.Now())
	if err != nil {
		return err
	}

	_, err = paniclog.RedirectStderr(f)
	if err != nil {
		return err
	}
	return nil
}

// openStderrLog opens dir/stderr.log for appending. Each start used to create
// a new stderr-<time>.log that nothing removed. The log is moved to
// stderr.log.old once it passes stderrLogMaxSize, and the old per-start files
// that stayed empty, as they almost all do, are removed.
func openStderrLog(dir string, now time.Time) (*os.File, error) {
	path := filepath.Join(dir, stderrLog)
	if info, err := os.Stat(path); err == nil && info.Size() > stderrLogMaxSize {
		if err := os.Rename(path, path+".old"); err != nil {
			return nil, err
		}
	}

	if old, err := filepath.Glob(filepath.Join(dir, "stderr-*.log")); err == nil {
		for _, name := range old {
			if info, err := os.Stat(name); err == nil && info.Mode().IsRegular() && info.Size() == 0 {
				_ = os.Remove(name)
			}
		}
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(f, "--- circular started at %s ---\n", now.UTC().Format(time.RFC3339))
	return f, nil
}
