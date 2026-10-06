package node

import (
	"circular/graph"
	"encoding/json"
	"github.com/dgraph-io/badger/v4"
	"github.com/elementsproject/glightning/glightning"
	"log"
	"sync"
	"time"
)

const (
	FOURTEEN_DAYS = 14 * 24 * time.Hour
)

// Store is the plugin's Badger database. Its methods are safe to call while
// it closes: they wait for Close, or return badger.ErrDBClosed after it.
// (Badger itself panics on a read that starts while it is closing.)
type Store struct {
	db     *badger.DB
	lock   sync.RWMutex // held for reading by every operation, for writing by Close
	closed bool
}

func NewDB(path string) *Store {
	options := badger.DefaultOptions(path)
	options.Logger = nil
	database, err := badger.Open(options)
	if err != nil {
		log.Fatalf("Error opening database: %v\n", err)
	}
	return &Store{
		db: database,
	}
}

// Close closes the database once the operations in progress are done.
// Further operations return badger.ErrDBClosed.
func (s *Store) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// open takes the read lock for an operation, and returns false, with the
// lock released, once the database is closed.
func (s *Store) open() bool {
	s.lock.RLock()
	if s.closed {
		s.lock.RUnlock()
		return false
	}
	return true
}

// DropPrefix deletes every key that starts with one of prefixes.
func (s *Store) DropPrefix(prefixes ...[]byte) error {
	if !s.open() {
		return badger.ErrDBClosed
	}
	defer s.lock.RUnlock()
	return s.db.DropPrefix(prefixes...)
}

// Every key is allowed to stay in the db for at most 14 days
func (s *Store) Set(key string, value []byte) error {
	if !s.open() {
		return badger.ErrDBClosed
	}
	defer s.lock.RUnlock()

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry([]byte(key), value).WithTTL(FOURTEEN_DAYS))
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	if !s.open() {
		return nil, badger.ErrDBClosed
	}
	defer s.lock.RUnlock()

	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		result, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *Store) Delete(key string) error {
	if !s.open() {
		return badger.ErrDBClosed
	}
	defer s.lock.RUnlock()

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) ListFailures() ([]glightning.SendPayFailure, error) {
	if !s.open() {
		return nil, badger.ErrDBClosed
	}
	defer s.lock.RUnlock()

	result := make([]glightning.SendPayFailure, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(FAILURE_PREFIX)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var sf glightning.SendPayFailure
			err = json.Unmarshal(v, &sf)
			if err != nil {
				return err
			}
			result = append(result, sf)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) ListSuccesses() ([]glightning.SendPaySuccess, error) {
	if !s.open() {
		return nil, badger.ErrDBClosed
	}
	defer s.lock.RUnlock()

	result := make([]glightning.SendPaySuccess, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(SUCCESS_PREFIX)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var sf glightning.SendPaySuccess
			err = json.Unmarshal(v, &sf)
			if err != nil {
				return err
			}
			result = append(result, sf)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) ListRoutes() ([]graph.PrettyRoute, error) {
	if !s.open() {
		return nil, badger.ErrDBClosed
	}
	defer s.lock.RUnlock()

	result := make([]graph.PrettyRoute, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(ROUTE_PREFIX)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var pr graph.PrettyRoute
			err = json.Unmarshal(v, &pr)
			if err != nil {
				return err
			}
			result = append(result, pr)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (n *Node) SaveToDb(key string, value any) error {
	if !n.saveStats {
		return nil
	}

	b, err := json.Marshal(value)
	if err != nil {
		n.Logln(glightning.Unusual, err)
		return err
	}

	err = n.DB.Set(key, b)
	if err != nil {
		n.Logln(glightning.Unusual, err)
		return err
	}

	return nil
}
