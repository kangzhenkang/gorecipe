package recipe

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/samuel/go-zookeeper/zk"
)

type CacheState int32

const (
	CacheLatent CacheState = iota
	CacheStarted
	CacheClosed
)

var (
	ErrAlreadStarted = errors.New("Alread started")
)

// Listener for TreeCache changed.
type Listener interface {
	Event(*TreeEvent)
}

// TreeCache the cache of a ZooKeeper subtree.
type TreeCache struct {
	root           *TreeNode
	client         *zk.Conn
	state          CacheState
	Listeners      []Listener
	mu             *sync.RWMutex
	outstandingOps int32
	initialized    *atomic.Value
	evt            <-chan zk.Event
}

// NewTreeeCache create a new TreeCache.
func NewTreeCache(conn *zk.Conn, evt <-chan zk.Event, path string) *TreeCache {
	tc := &TreeCache{
		state:       CacheLatent,
		mu:          &sync.RWMutex{},
		initialized: &atomic.Value{},
		evt:         evt,
		client:      conn,
	}
	tc.root = NewTreeNode(tc, nil, path, 0)
	tc.initialized.Store(false)
	return tc
}

// Start starts the cache.
// The cache is not started automatically. You must call this method.
// After a cache started, all changes of subtree will be synchronized
// from the ZooKeeper server. Events will be fired for those activity.
func (tc *TreeCache) Start() {
	tc.mu.Lock()
	if tc.state != CacheLatent {
		tc.mu.Unlock()
		panic(ErrAlreadStarted)
	}
	tc.state = CacheLatent
	tc.mu.Unlock()

	if tc.client.State() == zk.StateConnected {
		tc.root.wasCreated()
	}
}

// Close close the cache.
func (tc *TreeCache) Close() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.state == CacheStarted {
		tc.state = CacheClosed
		tc.root.wasDeleted()
	}
}

// Listener Registers a function to listen the cache events.
// The cache events are changes of local data. They are delivered from
// watching notifications in ZooKeeper session.
// This method can be use as a decorator.
func (tc *TreeCache) Listen(ls Listener) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.Listeners = append(tc.Listeners, ls)
}

// GetData gets data of a node from cache.
func (tc *TreeCache) GetData(path string) []byte {
	node := tc.findNode(strings.Split(path, "/"))
	if node == nil {
		return nil
	}
	return node.data
}

func (tc *TreeCache) GetChildren(path string) []string {
	node := tc.findNode(strings.Split(path, "/"))
	return node.getChildren()
}

func (tc *TreeCache) findNode(paths []string) *TreeNode {
	current := tc.root
	for _, p := range paths[1:] {
		current = current.getChild(p)
		if current == nil {
			return current
		}
	}
	return current
}

func (tc *TreeCache) isInitialized() bool {
	initialized, _ := tc.initialized.Load().(bool)
	return initialized
}

func (tc *TreeCache) setInitialized() {
	tc.initialized.Store(true)
}

func (tc *TreeCache) publishEvent(event *TreeEvent) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	for _, ls := range tc.Listeners {
		ls.Event(event)
	}
}

func (tc *TreeCache) sessionWatch() {
	// TODO Watch connection.
}

type ListenFunc func(*TreeEvent)

func (lf ListenFunc) Event(event *TreeEvent) {
	lf(event)
}
