package recipe

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	pathutil "path"

	"github.com/samuel/go-zookeeper/zk"
)

// CacheState state for TreeCache.
type CacheState int32

const (
	// CacheLatent the state when tree cache just created.
	CacheLatent CacheState = iota
	// CacheStarted the state when tree cache started.
	CacheStarted
	// CacheClosed the state when tree cache closed.
	CacheClosed
)

var (
	// ErrAlreadStarted tree cache alread started.
	ErrAlreadStarted = errors.New("Alread started")
)

// Listener for TreeCache changed.
type Listener interface {
	// Event execute when an event pushed.
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

// NewTreeCache create a new TreeCache.
func NewTreeCache(conn *zk.Conn, evt <-chan zk.Event, path string) *TreeCache {
	tc := &TreeCache{
		state:       CacheLatent,
		mu:          &sync.RWMutex{},
		initialized: &atomic.Value{},
		evt:         evt,
		client:      conn,
	}

	tc.root = NewTreeNode(tc, nil, pathutil.Clean(path), 0)
	tc.initialized.Store(false)
	return tc
}

// Start starts the cache.
// The cache is not started automatically. You must call this method.
// After a cache started, all changes of subtree will be synchronized
// from the ZooKeeper server. Events will be fired for those activity.
func (tc *TreeCache) Start() error {
	tc.mu.Lock()
	if tc.state != CacheLatent {
		tc.mu.Unlock()
		return ErrAlreadStarted
	}
	tc.state = CacheLatent
	tc.mu.Unlock()

	state := tc.client.State()
	if state == zk.StateConnected || state == zk.StateHasSession {
		have, _, err := tc.client.Exists(tc.root.path)
		if err != nil {
			return err
		}
		if !have {
			_, err := tc.client.Create(tc.root.path, []byte{}, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				return err
			}
		}
		tc.root.wasCreated()
	}
	return nil
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

// Listen registers a function to listen the cache events.
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
	node := tc.findNode(path)
	if node == nil {
		return nil
	}
	return node.Data()
}

// GetChildren get the children of zookeeper path.
func (tc *TreeCache) GetChildren(path string) []string {
	node := tc.findNode(path)
	if node == nil {
		return nil
	}
	return node.getChildren()
}

func (tc *TreeCache) findNode(path string) *TreeNode {
	path = pathutil.Clean(path)
	if !strings.HasPrefix(path, tc.root.path) {
		return nil
	}
	paths := strings.Split(strings.TrimPrefix(path, tc.root.path), "/")
	current := tc.root
	for _, p := range paths[1:] {
		current = current.getChild(p)
		if current == nil {
			return nil
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

// ListenFunc make a function to be a Listener.
type ListenFunc func(*TreeEvent)

// Event execute when an event pushed.
func (lf ListenFunc) Event(event *TreeEvent) {
	lf(event)
}
