package recipe

import (
	"context"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/samuel/go-zookeeper/zk"
)

// TreeEventType TreeEvent type.
type TreeEventType int

// TreeNodeState tree node state.
type TreeNodeState int

const (
	// Unknow unknow
	Unknow TreeEventType = iota
	// NodeAdd a node was added.
	NodeAdd
	// NodeUpdate a node's data was chagned.
	NodeUpdate
	// NodeRemoved a node was removed from the tree.
	NodeRemoved
	// ConnectionSuspended called when the connection has changed to SUSPENDED
	ConnectionSuspended
	// ConnectionReconnected called when the connection has changed to RECONNECTED.
	ConnectionReconnected
	// ConnectionLost called when the connection has changed to LOST.
	ConnectionLost
	// Initialized posted after the initial cache has been fully populated.
	Initialized

	// TreeNodePending the state of TreeNode that just created.
	TreeNodePending TreeNodeState = iota + 1
	// TreeNodeLive the state of TreeNode that alread watched.
	TreeNodeLive
	// TreeNodeDead the state of TreeNode that removed.
	TreeNodeDead
)

var (
	treeEventTypeString = map[TreeEventType]string{
		Unknow:                "Unknow",
		NodeAdd:               "NodeAdded",
		NodeUpdate:            "NodeUpdated",
		NodeRemoved:           "NodeRemoved",
		ConnectionSuspended:   "ConnSuspended",
		ConnectionReconnected: "ConnReconnected",
		ConnectionLost:        "ConnLost",
		Initialized:           "Initialized",
	}
	treeNodeStateString = map[TreeNodeState]string{
		TreeNodeState(0): "Unknow",
		TreeNodePending:  "TreeNodePending",
		TreeNodeLive:     "TreeNodeLive",
		TreeNodeDead:     "TreeNodeDead",
	}
)

func (tet TreeEventType) String() string {
	return treeEventTypeString[tet]
}

func (tns TreeNodeState) String() string {
	return treeNodeStateString[tns]
}

// TreeNode node for cache zookeeper path.
type TreeNode struct {
	tree     *TreeCache
	path     string
	stat     *zk.Stat
	state    TreeNodeState
	data     []byte
	depth    int
	parent   *TreeNode
	children map[string]*TreeNode
	result   chan *nodeResult
	events   chan zk.Event
	mu       *sync.RWMutex
	cancel   context.CancelFunc
	ctx      context.Context
}

// NewTreeNode reutrn the a TreeNode.
func NewTreeNode(tree *TreeCache, parent *TreeNode, path string, depth int) *TreeNode {
	tn := &TreeNode{
		parent:   parent,
		tree:     tree,
		path:     path,
		depth:    depth,
		events:   make(chan zk.Event, 10),
		result:   make(chan *nodeResult, 10),
		mu:       &sync.RWMutex{},
		children: make(map[string]*TreeNode),
	}
	tn.ctx, tn.cancel = context.WithCancel(context.Background())
	go tn.processResult()
	go tn.processWatch()
	return tn
}

func (tn *TreeNode) wasCreated() {
	tn.refresh()
}

func (tn *TreeNode) wasDeleted() {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	for _, child := range tn.children {
		child.wasDeleted()
	}

	if tn.tree.state == CacheClosed {
		return
	}

	oldState := tn.state
	tn.state = TreeNodeDead
	if oldState == TreeNodeLive {
		tn.cancel()
		tn.tree.publishEvent(&TreeEvent{
			Type: NodeRemoved,
			Data: &ChildData{
				Path: tn.path,
				Data: tn.data,
			},
		})
	}
	if tn.parent == nil {
		go func() {
			result := &nodeResult{methodType: methodExists}
			result.exists, result.stat, result.err = tn.tree.client.Exists(tn.path)
			tn.result <- result
		}()
	} else {
		tn.parent.deleteChild(lastPart(tn.path))
	}
}

func (tn *TreeNode) refresh() {
	tn.refreshData()
	tn.refreshChildren()
}

func (tn *TreeNode) refreshData() {
	atomic.AddInt32(&tn.tree.outstandingOps, 1)
	go func() {
		var evt <-chan zk.Event
		result := &nodeResult{methodType: methodGet}
		result.data, result.stat, evt, result.err = tn.tree.client.GetW(tn.path)
		tn.result <- result
		select {
		case e := <-evt:
			tn.events <- e
		case <-tn.ctx.Done():
		}
	}()
}

func (tn *TreeNode) refreshChildren() {
	atomic.AddInt32(&tn.tree.outstandingOps, 1)
	go func() {
		var evt <-chan zk.Event
	outer:
		for {
			result := &nodeResult{methodType: methodGetChildren}
			result.children, result.stat, evt, result.err = tn.tree.client.ChildrenW(tn.path)
			tn.result <- result
			select {
			case <-tn.ctx.Done():
				return
			case e := <-evt:
				// ChildrenW can recived EventNodeDataChanged event, so ignore it.
				if e.Type == zk.EventNodeDataChanged {
					continue outer
				}
				tn.events <- e
				return
			}
		}
	}()
}

func (tn *TreeNode) processWatch() {
	var event zk.Event
	for {
		select {
		case event = <-tn.events:
		case <-tn.ctx.Done():
			return
		}
		switch event.Type {
		case zk.EventNodeCreated:
			if tn.parent == nil {
				// TODO unexpect create of no-root
			} else {
				tn.wasCreated()
			}
		case zk.EventNodeDataChanged:
			tn.refreshData()
		case zk.EventNodeChildrenChanged:
			tn.refreshChildren()
		case zk.EventNodeDeleted:
			tn.result <- &nodeResult{methodType: methodWasDeleted}
			tn.wasDeleted()
			return
		case zk.EventNotWatching:
			// TODO zk closed closed
		}
	}
}

func (tn *TreeNode) processResult() {
	var result *nodeResult
	for {
		select {
		case result = <-tn.result:
		case <-tn.ctx.Done():
			return
		}
		if result.err != nil {
			// TODO handle error or log it.
		} else {
			switch result.methodType {
			case methodExists:
			case methodGetChildren:
				for _, child := range result.children {
					if !tn.haveChild(child) {
						fullPath := path.Join(tn.path, child)
						node := NewTreeNode(tn.tree, tn, fullPath, tn.depth+1)
						tn.addChild(child, node)
						node.wasCreated()
					}
				}
			case methodGet:
				tn.mu.Lock()
				oldState := tn.state
				oldStat := tn.stat
				tn.data = result.data
				tn.stat = result.stat
				tn.state = TreeNodeLive
				tn.mu.Unlock()
				if oldState == TreeNodeLive {
					if oldStat == nil || oldStat.Mzxid != result.stat.Mzxid {
						tn.tree.publishEvent(&TreeEvent{
							Type: NodeUpdate,
							Data: tn.makeData(),
						})
					}
				} else {
					tn.tree.publishEvent(&TreeEvent{
						Type: NodeAdd,
						Data: tn.makeData(),
					})
				}
			case methodWasDeleted:
				return
			default:
				// TODO unhandled method
			}
		}
		atomic.AddInt32(&tn.tree.outstandingOps, -1)
		if atomic.LoadInt32(&tn.tree.outstandingOps) == 0 && !tn.tree.isInitialized() {
			tn.tree.setInitialized()
			tn.tree.publishEvent(&TreeEvent{
				Type: Initialized,
			})
		}
	}
}

func (tn *TreeNode) getChild(path string) *TreeNode {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	return tn.children[path]
}

func (tn *TreeNode) getChildren() (children []string) {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	for child := range tn.children {
		children = append(children, child)
	}
	return children
}

func (tn *TreeNode) deleteChild(child string) {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	delete(tn.children, child)
}

func (tn *TreeNode) addChild(child string, node *TreeNode) {
	tn.mu.Lock()
	defer tn.mu.Unlock()

	tn.children[child] = node
}

func (tn *TreeNode) haveChild(child string) bool {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	_, ok := tn.children[child]
	return ok
}

func (tn *TreeNode) makeData() *ChildData {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	return &ChildData{
		Stat: tn.stat,
		Data: tn.data,
		Path: tn.path,
	}
}

// Data return the data of node.
func (tn *TreeNode) Data() []byte {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	return tn.data
}

type methodType int

const (
	methodUnknow methodType = iota
	methodGet
	methodGetChildren
	methodExists
	methodWasDeleted
)

type nodeResult struct {
	methodType methodType
	children   []string
	data       []byte
	ec         <-chan zk.Event
	stat       *zk.Stat
	exists     bool
	err        error
}

// TreeEvent event for Listener.
type TreeEvent struct {
	Data *ChildData
	Type TreeEventType
}

// ChildData the tree event data.
type ChildData struct {
	Path string
	Stat *zk.Stat
	Data []byte
}

func lastPart(fullPath string) string {
	parts := strings.Split(fullPath, "/")
	return parts[len(parts)-1]
}
