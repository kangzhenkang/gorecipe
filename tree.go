package recipe

import (
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/samuel/go-zookeeper/zk"
)

type TreeEventType int
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

	TreeNodePending TreeNodeState = iota + 1
	TreeNodeLive
	TreeNodeDead
)

var (
	treeEventTypeString = map[TreeEventType]string{
		Unknow:                "Unknow",
		NodeAdd:               "NodeAdd",
		NodeUpdate:            "NodeUpdate",
		NodeRemoved:           "NodeRemoved",
		ConnectionSuspended:   "ConnectionSuspended",
		ConnectionReconnected: "ConnectionReconnected",
		ConnectionLost:        "ConnectionLost",
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
}

func NewTreeNode(tree *TreeCache, parent *TreeNode, path string, depth int) *TreeNode {
	tn := &TreeNode{
		tree:     tree,
		path:     path,
		depth:    depth,
		events:   make(chan zk.Event, 10),
		result:   make(chan *nodeResult, 10),
		mu:       &sync.RWMutex{},
		children: make(map[string]*TreeNode),
	}
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
		tn.tree.publishEvent(&TreeEvent{
			Type: NodeRemoved,
			Data: ChildData{
				Path: tn.path,
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
		e := <-evt
		tn.events <- e
	}()
}

func (tn *TreeNode) refreshChildren() {
	atomic.AddInt32(&tn.tree.outstandingOps, 1)
	go func() {
		var evt <-chan zk.Event
		for {
			result := &nodeResult{methodType: methodGetChildren}
			result.children, result.stat, evt, result.err = tn.tree.client.ChildrenW(tn.path)
			tn.result <- result
			e := <-evt
			// ChildrenW can recived EventNodeDataChanged event, so ignore it.
			if e.Type == zk.EventNodeDataChanged {
				continue
			}
			tn.events <- e
			break
		}
	}()
}

func (tn *TreeNode) processWatch() {
	for {
		event := <-tn.events
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
			tn.wasDeleted()
		case zk.EventNotWatching:
			// TODO zk closed closed
		}
	}
}

func (tn *TreeNode) processResult() {
	for {
		result := <-tn.result
		if result.err != nil {
			// TODO handle error or log it.
			continue
		}
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
		default:
			// TODO unhandled method
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
	for child, _ := range tn.children {
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

func (tn *TreeNode) makeData() ChildData {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	return ChildData{
		Stat: tn.stat,
		Data: tn.data,
		Path: tn.path,
	}
}

type methodType int

const (
	methodUnknow methodType = iota
	methodGet
	methodGetChildren
	methodExists
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

type TreeEvent struct {
	Data ChildData
	Type TreeEventType
}

type ChildData struct {
	Path string
	Stat *zk.Stat
	Data []byte
}

func lastPart(fullPath string) string {
	parts := strings.Split(fullPath, "/")
	return parts[len(parts)-1]
}
