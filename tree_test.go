package recipe

import (
	"sync"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func getConnFromTestCluster(t *testing.T) (*zk.TestCluster, <-chan zk.Event, *zk.Conn) {
	ts, err := zk.StartTestCluster(1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, evt, err := ts.ConnectAll()
	if err != nil {
		t.Fatal(err)
	}
	return ts, evt, conn
}

func TestTreeCache(t *testing.T) {
	testServer, evt, conn := getConnFromTestCluster(t)
	defer testServer.Stop()
	defer conn.Close()
	for {
		e := <-evt
		if e.State == zk.StateConnected {
			break
		}
	}

	tc := NewTreeCache(conn, evt, "/")
	che := make(chan *TreeEvent, 100)
	tc.Listen(ListenFunc(func(event *TreeEvent) {
		che <- event
	}))
	tc.Start()
	for event := range che {
		if event.Type == Initialized {
			break
		}
	}

	testCases := []struct {
		Path  string
		Data  string
		Type  TreeEventType
		Sleep time.Duration
	}{
		{
			Path: "/hehe",
			Data: "00_data",
			Type: NodeAdd,
		},
		{
			Path: "/maybe",
			Data: "01_data",
			Type: NodeAdd,
		},
		{
			Path:  "/maybe",
			Data:  "01_data_1",
			Type:  NodeUpdate,
			Sleep: time.Millisecond * 50,
		},
		{
			Path:  "/maybe",
			Data:  "01_data_2",
			Type:  NodeUpdate,
			Sleep: time.Millisecond * 50,
		},
		{
			Path: "/hehe/haha",
			Data: "02_data",
			Type: NodeAdd,
		},
		{
			Path: "/hehe/hehe",
			Data: "03_data",
			Type: NodeAdd,
		},
		{
			Path:  "/hehe/hehe",
			Data:  "03_data_01",
			Type:  NodeUpdate,
			Sleep: time.Millisecond * 50,
		},
		{
			Path:  "/hehe/hehe",
			Data:  "",
			Type:  NodeRemoved,
			Sleep: time.Millisecond * 50,
		},
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, v := range testCases {
			if v.Sleep != 0 {
				time.Sleep(v.Sleep)
			}
			if v.Type == NodeAdd {
				_, err := conn.Create(v.Path, []byte(v.Data), 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					t.Error(err)
					che <- nil
				}
			} else if v.Type == NodeUpdate {
				_, stat, err := conn.Get(v.Path)
				if err != nil {
					t.Error(err)
					che <- nil
					continue
				}
				_, err = conn.Set(v.Path, []byte(v.Data), stat.Version)
				if err != nil {
					t.Error(err)
					che <- nil
				}
			} else if v.Type == NodeRemoved {
				_, stat, err := conn.Get(v.Path)
				if err != nil {
					t.Error(err)
					che <- nil
					continue
				}
				err = conn.Delete(v.Path, stat.Version)
				if err != nil {
					t.Error(err)
					che <- nil
				}
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, v := range testCases {
			var event *TreeEvent
			for event = range che {
				if event == nil || (event.Type != Initialized && event.Data.Path == v.Path) {
					break
				}
			}
			if event == nil {
				continue
			}
			if event.Data.Path != v.Path {
				t.Errorf("path whant=%s, got=%s", v.Path, event.Data.Path)
			}
			if string(event.Data.Data) != v.Data {
				t.Errorf("path whant=%s, got=%s", v.Data, string(event.Data.Data))
			}
		}
	}()
	wg.Wait()

	last := map[string]string{}
	for _, v := range testCases {
		if v.Type == NodeAdd || v.Type == NodeUpdate {
			last[v.Path] = v.Data
		} else if v.Type == NodeRemoved {
			delete(last, v.Path)
		}
	}
	for path, data := range last {
		getData, _, err := conn.Get(path)
		if err != nil {
			t.Error(err)
			continue
		}
		if data != string(getData) {
			t.Errorf("get data error: want=%s got=%s", data, string(getData))
		}
	}
}
