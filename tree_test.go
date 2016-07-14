package recipe

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type cases struct {
	Path  string
	Data  string
	Type  TreeEventType
	Sleep time.Duration
}

func errorEvent(path string) *TreeEvent {
	return &TreeEvent{Type: -1, Data: ChildData{Path: path}}
}

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

	prefix := "/test"

	tc := NewTreeCache(conn, evt, prefix)
	che := make(chan *TreeEvent, 100)
	tc.Listen(ListenFunc(func(event *TreeEvent) {
		che <- event
	}))
	err := tc.Start()
	if err != nil {
		t.Error(err)
		return
	}

	for event := range che {
		if event.Type == Initialized {
			break
		}
	}

	testCases := []cases{
		{
			Path: prefix + "/hehe",
			Data: "00_data",
			Type: NodeAdd,
		},
		{
			Path: prefix + "/maybe",
			Data: "01_data",
			Type: NodeAdd,
		},
		{
			Path:  prefix + "/maybe",
			Data:  "01_data_1",
			Type:  NodeUpdate,
			Sleep: time.Millisecond * 50,
		},
		{
			Path:  prefix + "/maybe",
			Data:  "01_data_2",
			Type:  NodeUpdate,
			Sleep: time.Millisecond * 50,
		},
		{
			Path: prefix + "/hehe/haha",
			Data: "02_data",
			Type: NodeAdd,
		},
		{
			Path: prefix + "/hehe/hehe",
			Data: "03_data",
			Type: NodeAdd,
		},
		{
			Path:  prefix + "/hehe/hehe",
			Data:  "03_data_01",
			Type:  NodeUpdate,
			Sleep: time.Millisecond * 50,
		},
		{
			Path:  prefix + "/hehe/hehe",
			Data:  "",
			Type:  NodeRemoved,
			Sleep: time.Millisecond * 50,
		},
	}

	casesMap := map[string]cases{}
	for _, v := range testCases {
		casesMap[fmt.Sprintf("%v%v%v", v.Path, v.Type, v.Data)] = v
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
					che <- errorEvent(v.Path)
				}
			} else if v.Type == NodeUpdate {
				_, stat, err := conn.Get(v.Path)
				if err != nil {
					t.Error(err)
					che <- errorEvent(v.Path)
					continue
				}
				_, err = conn.Set(v.Path, []byte(v.Data), stat.Version)
				if err != nil {
					t.Error(err)
					che <- errorEvent(v.Path)
				}
			} else if v.Type == NodeRemoved {
				_, stat, err := conn.Get(v.Path)
				if err != nil {
					t.Error(err)
					che <- errorEvent(v.Path)
					continue
				}
				err = conn.Delete(v.Path, stat.Version)
				if err != nil {
					t.Error(err)
					che <- errorEvent(v.Path)
				}
			}
		}
		close(che)
		log.Println("Finish Send.")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()

		var event *TreeEvent
		for event = range che {
			log.Println("Recived:", event)
			key := fmt.Sprintf("%v%v%v", event.Data.Path, event.Type, string(event.Data.Data))
			v, _ := casesMap[key]
			delete(casesMap, key)
			if event.Type == -1 {
				continue
			}
			if event.Data.Path != v.Path {
				t.Errorf("path whant=%s, got=%s", v.Path, event.Data.Path)
			}
			if string(event.Data.Data) != v.Data {
				t.Errorf("data whant=%s, got=%s", v.Data, string(event.Data.Data))
			}
		}
		if len(casesMap) > 0 {
			t.Error("CasesMaps not empty:", casesMap)
		}
		log.Println("Finish Recive.")
	}()
	wg.Wait()

	log.Println("Start Get.")
	last := map[string]string{}
	for _, v := range testCases {
		if v.Type == NodeAdd || v.Type == NodeUpdate {
			last[v.Path] = v.Data
		} else if v.Type == NodeRemoved {
			delete(last, v.Path)
		}
	}
	for path, data := range last {
		getData := tc.GetData(path)
		if getData == nil {
			t.Error("Get Data Error:", path)
			continue
		}
		if data != string(getData) {
			t.Errorf("get data error: want=%s got=%s", data, string(getData))
		}
	}
	log.Println("Finish Get.")
}
