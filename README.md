# gorecipe ![GoCI](http://goci.ele.me/na/goci/eleme/mux/badge?type=job)
-------
![GoLint](http://goci.ele.me/na/goci/eleme/mux/badge?type=lint)
![GoFmt](http://goci.ele.me/na/goci/eleme/mux/badge?type=fmt)
![GoVet](http://goci.ele.me/na/goci/eleme/mux/badge?type=vet)
![GoTest](http://goci.ele.me/na/goci/eleme/mux/badge?type=test)


zookeeper tree cache

### usage

```go
    conn, evt, err := zk.Connect([]string{"localhost"}, time.Second)
    if err != nil {
            log.Fatalln("Connect Error:", err)
    }
    defer conn.Close()

    tc := recipe.NewTreeCache(conn, evt, "/")
    tc.Listen(recipe.ListenFunc(func(e *recipe.TreeEvent) {
            fmt.Println("Recive:", e.Type)
    }))
    tc.Start()
```
