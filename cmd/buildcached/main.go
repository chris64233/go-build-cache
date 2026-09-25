// Command buildcached 启动内容寻址构建缓存服务。
//
// 用法：
//
//	buildcached -addr :8080 -data ./data -ttl 15m
//
// 元数据与内容块持久化到 -data 目录，重启后自动恢复；
// 可通过 -gc-interval 指定后台垃圾回收周期（0 表示只允许手动 POST /v1/gc）。
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	buildcache "github.com/chris64233/go-build-cache"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	dataDir := flag.String("data", "./data", "持久化数据目录")
	ttl := flag.Duration("ttl", buildcache.DefaultSessionTTL, "上传会话默认租约时长")
	gcInterval := flag.Duration("gc-interval", 10*time.Minute, "后台 GC 周期；0 表示关闭自动 GC")
	flag.Parse()

	store, err := buildcache.NewFileStore(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	cache, err := buildcache.New(store, buildcache.SystemClock{}, *ttl)
	if err != nil {
		log.Fatalf("init cache: %v", err)
	}
	handler := buildcache.NewHandler(cache)

	if *gcInterval > 0 {
		go func() {
			ticker := time.NewTicker(*gcInterval)
			defer ticker.Stop()
			for range ticker.C {
				report, err := cache.CollectGarbage()
				if err != nil {
					log.Printf("gc error: %v", err)
					continue
				}
				log.Printf("gc generation=%d live=%d deleted=%d swept=%d",
					report.Generation, report.LiveBlobCount,
					len(report.DeletedBlobs), len(report.SweptSessions))
			}
		}()
	}

	log.Printf("build cache listening on %s, data=%s ttl=%s", *addr, *dataDir, *ttl)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           handler.Mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
