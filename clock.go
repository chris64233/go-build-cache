package buildcache

import (
	"sync"
	"sync/atomic"
	"time"
)

// Clock 是全系统统一的"当前时间"来源。
// 所有租约判定、过期清理、GC 审计时间戳都必须取自 Clock，禁止直接调用 time.Now()。
type Clock interface {
	Now() time.Time
}

// SystemClock 返回真实墙钟时间。
type SystemClock struct{}

// Now 返回当前本地时间。
func (SystemClock) Now() time.Time { return time.Now() }

// FakeClock 是测试用可控时钟，支持并发安全地推进时间。
type FakeClock struct {
	mu  sync.RWMutex
	now atomic.Int64 // unix nanos
}

// NewFakeClock 以给定时刻创建可控时钟。
func NewFakeClock(t time.Time) *FakeClock {
	c := &FakeClock{}
	c.now.Store(t.UnixNano())
	return c
}

// Now 返回当前的模拟时间。
func (c *FakeClock) Now() time.Time {
	return time.Unix(0, c.now.Load())
}

// Advance 将时钟向前推进 d。
func (c *FakeClock) Advance(d time.Duration) {
	c.now.Add(int64(d))
}

// Set 直接设置当前时间（不得往回拨过租约语义所依赖的时刻，调用方自行保证）。
func (c *FakeClock) Set(t time.Time) { c.now.Store(t.UnixNano()) }
