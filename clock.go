package buildcache

import (
	"encoding/hex"
	"time"
)

// Digest 是内容块/拼接结果的 SHA-256 摘要，使用小写十六进制表示。
type Digest string

// EmptyDigest 是零字节内容的 SHA-256 摘要。
const EmptyDigest Digest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// Valid 报告摘要是否为合法的 64 位十六进制 SHA-256。
func (d Digest) Valid() bool {
	if len(d) != 64 {
		return false
	}
	_, err := hex.DecodeString(string(d))
	return err == nil
}

// Clock 是全系统统一的当前时间来源。所有租约/过期判断都必须经过它，
// 生产环境使用 RealClock，测试使用可控的假时钟。
type Clock interface {
	Now() time.Time
}

// RealClock 直接返回墙钟时间。
type RealClock struct{}

// Now 实现 Clock。
func (RealClock) Now() time.Time { return time.Now() }

// FakeClock 是测试用的可控时钟。
type FakeClock struct {
	t time.Time
}

// NewFakeClock 从给定时间创建假时钟。
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{t: t} }

// Now 实现 Clock。
func (c *FakeClock) Now() time.Time { return c.t }

// Advance 将时钟向前推进 d。
func (c *FakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
