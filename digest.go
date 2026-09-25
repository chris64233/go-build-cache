package buildcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
)

// Digest 是内容摘要，形如 "sha256:<64个十六进制字符>"。
// 默认算法为 SHA-256，算法名随摘要一起持久化，便于将来扩展。
type Digest struct {
	Algo string
	Hex  string
}

// ParseDigest 解析 "algo:hex" 形式的摘要。
func ParseDigest(s string) (Digest, error) {
	algo, hexPart, ok := strings.Cut(s, ":")
	if !ok || algo == "" || hexPart == "" {
		return Digest{}, fmt.Errorf("buildcache: malformed digest %q", s)
	}
	if err := validateHex(algo, hexPart); err != nil {
		return Digest{}, err
	}
	return Digest{Algo: algo, Hex: hexPart}, nil
}

func validateHex(algo, hx string) error {
	switch algo {
	case "sha256":
		if len(hx) != 64 {
			return fmt.Errorf("buildcache: sha256 digest must be 64 hex chars, got %d", len(hx))
		}
	default:
		return fmt.Errorf("buildcache: unsupported digest algorithm %q", algo)
	}
	for _, c := range hx {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return fmt.Errorf("buildcache: digest %q contains non-lowercase-hex character %q", algo+":"+hx, c)
		}
	}
	return nil
}

func (d Digest) String() string {
	if d.Algo == "" {
		return ""
	}
	return d.Algo + ":" + d.Hex
}

// Valid 报告摘要是否非空且格式合法。
func (d Digest) Valid() bool {
	return d.Algo != "" && validateHex(d.Algo, d.Hex) == nil
}

func (d Digest) equal(o Digest) bool { return d == o }

// newHasher 按算法返回新的哈希实例。
func newHasher(algo string) (hash.Hash, error) {
	switch algo {
	case "sha256":
		return sha256.New(), nil
	default:
		return nil, fmt.Errorf("buildcache: unsupported digest algorithm %q", algo)
	}
}

// hashBytes 计算数据的摘要。
func hashBytes(algo string, data []byte) (Digest, error) {
	switch algo {
	case "sha256":
		sum := sha256.Sum256(data)
		return Digest{Algo: algo, Hex: hex.EncodeToString(sum[:])}, nil
	default:
		return Digest{}, fmt.Errorf("buildcache: unsupported digest algorithm %q", algo)
	}
}
