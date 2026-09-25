package buildcache

import (
	"testing"
)

func TestParseDigest(t *testing.T) {
	good := "sha256:" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	d, err := ParseDigest(good)
	if err != nil {
		t.Fatal(err)
	}
	if d.String() != good || !d.Valid() {
		t.Fatalf("round trip failed: %q valid=%v", d.String(), d.Valid())
	}
	for _, bad := range []string{
		"",
		"noseparator",
		"sha256:short",
		"md5:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef", // 大写不接受
		":0123",
		"sha256:",
	} {
		if _, err := ParseDigest(bad); err == nil {
			t.Fatalf("ParseDigest(%q) should fail", bad)
		}
	}
}
