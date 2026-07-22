package statsiglocal

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strconv"
	"testing"
)

func TestGenerateProducesSelfConsistentStatsig(t *testing.T) {
	const (
		path = "/rest/app-chat/conversations/new"
		now  = int64(1784708660)
		mask = byte(0x4d)
	)
	value, err := generateWithMask(path, "post", now, mask)
	if err != nil {
		t.Fatal(err)
	}
	const expected = "Tfouzk0bde54kJ3g1b2KBZQ6vAF1xOzkVHOnV2S6XdKIIyeLAT3kL4ddeOnS5ZvOUolXXEt/Cp/OLInruuUrRFdyJSxKTg"
	if value != expected {
		t.Fatalf("Statsig=%q want %q", value, expected)
	}
	raw, err := base64.RawStdEncoding.DecodeString(value)
	if err != nil || len(raw) != 70 {
		t.Fatalf("decoded Statsig length=%d err=%v", len(raw), err)
	}
	if raw[0] != mask {
		t.Fatalf("mask=%d", raw[0])
	}
	for index, expected := range protocolSeed {
		if actual := raw[index+1] ^ mask; actual != expected {
			t.Fatalf("seed[%d]=%d want %d", index, actual, expected)
		}
	}

	number := binary.LittleEndian.Uint32([]byte{
		raw[49] ^ mask, raw[50] ^ mask, raw[51] ^ mask, raw[52] ^ mask,
	})
	if want := uint32(now - statsigEpoch); number != want {
		t.Fatalf("number=%d want %d", number, want)
	}
	input := "POST!" + path + "!" + strconv.FormatUint(uint64(number), 10) + statsigSalt + fingerprint
	digest := sha256.Sum256([]byte(input))
	for index, expected := range digest[:16] {
		if actual := raw[index+53] ^ mask; actual != expected {
			t.Fatalf("digest[%d]=%d want %d", index, actual, expected)
		}
	}
	if actual := raw[69] ^ mask; actual != statsigMark {
		t.Fatalf("marker=%d want %d", actual, statsigMark)
	}
}

func TestGenerateValidatesMethodPathAndTimestamp(t *testing.T) {
	for _, test := range []struct {
		name   string
		path   string
		method string
		now    int64
	}{
		{name: "relative path", path: "rest/test", method: "POST", now: 1784708660},
		{name: "empty method", path: "/rest/test", now: 1784708660},
		{name: "old timestamp", path: "/rest/test", method: "POST", now: statsigEpoch - 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := generateWithMask(test.path, test.method, test.now, 1); err == nil {
				t.Fatal("invalid input was accepted")
			}
		})
	}
}
