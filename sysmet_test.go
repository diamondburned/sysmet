package sysmet

import (
	"bytes"
	"testing"
	"time"
)

func BenchmarkEncode(b *testing.B) {
	s, err := PrepareMetrics()
	if err != nil {
		s = Snapshot{}
	}

	v, err := encodeSnapshot(s)
	if err != nil {
		b.Fatal("cannot encode")
	}

	b.SetBytes(int64(len(v)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := encodeSnapshot(s)
		if err != nil {
			b.Fatal("cannot encode")
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	s, err := PrepareMetrics()
	if err != nil {
		s = Snapshot{}
	}

	v, err := encodeSnapshot(s)
	if err != nil {
		b.Fatal("cannot encode")
	}

	b.SetBytes(int64(len(v)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := decodeSnapshot(v, &s); err != nil {
			b.Fatal("cannot decode")
		}
	}
}

func TestBkey(t *testing.T) {
	epoch := time.Now().Unix()
	epochb := unixToBE(uint32(epoch))

	k := bkey(bPoints, epochb)
	if !bytes.Equal(k, []byte("s3\x00p\x00"+string(epochb))) {
		t.Fatalf("bad key: %q", k)
	}

	k = bkeyTrim(k, bPoints)
	if !bytes.Equal(k, epochb) {
		t.Fatalf("bad value after trimming: %q", k)
	}
}
