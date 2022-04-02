package sysmet

import "testing"

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
