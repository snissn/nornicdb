package storage

import "testing"

func BenchmarkFindCapability_StorageByteStatsProvider(b *testing.B) {
	be, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer be.Close()
	wrapped := NewNamespacedEngine(NewTracedEngine(be), "bench")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := FindCapability[StorageByteStatsProvider](wrapped); !ok {
			b.Fatal("missing byte stats provider")
		}
	}
}

func BenchmarkInspectEngineCapabilities(b *testing.B) {
	be, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer be.Close()
	wrapped := NewNamespacedEngine(NewTracedEngine(be), "bench")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		caps := InspectEngineCapabilities(wrapped)
		if caps.Backend != "badger" {
			b.Fatal("unexpected backend")
		}
	}
}
