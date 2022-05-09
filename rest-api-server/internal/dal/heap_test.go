package dal

import (
	"testing"

	"hopsworks.ai/rdrs/internal/config"
)

func TestHeap(t *testing.T) {
	InitializeBuffers()

	stats := GetNativeBuffersStats()
	totalBuffers := stats.BuffersCount

	if stats.AllocationsCount != uint64(config.PreAllocBuffers()) {
		t.Fatalf("Number of pre allocated buffers does not match. Expecting: %d, Got: %d ",
			config.PreAllocBuffers(), stats.AllocationsCount)
	}

	buff := GetBuffer()
	stats = GetNativeBuffersStats()
	if stats.FreeBuffers != totalBuffers-1 {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			stats.FreeBuffers, totalBuffers-1)
	}
	ReturnBuffer(buff)
	stats = GetNativeBuffersStats()
	if stats.FreeBuffers != totalBuffers {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			stats.FreeBuffers, totalBuffers)
	}

	allocations := stats.FreeBuffers + 100
	c := make(chan *NativeBuffer)
	for i := uint64(0); i < allocations; i++ {
		go allocateBuffTest(t, c)
	}

	myBuffers := make([]*NativeBuffer, allocations)
	for i := uint64(0); i < allocations; i++ {
		myBuffers = append(myBuffers, <-c)
	}

	stats = GetNativeBuffersStats()
	if stats.FreeBuffers != 0 {
		t.Fatalf("Number of free buffers is not zero. Expecting: 0, Got: %d", stats.FreeBuffers)
	}

	if stats.BuffersCount != allocations {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			config.PreAllocBuffers(), stats.AllocationsCount)
	}

	for i := uint64(0); i < allocations; i++ {
		ReturnBuffer(myBuffers[i])
	}

	stats = GetNativeBuffersStats()
	if stats.FreeBuffers != allocations {
		t.Fatalf("Number of free buffers does not match. Expecting: %d, Got: %d",
			allocations, stats.FreeBuffers)
	}
}

func allocateBuffTest(t *testing.T, c chan *NativeBuffer) {
	b := GetBuffer()
	c <- b
}
