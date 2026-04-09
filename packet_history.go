package main

import (
	"sync"
	"time"

	"github.com/pion/rtp"
)

type PacketEntry struct {
	Seq   uint16
	Ts    uint32
	Bytes []byte
	Time  time.Time
}

type PacketHistory struct {
	mu       sync.RWMutex
	entries  map[uint16]*PacketEntry
	maxMs    int
	lastSeq  uint16
	lastTime time.Time
}

func NewPacketHistory(maxMs int) *PacketHistory {
	if maxMs <= 0 {
		maxMs = 1500
	}
	return &PacketHistory{
		entries: make(map[uint16]*PacketEntry),
		maxMs:   maxMs,
	}
}

func (ph *PacketHistory) UpdateMaxMS(maxMs int) {
	if maxMs <= 0 {
		maxMs = 1500
	}
	ph.mu.Lock()
	defer ph.mu.Unlock()
	ph.maxMs = maxMs
	expire := time.Now().Add(-time.Duration(ph.maxMs) * time.Millisecond)
	for seq, e := range ph.entries {
		if e.Time.Before(expire) {
			delete(ph.entries, seq)
		}
	}
}

func (ph *PacketHistory) Add(pkt *rtp.Packet, raw []byte) {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	now := time.Now()
	ph.entries[pkt.SequenceNumber] = &PacketEntry{
		Seq:   pkt.SequenceNumber,
		Ts:    pkt.Timestamp,
		Bytes: append([]byte(nil), raw...),
		Time:  now,
	}
	ph.lastSeq = pkt.SequenceNumber
	ph.lastTime = now
	expire := now.Add(-time.Duration(ph.maxMs) * time.Millisecond)
	for seq, e := range ph.entries {
		if e.Time.Before(expire) {
			delete(ph.entries, seq)
		}
	}
}

func (ph *PacketHistory) Get(seq uint16) ([]byte, bool) {
	ph.mu.RLock()
	defer ph.mu.RUnlock()
	e, ok := ph.entries[seq]
	if !ok {
		return nil, false
	}
	return e.Bytes, true
}
