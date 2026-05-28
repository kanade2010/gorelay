package main

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func makeTestRTP(seq uint16, ssrc uint32, payloadLen int) []byte {
	if payloadLen < 0 {
		payloadLen = 0
	}
	b := make([]byte, 12+payloadLen)
	b[0] = 0x80
	b[1] = 96
	binary.BigEndian.PutUint16(b[2:4], seq)
	binary.BigEndian.PutUint32(b[8:12], ssrc)
	return b
}

func TestWeakNetFixedDropEveryN(t *testing.T) {
	txOut := 0
	cfg := WeakNetEmulationConfig{
		Enabled:         true,
		Seed:            1,
		FixedDropEveryN: 2,
	}
	normalizeWeakNetEmulationConfig(&cfg)

	w := NewWeakNetEmulation(cfg, WeakNetEmulationConfig{}, func(pkt []byte, _ *net.UDPAddr) {
		txOut++
	}, func(_ []byte, _ *net.UDPAddr) {})
	defer w.Close()

	key := WeakNetFlowKey{
		Direction: "tx",
		PeerIP:    "127.0.0.1",
		PeerPort:  9000,
		StreamID:  "ssrc:111",
	}
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9000}
	now := time.Now()
	for i := 1; i <= 4; i++ {
		p := makeTestRTP(uint16(i), 111, 10)
		if err := w.EnqueueTx(key, &WeakNetPacket{Bytes: p, Addr: addr, Seq: uint16(i), SSRC: 111}, now); err != nil {
			t.Fatalf("enqueue tx failed: %v", err)
		}
	}

	st, ok, err := w.GetStats(key)
	if err != nil {
		t.Fatalf("get stats failed: %v", err)
	}
	if !ok {
		t.Fatalf("stats not found")
	}
	if st.OutPackets != 2 || st.DroppedPackets != 2 {
		t.Fatalf("unexpected stats out=%d drop=%d", st.OutPackets, st.DroppedPackets)
	}
	if txOut != 2 {
		t.Fatalf("unexpected output count=%d", txOut)
	}
}

func TestWeakNetQueueGuardAndFlush(t *testing.T) {
	txOut := 0
	cfg := WeakNetEmulationConfig{
		Enabled:         true,
		Seed:            7,
		FixedDelayMs:    1000,
		MaxQueuePackets: 1,
	}
	normalizeWeakNetEmulationConfig(&cfg)

	w := NewWeakNetEmulation(cfg, WeakNetEmulationConfig{}, func(pkt []byte, _ *net.UDPAddr) {
		txOut++
	}, func(_ []byte, _ *net.UDPAddr) {})
	defer w.Close()

	key := WeakNetFlowKey{
		Direction: "tx",
		PeerIP:    "127.0.0.1",
		PeerPort:  9001,
		StreamID:  "ssrc:222",
	}
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9001}
	now := time.Now()

	if err := w.EnqueueTx(key, &WeakNetPacket{Bytes: makeTestRTP(1, 222, 10), Addr: addr, Seq: 1, SSRC: 222}, now); err != nil {
		t.Fatalf("enqueue 1 failed: %v", err)
	}
	if err := w.EnqueueTx(key, &WeakNetPacket{Bytes: makeTestRTP(2, 222, 10), Addr: addr, Seq: 2, SSRC: 222}, now); err != nil {
		t.Fatalf("enqueue 2 failed: %v", err)
	}

	st, ok, err := w.GetStats(key)
	if err != nil || !ok {
		t.Fatalf("stats unavailable err=%v ok=%v", err, ok)
	}
	if st.QueuePackets != 1 || st.DroppedPackets != 1 {
		t.Fatalf("unexpected pre-flush stats queue=%d drop=%d", st.QueuePackets, st.DroppedPackets)
	}

	w.FlushDue(now.Add(2 * time.Second))
	st, ok, err = w.GetStats(key)
	if err != nil || !ok {
		t.Fatalf("stats unavailable after flush err=%v ok=%v", err, ok)
	}
	if st.QueuePackets != 0 || st.OutPackets != 1 {
		t.Fatalf("unexpected post-flush stats queue=%d out=%d", st.QueuePackets, st.OutPackets)
	}
	if txOut != 1 {
		t.Fatalf("unexpected output count=%d", txOut)
	}
}

func TestWeakNetSeedDeterministic(t *testing.T) {
	cfg := WeakNetEmulationConfig{
		Enabled:        true,
		Seed:           42,
		RandomLossRate: 0.4,
	}
	normalizeWeakNetEmulationConfig(&cfg)

	build := func() *WeakNetEmulation {
		return NewWeakNetEmulation(cfg, WeakNetEmulationConfig{}, func(_ []byte, _ *net.UDPAddr) {}, func(_ []byte, _ *net.UDPAddr) {})
	}

	w1 := build()
	defer w1.Close()
	w2 := build()
	defer w2.Close()

	key := WeakNetFlowKey{
		Direction: "tx",
		PeerIP:    "127.0.0.1",
		PeerPort:  9002,
		StreamID:  "ssrc:333",
	}
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9002}
	now := time.Now()

	for i := 0; i < 200; i++ {
		seq := uint16(i + 1)
		p := &WeakNetPacket{Bytes: makeTestRTP(seq, 333, 10), Addr: addr, Seq: seq, SSRC: 333}
		if err := w1.EnqueueTx(key, p, now); err != nil {
			t.Fatalf("w1 enqueue failed: %v", err)
		}
		if err := w2.EnqueueTx(key, p, now); err != nil {
			t.Fatalf("w2 enqueue failed: %v", err)
		}
	}

	s1, ok1, err1 := w1.GetStats(key)
	s2, ok2, err2 := w2.GetStats(key)
	if err1 != nil || err2 != nil || !ok1 || !ok2 {
		t.Fatalf("stats unavailable err1=%v err2=%v ok1=%v ok2=%v", err1, err2, ok1, ok2)
	}
	if s1.OutPackets != s2.OutPackets || s1.DroppedPackets != s2.DroppedPackets {
		t.Fatalf("determinism mismatch out %d/%d drop %d/%d", s1.OutPackets, s2.OutPackets, s1.DroppedPackets, s2.DroppedPackets)
	}
}

func TestWeakNetTxRxIsolation(t *testing.T) {
	txOut := 0
	rxOut := 0

	txCfg := WeakNetEmulationConfig{Enabled: true, FixedDelayMs: 0}
	rxCfg := WeakNetEmulationConfig{Enabled: false}
	normalizeWeakNetEmulationConfig(&txCfg)
	normalizeWeakNetEmulationConfig(&rxCfg)

	w := NewWeakNetEmulation(txCfg, rxCfg, func(_ []byte, _ *net.UDPAddr) {
		txOut++
	}, func(_ []byte, _ *net.UDPAddr) {
		rxOut++
	})
	defer w.Close()

	txKey := WeakNetFlowKey{Direction: "tx", PeerIP: "127.0.0.1", PeerPort: 9003, StreamID: "ssrc:444"}
	rxKey := WeakNetFlowKey{Direction: "rx", PeerIP: "127.0.0.1", PeerPort: 9004, StreamID: "ssrc:555"}
	txAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9003}
	rxAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9004}
	now := time.Now()

	if err := w.EnqueueTx(txKey, &WeakNetPacket{Bytes: makeTestRTP(1, 444, 10), Addr: txAddr, Seq: 1, SSRC: 444}, now); err != nil {
		t.Fatalf("enqueue tx failed: %v", err)
	}
	if err := w.EnqueueRx(rxKey, &WeakNetPacket{Bytes: makeTestRTP(1, 555, 10), Addr: rxAddr, Seq: 1, SSRC: 555}, now); err != nil {
		t.Fatalf("enqueue rx failed: %v", err)
	}

	txStats, txOk, txErr := w.GetStats(txKey)
	if txErr != nil || !txOk {
		t.Fatalf("tx stats unavailable err=%v ok=%v", txErr, txOk)
	}
	if txStats.OutPackets != 1 {
		t.Fatalf("tx out expected 1 got %d", txStats.OutPackets)
	}
	if txOut != 1 || rxOut != 1 {
		t.Fatalf("unexpected outputs tx=%d rx=%d", txOut, rxOut)
	}
}
