package main

import (
	"reflect"
	"testing"
	"time"
)

func TestLossMonitorSeriesFill(t *testing.T) {
	m := NewLossMonitor(300)
	base := time.Unix(1000, 0)
	key := WeakNetFlowKey{
		PeerIP:    "1.1.1.1",
		PeerPort:  10000,
		StreamID:  "ssrc:111",
		Direction: "tx",
	}
	pkt := &LossMonitorPacket{Key: key, Seq: 10, SSRC: 111}
	m.OnTxInput(pkt, base)
	m.OnTxEmuDrop(pkt, base)

	points := m.getSeriesAt(base.Add(2*time.Second), 3, nil)
	if len(points) != 3 {
		t.Fatalf("unexpected point len: got=%d want=3", len(points))
	}

	if points[0].TsSec != base.Unix() {
		t.Fatalf("unexpected first ts: got=%d want=%d", points[0].TsSec, base.Unix())
	}
	if points[0].TxInputPackets != 1 || points[0].TxEmuDroppedPackets != 1 {
		t.Fatalf("unexpected first bucket counters: %+v", points[0])
	}
	if points[1].TxInputPackets != 0 || points[1].TxEmuDroppedPackets != 0 {
		t.Fatalf("middle bucket should be zero-filled: %+v", points[1])
	}
}

func TestLossMonitorRxSeqGap(t *testing.T) {
	m := NewLossMonitor(300)
	now := time.Unix(2000, 0)
	key := WeakNetFlowKey{
		PeerIP:    "2.2.2.2",
		PeerPort:  20000,
		StreamID:  "ssrc:222",
		Direction: "rx",
	}

	p1 := &LossMonitorPacket{Key: key, Seq: 100, SSRC: 222}
	p2 := &LossMonitorPacket{Key: key, Seq: 103, SSRC: 222}

	m.OnRxInput(p1, now)
	m.OnRxInput(p2, now)
	m.OnRxPacketSeq(p1, now)
	m.OnRxPacketSeq(p2, now)

	points := m.getSeriesAt(now, 1, nil)
	if len(points) != 1 {
		t.Fatalf("unexpected point len: got=%d want=1", len(points))
	}
	if points[0].RxRealLostPackets != 2 {
		t.Fatalf("unexpected rx real lost: got=%d want=2", points[0].RxRealLostPackets)
	}
}

func TestLossMonitorTxRtcpDelta(t *testing.T) {
	m := NewLossMonitor(300)
	t0 := time.Unix(3000, 0)
	t1 := t0.Add(1 * time.Second)
	key := WeakNetFlowKey{
		PeerIP:    "3.3.3.3",
		PeerPort:  30000,
		StreamID:  "ssrc:333",
		Direction: "tx",
	}

	for i := 0; i < 100; i++ {
		m.OnTxInput(&LossMonitorPacket{Key: key, Seq: uint16(i), SSRC: 333}, t1)
	}

	m.OnTxRtcpReport(&LossMonitorRTCPReport{
		Key:            key,
		FractionLost:   0,
		CumulativeLost: 10,
	}, t0)
	m.OnTxRtcpReport(&LossMonitorRTCPReport{
		Key:            key,
		FractionLost:   0,
		CumulativeLost: 15,
	}, t1)

	points := m.getSeriesAt(t1, 1, &key)
	if len(points) != 1 {
		t.Fatalf("unexpected point len: got=%d want=1", len(points))
	}
	if points[0].TxRealLostPackets != 5 {
		t.Fatalf("unexpected tx real lost: got=%d want=5", points[0].TxRealLostPackets)
	}
}

func TestLossMonitorSeriesByPeerIP(t *testing.T) {
	m := NewLossMonitor(300)
	now := time.Unix(4000, 0)

	tx1 := WeakNetFlowKey{PeerIP: "10.0.0.1", PeerPort: 10000, StreamID: "ssrc:1", Direction: "tx"}
	tx2 := WeakNetFlowKey{PeerIP: "10.0.0.1", PeerPort: 10002, StreamID: "ssrc:2", Direction: "tx"}
	rx1 := WeakNetFlowKey{PeerIP: "10.0.0.1", PeerPort: 10004, StreamID: "ssrc:3", Direction: "rx"}
	other := WeakNetFlowKey{PeerIP: "10.0.0.2", PeerPort: 20000, StreamID: "ssrc:4", Direction: "tx"}

	m.OnTxInput(&LossMonitorPacket{Key: tx1, Seq: 1, SSRC: 1}, now)
	m.OnTxInput(&LossMonitorPacket{Key: tx2, Seq: 2, SSRC: 2}, now)
	m.OnTxEmuDrop(&LossMonitorPacket{Key: tx2, Seq: 2, SSRC: 2}, now)
	m.OnRxInput(&LossMonitorPacket{Key: rx1, Seq: 10, SSRC: 3}, now)
	m.OnRxEmuDrop(&LossMonitorPacket{Key: rx1, Seq: 10, SSRC: 3}, now)

	// different peer ip should not be merged
	m.OnTxInput(&LossMonitorPacket{Key: other, Seq: 11, SSRC: 4}, now)
	m.OnTxEmuDrop(&LossMonitorPacket{Key: other, Seq: 11, SSRC: 4}, now)

	points := m.getSeriesByPeerIPAt(now, 1, "10.0.0.1")
	if len(points) != 1 {
		t.Fatalf("unexpected point len: got=%d want=1", len(points))
	}
	p := points[0]
	if p.TxInputPackets != 2 || p.TxEmuDroppedPackets != 1 {
		t.Fatalf("unexpected tx aggregate: %+v", p)
	}
	if p.RxInputPackets != 1 || p.RxEmuDroppedPackets != 1 {
		t.Fatalf("unexpected rx aggregate: %+v", p)
	}

	ips := m.ListPeerIPs()
	want := []string{"10.0.0.1", "10.0.0.2"}
	if !reflect.DeepEqual(ips, want) {
		t.Fatalf("unexpected peer ip list: got=%v want=%v", ips, want)
	}
}
