package main

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	weakNetDefaultFlushInterval = 2 * time.Millisecond
	weakNetDefaultIdleTTL       = 120 * time.Second

	weakNetDefaultMaxQueuePackets = 2000
	weakNetDefaultMaxQueueBytes   = int64(16 * 1024 * 1024)
	weakNetDefaultMaxResidenceMs  = 2000
	weakNetDefaultDelayCapMs      = 2000
)

type WeakNetFlowKey struct {
	PeerIP    string `json:"peer_ip"`
	PeerPort  int    `json:"peer_port"`
	StreamID  string `json:"stream_id"`
	Direction string `json:"direction"`
}

type WeakNetEmulationConfig struct {
	Enabled bool  `json:"enabled"`
	Seed    int64 `json:"seed"`

	FixedDelayMs  int    `json:"fixed_delay_ms"`
	JitterDelayMs int    `json:"jitter_delay_ms"`
	JitterDist    string `json:"jitter_dist"` // fixed / uniform / normal

	RandomLossRate float64 `json:"random_loss_rate"` // 0~1

	BurstLossEnabled bool    `json:"burst_loss_enabled"`
	PGoodToBad       float64 `json:"p_good_to_bad"`
	PBadToGood       float64 `json:"p_bad_to_good"`
	LossInGood       float64 `json:"loss_in_good"`
	LossInBad        float64 `json:"loss_in_bad"`

	FixedDropEveryN int `json:"fixed_drop_every_n"` // every N packets drop 1, 0 disabled

	MaxQueuePackets int   `json:"max_queue_packets"`
	MaxQueueBytes   int64 `json:"max_queue_bytes"`
	MaxResidenceMs  int   `json:"max_residence_ms"`
	DelayCapMs      int   `json:"delay_cap_ms"`
}

type WeakNetConfig struct {
	TxDefault WeakNetEmulationConfig `json:"tx_default"`
	RxDefault WeakNetEmulationConfig `json:"rx_default"`
}

type EmulatedPacket struct {
	Bytes     []byte
	ReleaseAt time.Time
	EnqueueAt time.Time
	SizeBytes int
	Seq       uint16
	SSRC      uint32
	Addr      *net.UDPAddr
}

type emulatedPacketHeap []EmulatedPacket

func (h emulatedPacketHeap) Len() int { return len(h) }

func (h emulatedPacketHeap) Less(i, j int) bool {
	if h[i].ReleaseAt.Equal(h[j].ReleaseAt) {
		return h[i].EnqueueAt.Before(h[j].EnqueueAt)
	}
	return h[i].ReleaseAt.Before(h[j].ReleaseAt)
}

func (h emulatedPacketHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *emulatedPacketHeap) Push(x any) { *h = append(*h, x.(EmulatedPacket)) }

func (h *emulatedPacketHeap) Pop() any {
	n := len(*h)
	if n == 0 {
		return EmulatedPacket{}
	}
	v := (*h)[n-1]
	*h = (*h)[:n-1]
	return v
}

func (h emulatedPacketHeap) Peek() (EmulatedPacket, bool) {
	if len(h) == 0 {
		return EmulatedPacket{}, false
	}
	return h[0], true
}

type WeakNetFlowStats struct {
	InPackets      uint64            `json:"in_packets"`
	OutPackets     uint64            `json:"out_packets"`
	DroppedPackets uint64            `json:"dropped_packets"`
	QueuePackets   int               `json:"queue_packets"`
	QueueBytes     int64             `json:"queue_bytes"`
	DropReasons    map[string]uint64 `json:"drop_reasons,omitempty"`
}

type WeakNetEmulationFlow struct {
	Key         WeakNetFlowKey
	Cfg         WeakNetEmulationConfig
	Heap        emulatedPacketHeap
	Rand        *rand.Rand
	PacketCount uint64
	BurstState  string // Good / Bad
	Stats       WeakNetFlowStats
	LastActive  time.Time
}

type WeakNetPacket struct {
	Bytes []byte
	Addr  *net.UDPAddr
	Seq   uint16
	SSRC  uint32
}

type weakNetDelivery struct {
	Bytes []byte
	Addr  *net.UDPAddr
}

type weakNetDirectionSnapshot struct {
	Direction    string                           `json:"direction"`
	DefaultCfg   WeakNetEmulationConfig           `json:"default_config"`
	FlowCount    int                              `json:"flow_count"`
	OverrideCnt  int                              `json:"override_count"`
	Totals       WeakNetFlowStats                 `json:"totals"`
	Flows        []WeakNetEmulationFlowSnapshot   `json:"flows"`
	OverrideList []WeakNetFlowOverrideConfigEntry `json:"overrides"`
}

type WeakNetEmulationFlowSnapshot struct {
	Key         WeakNetFlowKey `json:"key"`
	PacketCount uint64         `json:"packet_count"`
	BurstState  string         `json:"burst_state"`
	Stats       WeakNetFlowStats
}

type WeakNetFlowOverrideConfigEntry struct {
	Key    WeakNetFlowKey         `json:"key"`
	Config WeakNetEmulationConfig `json:"config"`
}

type weakNetDirectionEngine struct {
	direction string
	onOut     func(pkt []byte, addr *net.UDPAddr)
	monitor   *LossMonitor

	mu         sync.Mutex
	defaultCfg WeakNetEmulationConfig
	overrides  map[string]WeakNetEmulationConfig
	flows      map[string]*WeakNetEmulationFlow

	flushInterval time.Duration
	stopCh        chan struct{}
	doneCh        chan struct{}
}

type WeakNetEmulation struct {
	tx *weakNetDirectionEngine
	rx *weakNetDirectionEngine
}

func normalizeWeakNetEmulationConfig(cfg *WeakNetEmulationConfig) {
	if cfg == nil {
		return
	}
	if cfg.FixedDelayMs < 0 {
		cfg.FixedDelayMs = 0
	}
	if cfg.JitterDelayMs < 0 {
		cfg.JitterDelayMs = 0
	}
	cfg.JitterDist = strings.ToLower(strings.TrimSpace(cfg.JitterDist))
	switch cfg.JitterDist {
	case "", "fixed":
		cfg.JitterDist = "fixed"
	case "uniform", "normal":
	default:
		cfg.JitterDist = "fixed"
	}

	cfg.RandomLossRate = clampProbability(cfg.RandomLossRate)
	cfg.PGoodToBad = clampProbability(cfg.PGoodToBad)
	cfg.PBadToGood = clampProbability(cfg.PBadToGood)
	cfg.LossInGood = clampProbability(cfg.LossInGood)
	cfg.LossInBad = clampProbability(cfg.LossInBad)

	if cfg.FixedDropEveryN < 0 {
		cfg.FixedDropEveryN = 0
	}

	if cfg.MaxQueuePackets <= 0 {
		cfg.MaxQueuePackets = weakNetDefaultMaxQueuePackets
	}
	if cfg.MaxQueueBytes <= 0 {
		cfg.MaxQueueBytes = weakNetDefaultMaxQueueBytes
	}
	if cfg.MaxResidenceMs <= 0 {
		cfg.MaxResidenceMs = weakNetDefaultMaxResidenceMs
	}
	if cfg.DelayCapMs <= 0 {
		cfg.DelayCapMs = weakNetDefaultDelayCapMs
	}
}

func normalizeWeakNetConfig(cfg *WeakNetConfig) {
	if cfg == nil {
		return
	}
	normalizeWeakNetEmulationConfig(&cfg.TxDefault)
	normalizeWeakNetEmulationConfig(&cfg.RxDefault)
}

func cloneWeakNetEmulationConfig(in WeakNetEmulationConfig) WeakNetEmulationConfig {
	out := in
	normalizeWeakNetEmulationConfig(&out)
	return out
}

func clampProbability(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func weakNetFlowMapKey(peerIP string, peerPort int, streamID string) string {
	return peerIP + "|" + strconv.Itoa(peerPort) + "|" + streamID
}

func normalizeWeakNetFlowKey(key WeakNetFlowKey, direction string) (WeakNetFlowKey, error) {
	out := key
	out.Direction = strings.ToLower(strings.TrimSpace(direction))
	if out.Direction != "tx" && out.Direction != "rx" {
		return out, fmt.Errorf("invalid direction: %s", direction)
	}

	ip := net.ParseIP(strings.TrimSpace(out.PeerIP))
	if ip == nil {
		return out, fmt.Errorf("invalid peer_ip: %s", out.PeerIP)
	}
	out.PeerIP = ip.String()

	if out.PeerPort <= 0 || out.PeerPort > 65535 {
		return out, fmt.Errorf("invalid peer_port: %d", out.PeerPort)
	}

	out.StreamID = strings.TrimSpace(out.StreamID)
	if out.StreamID == "" {
		out.StreamID = "*"
	}
	return out, nil
}

func streamIDFromSSRC(ssrc uint32) string {
	if ssrc == 0 {
		return "*"
	}
	return "ssrc:" + itoaU32(ssrc)
}

func parseRTPSeqSSRCFast(pkt []byte) (uint16, uint32, bool) {
	if len(pkt) < 12 {
		return 0, 0, false
	}
	if (pkt[0] >> 6) != 2 {
		return 0, 0, false
	}
	seq := binary.BigEndian.Uint16(pkt[2:4])
	ssrc := binary.BigEndian.Uint32(pkt[8:12])
	return seq, ssrc, true
}

func weakNetFlowSeed(base int64, key WeakNetFlowKey) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key.PeerIP))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(strconv.Itoa(key.PeerPort)))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(key.StreamID))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(key.Direction))
	return base + int64(h.Sum64())
}

func newWeakNetDirectionEngine(direction string, defaultCfg WeakNetEmulationConfig, interval time.Duration, out func(pkt []byte, addr *net.UDPAddr)) *weakNetDirectionEngine {
	cfg := cloneWeakNetEmulationConfig(defaultCfg)
	if interval <= 0 {
		interval = weakNetDefaultFlushInterval
	}
	e := &weakNetDirectionEngine{
		direction:     direction,
		onOut:         out,
		defaultCfg:    cfg,
		overrides:     make(map[string]WeakNetEmulationConfig),
		flows:         make(map[string]*WeakNetEmulationFlow),
		flushInterval: interval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	go e.loop()
	return e
}

func (e *weakNetDirectionEngine) loop() {
	defer close(e.doneCh)
	tk := time.NewTicker(e.flushInterval)
	defer tk.Stop()
	for {
		select {
		case now := <-tk.C:
			e.flushDue(now)
		case <-e.stopCh:
			return
		}
	}
}

func (e *weakNetDirectionEngine) stop() {
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}
	<-e.doneCh
}

func (e *weakNetDirectionEngine) setLossMonitor(m *LossMonitor) {
	e.mu.Lock()
	e.monitor = m
	e.mu.Unlock()
}

func (e *weakNetDirectionEngine) updateDefault(cfg WeakNetEmulationConfig) {
	next := cloneWeakNetEmulationConfig(cfg)
	e.mu.Lock()
	e.defaultCfg = next
	e.mu.Unlock()
}

func (e *weakNetDirectionEngine) getDefault() WeakNetEmulationConfig {
	e.mu.Lock()
	defer e.mu.Unlock()
	return cloneWeakNetEmulationConfig(e.defaultCfg)
}

func (e *weakNetDirectionEngine) setOverride(key WeakNetFlowKey, cfg WeakNetEmulationConfig) error {
	nk, err := normalizeWeakNetFlowKey(key, e.direction)
	if err != nil {
		return err
	}
	next := cloneWeakNetEmulationConfig(cfg)
	flowKey := weakNetFlowMapKey(nk.PeerIP, nk.PeerPort, nk.StreamID)
	e.mu.Lock()
	e.overrides[flowKey] = next
	e.mu.Unlock()
	return nil
}

func (e *weakNetDirectionEngine) deleteOverride(key WeakNetFlowKey) error {
	nk, err := normalizeWeakNetFlowKey(key, e.direction)
	if err != nil {
		return err
	}
	flowKey := weakNetFlowMapKey(nk.PeerIP, nk.PeerPort, nk.StreamID)
	e.mu.Lock()
	delete(e.overrides, flowKey)
	e.mu.Unlock()
	return nil
}

func (e *weakNetDirectionEngine) getOverride(key WeakNetFlowKey) (WeakNetEmulationConfig, bool, error) {
	nk, err := normalizeWeakNetFlowKey(key, e.direction)
	if err != nil {
		return WeakNetEmulationConfig{}, false, err
	}
	flowKey := weakNetFlowMapKey(nk.PeerIP, nk.PeerPort, nk.StreamID)
	e.mu.Lock()
	defer e.mu.Unlock()
	cfg, ok := e.overrides[flowKey]
	if !ok {
		return WeakNetEmulationConfig{}, false, nil
	}
	return cloneWeakNetEmulationConfig(cfg), true, nil
}

func (e *weakNetDirectionEngine) listOverrides() []WeakNetFlowOverrideConfigEntry {
	e.mu.Lock()
	defer e.mu.Unlock()

	out := make([]WeakNetFlowOverrideConfigEntry, 0, len(e.overrides))
	for mk, cfg := range e.overrides {
		key := parseWeakNetFlowMapKey(mk, e.direction)
		out = append(out, WeakNetFlowOverrideConfigEntry{
			Key:    key,
			Config: cloneWeakNetEmulationConfig(cfg),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		ki := out[i].Key
		kj := out[j].Key
		if ki.PeerIP != kj.PeerIP {
			return ki.PeerIP < kj.PeerIP
		}
		if ki.PeerPort != kj.PeerPort {
			return ki.PeerPort < kj.PeerPort
		}
		return ki.StreamID < kj.StreamID
	})
	return out
}

func parseWeakNetFlowMapKey(mk string, direction string) WeakNetFlowKey {
	parts := strings.SplitN(mk, "|", 3)
	key := WeakNetFlowKey{Direction: direction}
	if len(parts) > 0 {
		key.PeerIP = parts[0]
	}
	if len(parts) > 1 {
		if p, err := strconv.Atoi(parts[1]); err == nil {
			key.PeerPort = p
		}
	}
	if len(parts) > 2 {
		key.StreamID = parts[2]
	}
	return key
}

func (e *weakNetDirectionEngine) resolveConfigLocked(key WeakNetFlowKey) WeakNetEmulationConfig {
	exact := weakNetFlowMapKey(key.PeerIP, key.PeerPort, key.StreamID)
	if cfg, ok := e.overrides[exact]; ok {
		return cloneWeakNetEmulationConfig(cfg)
	}
	wild := weakNetFlowMapKey(key.PeerIP, key.PeerPort, "*")
	if cfg, ok := e.overrides[wild]; ok {
		return cloneWeakNetEmulationConfig(cfg)
	}
	return cloneWeakNetEmulationConfig(e.defaultCfg)
}

func (e *weakNetDirectionEngine) getOrCreateFlowLocked(key WeakNetFlowKey, cfg WeakNetEmulationConfig) *WeakNetEmulationFlow {
	fk := weakNetFlowMapKey(key.PeerIP, key.PeerPort, key.StreamID)
	if f := e.flows[fk]; f != nil {
		f.Cfg = cfg
		return f
	}
	seed := weakNetFlowSeed(cfg.Seed, key)
	f := &WeakNetEmulationFlow{
		Key:        key,
		Cfg:        cfg,
		Heap:       make(emulatedPacketHeap, 0, 64),
		Rand:       rand.New(rand.NewSource(seed)),
		BurstState: "Good",
		Stats: WeakNetFlowStats{
			DropReasons: make(map[string]uint64),
		},
		LastActive: time.Now(),
	}
	heap.Init(&f.Heap)
	e.flows[fk] = f
	return f
}

func (e *weakNetDirectionEngine) notifyInputLocked(key WeakNetFlowKey, pkt *WeakNetPacket, now time.Time) {
	if e.monitor == nil || pkt == nil {
		return
	}
	mp := &LossMonitorPacket{
		Key:  key,
		Seq:  pkt.Seq,
		SSRC: pkt.SSRC,
	}
	if e.direction == "tx" {
		e.monitor.OnTxInput(mp, now)
	} else {
		e.monitor.OnRxInput(mp, now)
	}
}

func (e *weakNetDirectionEngine) notifyDropLocked(key WeakNetFlowKey, seq uint16, ssrc uint32, now time.Time) {
	if e.monitor == nil {
		return
	}
	mp := &LossMonitorPacket{
		Key:  key,
		Seq:  seq,
		SSRC: ssrc,
	}
	if e.direction == "tx" {
		e.monitor.OnTxEmuDrop(mp, now)
	} else {
		e.monitor.OnRxEmuDrop(mp, now)
	}
}

func (e *weakNetDirectionEngine) markDropLocked(f *WeakNetEmulationFlow, reason string) {
	if f == nil {
		return
	}
	f.Stats.DroppedPackets++
	if f.Stats.DropReasons == nil {
		f.Stats.DropReasons = make(map[string]uint64)
	}
	f.Stats.DropReasons[reason]++
}

func (e *weakNetDirectionEngine) shouldDropLocked(f *WeakNetEmulationFlow, seq uint16, ssrc uint32, now time.Time) bool {
	cfg := f.Cfg

	if cfg.FixedDropEveryN > 0 && f.PacketCount%uint64(cfg.FixedDropEveryN) == 0 {
		e.markDropLocked(f, "fixed_drop")
		e.notifyDropLocked(f.Key, seq, ssrc, now)
		return true
	}

	if cfg.RandomLossRate > 0 && f.Rand.Float64() < cfg.RandomLossRate {
		e.markDropLocked(f, "random_loss")
		e.notifyDropLocked(f.Key, seq, ssrc, now)
		return true
	}

	if cfg.BurstLossEnabled {
		if f.BurstState == "Bad" {
			if f.Rand.Float64() < cfg.PBadToGood {
				f.BurstState = "Good"
			}
		} else {
			if f.Rand.Float64() < cfg.PGoodToBad {
				f.BurstState = "Bad"
			}
		}
		lossP := cfg.LossInGood
		if f.BurstState == "Bad" {
			lossP = cfg.LossInBad
		}
		if lossP > 0 && f.Rand.Float64() < lossP {
			e.markDropLocked(f, "burst_loss")
			e.notifyDropLocked(f.Key, seq, ssrc, now)
			return true
		}
	}
	return false
}

func (e *weakNetDirectionEngine) sampleDelayLocked(f *WeakNetEmulationFlow) int {
	cfg := f.Cfg
	delay := cfg.FixedDelayMs
	if cfg.JitterDelayMs > 0 {
		switch cfg.JitterDist {
		case "uniform":
			span := cfg.JitterDelayMs * 2
			if span > 0 {
				jitter := f.Rand.Intn(span+1) - cfg.JitterDelayMs
				delay += jitter
			}
		case "normal":
			sigma := float64(cfg.JitterDelayMs) / 2.0
			jitter := int(math.Round(f.Rand.NormFloat64() * sigma))
			delay += jitter
		case "fixed":
			// no-op
		}
	}
	if delay < 0 {
		delay = 0
	}
	if cfg.DelayCapMs > 0 && delay > cfg.DelayCapMs {
		delay = cfg.DelayCapMs
	}
	return delay
}

func copyUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	out := *addr
	if addr.IP != nil {
		out.IP = append([]byte(nil), addr.IP...)
	}
	return &out
}

func (e *weakNetDirectionEngine) enqueue(key WeakNetFlowKey, pkt *WeakNetPacket, now time.Time) error {
	if pkt == nil || pkt.Addr == nil || len(pkt.Bytes) == 0 {
		return fmt.Errorf("invalid packet")
	}
	nk, err := normalizeWeakNetFlowKey(key, e.direction)
	if err != nil {
		return err
	}

	e.mu.Lock()
	cfg := e.resolveConfigLocked(nk)
	e.notifyInputLocked(nk, pkt, now)

	// Disabled: bypass without maintaining queue/flow state.
	if !cfg.Enabled {
		e.mu.Unlock()
		if e.onOut != nil {
			e.onOut(pkt.Bytes, pkt.Addr)
		}
		return nil
	}

	flow := e.getOrCreateFlowLocked(nk, cfg)
	flow.LastActive = now
	flow.PacketCount++
	flow.Stats.InPackets++

	if e.shouldDropLocked(flow, pkt.Seq, pkt.SSRC, now) {
		e.mu.Unlock()
		return nil
	}

	pktLen := len(pkt.Bytes)
	if cfg.MaxQueuePackets > 0 && flow.Stats.QueuePackets >= cfg.MaxQueuePackets {
		e.markDropLocked(flow, "queue_packets_full")
		e.notifyDropLocked(flow.Key, pkt.Seq, pkt.SSRC, now)
		e.mu.Unlock()
		return nil
	}
	if cfg.MaxQueueBytes > 0 && flow.Stats.QueueBytes+int64(pktLen) > cfg.MaxQueueBytes {
		e.markDropLocked(flow, "queue_bytes_full")
		e.notifyDropLocked(flow.Key, pkt.Seq, pkt.SSRC, now)
		e.mu.Unlock()
		return nil
	}

	delayMs := e.sampleDelayLocked(flow)
	releaseAt := now.Add(time.Duration(delayMs) * time.Millisecond)

	item := EmulatedPacket{
		Bytes:     append([]byte(nil), pkt.Bytes...),
		ReleaseAt: releaseAt,
		EnqueueAt: now,
		SizeBytes: pktLen,
		Seq:       pkt.Seq,
		SSRC:      pkt.SSRC,
		Addr:      copyUDPAddr(pkt.Addr),
	}
	heap.Push(&flow.Heap, item)
	flow.Stats.QueuePackets++
	flow.Stats.QueueBytes += int64(pktLen)

	deliveries := make([]weakNetDelivery, 0, 2)
	e.collectFlowDueLocked(flow, now, &deliveries)
	e.mu.Unlock()

	e.emitDeliveries(deliveries)
	return nil
}

func (e *weakNetDirectionEngine) collectFlowDueLocked(flow *WeakNetEmulationFlow, now time.Time, out *[]weakNetDelivery) {
	if flow == nil {
		return
	}
	e.pruneResidenceExpiredLocked(flow, now)
	for flow.Heap.Len() > 0 {
		top, ok := flow.Heap.Peek()
		if !ok || top.ReleaseAt.After(now) {
			break
		}
		item := heap.Pop(&flow.Heap).(EmulatedPacket)
		flow.Stats.QueuePackets--
		if flow.Stats.QueuePackets < 0 {
			flow.Stats.QueuePackets = 0
		}
		flow.Stats.QueueBytes -= int64(item.SizeBytes)
		if flow.Stats.QueueBytes < 0 {
			flow.Stats.QueueBytes = 0
		}

		if flow.Cfg.MaxResidenceMs > 0 && now.Sub(item.EnqueueAt) > time.Duration(flow.Cfg.MaxResidenceMs)*time.Millisecond {
			e.markDropLocked(flow, "max_residence")
			e.notifyDropLocked(flow.Key, item.Seq, item.SSRC, now)
			continue
		}

		flow.Stats.OutPackets++
		*out = append(*out, weakNetDelivery{
			Bytes: item.Bytes,
			Addr:  item.Addr,
		})
	}
	flow.LastActive = now
}

func (e *weakNetDirectionEngine) pruneResidenceExpiredLocked(flow *WeakNetEmulationFlow, now time.Time) {
	if flow == nil || flow.Heap.Len() == 0 || flow.Cfg.MaxResidenceMs <= 0 {
		return
	}
	maxResidence := time.Duration(flow.Cfg.MaxResidenceMs) * time.Millisecond
	filtered := make(emulatedPacketHeap, 0, flow.Heap.Len())
	dropped := 0
	droppedBytes := int64(0)
	droppedPackets := make([]EmulatedPacket, 0, 8)
	for _, item := range flow.Heap {
		if now.Sub(item.EnqueueAt) > maxResidence {
			dropped++
			droppedBytes += int64(item.SizeBytes)
			droppedPackets = append(droppedPackets, item)
			continue
		}
		filtered = append(filtered, item)
	}
	if dropped == 0 {
		return
	}
	flow.Heap = filtered
	heap.Init(&flow.Heap)
	flow.Stats.QueuePackets -= dropped
	if flow.Stats.QueuePackets < 0 {
		flow.Stats.QueuePackets = 0
	}
	flow.Stats.QueueBytes -= droppedBytes
	if flow.Stats.QueueBytes < 0 {
		flow.Stats.QueueBytes = 0
	}
	for _, item := range droppedPackets {
		e.markDropLocked(flow, "max_residence")
		e.notifyDropLocked(flow.Key, item.Seq, item.SSRC, now)
	}
}

func (e *weakNetDirectionEngine) flushDue(now time.Time) {
	deliveries := make([]weakNetDelivery, 0, 64)
	e.mu.Lock()
	for _, flow := range e.flows {
		e.collectFlowDueLocked(flow, now, &deliveries)
	}
	e.mu.Unlock()
	e.emitDeliveries(deliveries)
}

func (e *weakNetDirectionEngine) emitDeliveries(deliveries []weakNetDelivery) {
	if len(deliveries) == 0 || e.onOut == nil {
		return
	}
	for _, d := range deliveries {
		e.onOut(d.Bytes, d.Addr)
	}
}

func (e *weakNetDirectionEngine) resetFlow(key WeakNetFlowKey) error {
	nk, err := normalizeWeakNetFlowKey(key, e.direction)
	if err != nil {
		return err
	}
	mk := weakNetFlowMapKey(nk.PeerIP, nk.PeerPort, nk.StreamID)
	e.mu.Lock()
	delete(e.flows, mk)
	e.mu.Unlock()
	return nil
}

func (e *weakNetDirectionEngine) getFlowStats(key WeakNetFlowKey) (WeakNetFlowStats, bool, error) {
	nk, err := normalizeWeakNetFlowKey(key, e.direction)
	if err != nil {
		return WeakNetFlowStats{}, false, err
	}
	mk := weakNetFlowMapKey(nk.PeerIP, nk.PeerPort, nk.StreamID)
	e.mu.Lock()
	defer e.mu.Unlock()
	f := e.flows[mk]
	if f == nil {
		return WeakNetFlowStats{}, false, nil
	}
	return cloneWeakNetFlowStats(f.Stats), true, nil
}

func cloneWeakNetFlowStats(in WeakNetFlowStats) WeakNetFlowStats {
	out := in
	if in.DropReasons != nil {
		out.DropReasons = make(map[string]uint64, len(in.DropReasons))
		for k, v := range in.DropReasons {
			out.DropReasons[k] = v
		}
	}
	return out
}

func (e *weakNetDirectionEngine) snapshot() weakNetDirectionSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	snap := weakNetDirectionSnapshot{
		Direction:   e.direction,
		DefaultCfg:  cloneWeakNetEmulationConfig(e.defaultCfg),
		FlowCount:   len(e.flows),
		OverrideCnt: len(e.overrides),
		Totals: WeakNetFlowStats{
			DropReasons: make(map[string]uint64),
		},
		Flows:        make([]WeakNetEmulationFlowSnapshot, 0, len(e.flows)),
		OverrideList: make([]WeakNetFlowOverrideConfigEntry, 0, len(e.overrides)),
	}

	for mk, cfg := range e.overrides {
		snap.OverrideList = append(snap.OverrideList, WeakNetFlowOverrideConfigEntry{
			Key:    parseWeakNetFlowMapKey(mk, e.direction),
			Config: cloneWeakNetEmulationConfig(cfg),
		})
	}

	for _, f := range e.flows {
		fs := cloneWeakNetFlowStats(f.Stats)
		snap.Totals.InPackets += fs.InPackets
		snap.Totals.OutPackets += fs.OutPackets
		snap.Totals.DroppedPackets += fs.DroppedPackets
		snap.Totals.QueuePackets += fs.QueuePackets
		snap.Totals.QueueBytes += fs.QueueBytes
		for reason, cnt := range fs.DropReasons {
			snap.Totals.DropReasons[reason] += cnt
		}
		snap.Flows = append(snap.Flows, WeakNetEmulationFlowSnapshot{
			Key:         f.Key,
			PacketCount: f.PacketCount,
			BurstState:  f.BurstState,
			Stats:       fs,
		})
	}

	sort.Slice(snap.OverrideList, func(i, j int) bool {
		ki := snap.OverrideList[i].Key
		kj := snap.OverrideList[j].Key
		if ki.PeerIP != kj.PeerIP {
			return ki.PeerIP < kj.PeerIP
		}
		if ki.PeerPort != kj.PeerPort {
			return ki.PeerPort < kj.PeerPort
		}
		return ki.StreamID < kj.StreamID
	})
	sort.Slice(snap.Flows, func(i, j int) bool {
		ki := snap.Flows[i].Key
		kj := snap.Flows[j].Key
		if ki.PeerIP != kj.PeerIP {
			return ki.PeerIP < kj.PeerIP
		}
		if ki.PeerPort != kj.PeerPort {
			return ki.PeerPort < kj.PeerPort
		}
		return ki.StreamID < kj.StreamID
	})
	return snap
}

func (e *weakNetDirectionEngine) cleanupIdle(now time.Time, ttl time.Duration) int {
	if ttl <= 0 {
		ttl = weakNetDefaultIdleTTL
	}
	removed := 0
	e.mu.Lock()
	for mk, flow := range e.flows {
		if flow == nil {
			delete(e.flows, mk)
			removed++
			continue
		}
		if flow.Stats.QueuePackets > 0 {
			continue
		}
		if now.Sub(flow.LastActive) <= ttl {
			continue
		}
		delete(e.flows, mk)
		removed++
	}
	e.mu.Unlock()
	return removed
}

func NewWeakNetEmulation(txDefault WeakNetEmulationConfig, rxDefault WeakNetEmulationConfig, txOut func(pkt []byte, addr *net.UDPAddr), rxOut func(pkt []byte, addr *net.UDPAddr)) *WeakNetEmulation {
	return &WeakNetEmulation{
		tx: newWeakNetDirectionEngine("tx", txDefault, weakNetDefaultFlushInterval, txOut),
		rx: newWeakNetDirectionEngine("rx", rxDefault, weakNetDefaultFlushInterval, rxOut),
	}
}

func (w *WeakNetEmulation) SetLossMonitor(m *LossMonitor) {
	if w == nil {
		return
	}
	if w.tx != nil {
		w.tx.setLossMonitor(m)
	}
	if w.rx != nil {
		w.rx.setLossMonitor(m)
	}
}

func (w *WeakNetEmulation) Close() {
	if w == nil {
		return
	}
	if w.tx != nil {
		w.tx.stop()
	}
	if w.rx != nil {
		w.rx.stop()
	}
}

func (w *WeakNetEmulation) EnqueueTx(key WeakNetFlowKey, pkt *WeakNetPacket, now time.Time) error {
	if w == nil || w.tx == nil {
		return fmt.Errorf("weaknet tx unavailable")
	}
	return w.tx.enqueue(key, pkt, now)
}

func (w *WeakNetEmulation) EnqueueRx(key WeakNetFlowKey, pkt *WeakNetPacket, now time.Time) error {
	if w == nil || w.rx == nil {
		return fmt.Errorf("weaknet rx unavailable")
	}
	return w.rx.enqueue(key, pkt, now)
}

func (w *WeakNetEmulation) FlushDue(now time.Time) {
	if w == nil {
		return
	}
	if w.tx != nil {
		w.tx.flushDue(now)
	}
	if w.rx != nil {
		w.rx.flushDue(now)
	}
}

func (w *WeakNetEmulation) UpdateTxConfig(key WeakNetFlowKey, cfg WeakNetEmulationConfig) error {
	if w == nil || w.tx == nil {
		return fmt.Errorf("weaknet tx unavailable")
	}
	return w.tx.setOverride(key, cfg)
}

func (w *WeakNetEmulation) UpdateRxConfig(key WeakNetFlowKey, cfg WeakNetEmulationConfig) error {
	if w == nil || w.rx == nil {
		return fmt.Errorf("weaknet rx unavailable")
	}
	return w.rx.setOverride(key, cfg)
}

func (w *WeakNetEmulation) DeleteTxConfig(key WeakNetFlowKey) error {
	if w == nil || w.tx == nil {
		return fmt.Errorf("weaknet tx unavailable")
	}
	return w.tx.deleteOverride(key)
}

func (w *WeakNetEmulation) DeleteRxConfig(key WeakNetFlowKey) error {
	if w == nil || w.rx == nil {
		return fmt.Errorf("weaknet rx unavailable")
	}
	return w.rx.deleteOverride(key)
}

func (w *WeakNetEmulation) GetTxConfig(key WeakNetFlowKey) (WeakNetEmulationConfig, bool, error) {
	if w == nil || w.tx == nil {
		return WeakNetEmulationConfig{}, false, fmt.Errorf("weaknet tx unavailable")
	}
	return w.tx.getOverride(key)
}

func (w *WeakNetEmulation) GetRxConfig(key WeakNetFlowKey) (WeakNetEmulationConfig, bool, error) {
	if w == nil || w.rx == nil {
		return WeakNetEmulationConfig{}, false, fmt.Errorf("weaknet rx unavailable")
	}
	return w.rx.getOverride(key)
}

func (w *WeakNetEmulation) ListTxConfig() []WeakNetFlowOverrideConfigEntry {
	if w == nil || w.tx == nil {
		return nil
	}
	return w.tx.listOverrides()
}

func (w *WeakNetEmulation) ListRxConfig() []WeakNetFlowOverrideConfigEntry {
	if w == nil || w.rx == nil {
		return nil
	}
	return w.rx.listOverrides()
}

func (w *WeakNetEmulation) UpdateTxDefault(cfg WeakNetEmulationConfig) {
	if w == nil || w.tx == nil {
		return
	}
	w.tx.updateDefault(cfg)
}

func (w *WeakNetEmulation) UpdateRxDefault(cfg WeakNetEmulationConfig) {
	if w == nil || w.rx == nil {
		return
	}
	w.rx.updateDefault(cfg)
}

func (w *WeakNetEmulation) GetTxDefault() WeakNetEmulationConfig {
	if w == nil || w.tx == nil {
		return WeakNetEmulationConfig{}
	}
	return w.tx.getDefault()
}

func (w *WeakNetEmulation) GetRxDefault() WeakNetEmulationConfig {
	if w == nil || w.rx == nil {
		return WeakNetEmulationConfig{}
	}
	return w.rx.getDefault()
}

func (w *WeakNetEmulation) GetStats(key WeakNetFlowKey) (WeakNetFlowStats, bool, error) {
	if w == nil {
		return WeakNetFlowStats{}, false, fmt.Errorf("weaknet unavailable")
	}
	dir := strings.ToLower(strings.TrimSpace(key.Direction))
	switch dir {
	case "tx":
		if w.tx == nil {
			return WeakNetFlowStats{}, false, fmt.Errorf("weaknet tx unavailable")
		}
		return w.tx.getFlowStats(key)
	case "rx":
		if w.rx == nil {
			return WeakNetFlowStats{}, false, fmt.Errorf("weaknet rx unavailable")
		}
		return w.rx.getFlowStats(key)
	default:
		return WeakNetFlowStats{}, false, fmt.Errorf("invalid direction: %s", key.Direction)
	}
}

func (w *WeakNetEmulation) Reset(key WeakNetFlowKey) error {
	if w == nil {
		return fmt.Errorf("weaknet unavailable")
	}
	dir := strings.ToLower(strings.TrimSpace(key.Direction))
	switch dir {
	case "tx":
		if w.tx == nil {
			return fmt.Errorf("weaknet tx unavailable")
		}
		return w.tx.resetFlow(key)
	case "rx":
		if w.rx == nil {
			return fmt.Errorf("weaknet rx unavailable")
		}
		return w.rx.resetFlow(key)
	default:
		return fmt.Errorf("invalid direction: %s", key.Direction)
	}
}

func (w *WeakNetEmulation) SnapshotStats() map[string]any {
	if w == nil {
		return map[string]any{}
	}
	out := map[string]any{}
	if w.tx != nil {
		out["tx"] = w.tx.snapshot()
	}
	if w.rx != nil {
		out["rx"] = w.rx.snapshot()
	}
	return out
}

func (w *WeakNetEmulation) CleanupIdle(now time.Time, ttl time.Duration) (int, int) {
	if w == nil {
		return 0, 0
	}
	txRemoved := 0
	rxRemoved := 0
	if w.tx != nil {
		txRemoved = w.tx.cleanupIdle(now, ttl)
	}
	if w.rx != nil {
		rxRemoved = w.rx.cleanupIdle(now, ttl)
	}
	return txRemoved, rxRemoved
}

type weakNetTxWriter struct {
	relay *Relay
}

func (w *weakNetTxWriter) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	if w == nil || w.relay == nil {
		return 0, fmt.Errorf("weaknet tx writer unavailable")
	}
	if addr == nil {
		return 0, fmt.Errorf("nil udp addr")
	}
	if len(b) == 0 {
		return 0, nil
	}
	seq, ssrc, _ := parseRTPSeqSSRCFast(b)
	key := w.relay.makeWeakNetFlowKey(addr, ssrc, "tx")
	err := w.relay.weaknet.EnqueueTx(key, &WeakNetPacket{
		Bytes: b,
		Addr:  addr,
		Seq:   seq,
		SSRC:  ssrc,
	}, time.Now())
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (r *Relay) makeWeakNetFlowKey(peer *net.UDPAddr, ssrc uint32, direction string) WeakNetFlowKey {
	key := WeakNetFlowKey{
		Direction: strings.ToLower(strings.TrimSpace(direction)),
		StreamID:  streamIDFromSSRC(ssrc),
	}
	if peer != nil {
		if peer.IP != nil {
			key.PeerIP = peer.IP.String()
		}
		key.PeerPort = peer.Port
	}
	return key
}

func (r *Relay) applyWeakNetDefaultsToRuntime() {
	if r == nil || r.weaknet == nil || r.cfg == nil {
		return
	}
	r.weaknet.UpdateTxDefault(r.cfg.WeakNet.TxDefault)
	r.weaknet.UpdateRxDefault(r.cfg.WeakNet.RxDefault)
}

func (r *Relay) initWeakNet() {
	if r == nil || r.rtpConn == nil || r.cfg == nil {
		return
	}
	txOut := func(pkt []byte, addr *net.UDPAddr) {
		if addr == nil || len(pkt) == 0 {
			return
		}
		if _, err := r.rtpConn.WriteToUDP(pkt, addr); err != nil {
			log.Printf("[weaknet][tx] udp write failed: target=%s len=%d err=%v", addr.String(), len(pkt), err)
		}
	}
	rxOut := func(pkt []byte, addr *net.UDPAddr) {
		if addr == nil || len(pkt) == 0 {
			return
		}
		r.handleIncomingRTPPacket(pkt, addr)
	}

	if r.weaknet == nil {
		r.weaknet = NewWeakNetEmulation(r.cfg.WeakNet.TxDefault, r.cfg.WeakNet.RxDefault, txOut, rxOut)
	} else {
		r.applyWeakNetDefaultsToRuntime()
	}
	r.weaknet.SetLossMonitor(r.lossMonitor)
	r.rtpWriter = &weakNetTxWriter{relay: r}
}

func weakNetPreset(name string) (WeakNetEmulationConfig, bool) {
	n := strings.ToLower(strings.TrimSpace(name))
	cfg := WeakNetEmulationConfig{Enabled: true}
	switch n {
	case "light", "mild", "preset_light":
		cfg.FixedDelayMs = 30
		cfg.JitterDelayMs = 10
		cfg.JitterDist = "uniform"
		cfg.RandomLossRate = 0.005
		cfg.BurstLossEnabled = false
		cfg.FixedDropEveryN = 0
	case "medium", "moderate", "preset_medium":
		cfg.FixedDelayMs = 80
		cfg.JitterDelayMs = 30
		cfg.JitterDist = "normal"
		cfg.RandomLossRate = 0
		cfg.BurstLossEnabled = true
		cfg.PGoodToBad = 0.015
		cfg.PBadToGood = 0.25
		cfg.LossInGood = 0.003
		cfg.LossInBad = 0.25
		cfg.FixedDropEveryN = 0
	case "fixed_drop", "fixed_drop_test", "preset_fixed_drop":
		cfg.FixedDelayMs = 50
		cfg.JitterDelayMs = 0
		cfg.JitterDist = "fixed"
		cfg.RandomLossRate = 0
		cfg.BurstLossEnabled = false
		cfg.FixedDropEveryN = 20
	default:
		return WeakNetEmulationConfig{}, false
	}
	normalizeWeakNetEmulationConfig(&cfg)
	return cfg, true
}
