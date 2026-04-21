package main

import (
	"container/heap"
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"
)

// PacerPriority marks relative priority for queued packets.
type PacerPriority uint8

const (
	PacerPriorityNormal PacerPriority = iota
	PacerPriorityHigh
)

const pacerDefaultStartRateRatio = 0.618

// UDPPacketWriter abstracts UDP send so pacer can be tested independently.
type UDPPacketWriter interface {
	WriteToUDP(b []byte, addr *net.UDPAddr) (int, error)
}

// PacerConfig controls pacing and queue behavior.
type PacerConfig struct {
	Enabled bool

	// Average egress bitrate in bits/sec.
	RateBps int
	// Average egress rate in bytes/sec (legacy/compat field).
	RateBytesPerSec int
	// Tick cadence for refill/scheduling.
	Tick time.Duration
	// Maximum credit accumulation in bytes.
	BurstBytes int

	// Deprecated: packet-count cap is intentionally disabled to avoid drops
	// when queue builds up; queue is drained by dynamic pacing instead.
	PerFlowQueuePackets int
	// Global queue caps for this pacer instance.
	// These prevent unbounded memory growth when downstream is congested.
	MaxQueuePackets int
	MaxQueueBytes   int
	// Upper bound on live flows to avoid unbounded memory growth.
	MaxFlows int
	// Max bytes emitted in one flush round.
	FlushBudgetBytes int

	LogEnabled bool
	LogPeriod  time.Duration
	// EnableAdaptiveEstimator toggles follow-rate shaping module.
	EnableAdaptiveEstimator bool

	// Follow-rate estimator knobs.
	ShortEWMA       time.Duration
	LongEWMA        time.Duration
	ShortHeadroom   float64
	LongHeadroom    float64
	RTXReservePct   float64
	RTCPOverheadPct float64

	// Hard-cap hierarchy and floor.
	NegotiatedCapBps int
	AdminCapBps      int
	EgressCapBps     int
	StartRateBps     int
	FloorRateBps     int

	// Queue-delay guardrails.
	QueueDelayTarget       time.Duration
	QueueHighWatermark     time.Duration
	QueueSustain           time.Duration
	QueueBoostFactor       float64
	ForceHardCapQueueDelay time.Duration
	TargetMaxUpPerSec      float64

	// Feedback-based slow down / recovery controls.
	FeedbackLossHigh    float64
	FeedbackLossMid     float64
	FeedbackDropHigh    float64
	FeedbackDropMid     float64
	FeedbackMinScale    float64
	FeedbackRecoverGain float64
	FeedbackRecoverMS   time.Duration
	FeedbackStepMS      time.Duration
	FeedbackMidCount    int
	FeedbackEnabled     bool
}

// PacerStats is a snapshot of runtime counters.
type PacerStats struct {
	EnqueuedPackets uint64
	EnqueuedBytes   uint64
	SentPackets     uint64
	SentBytes       uint64
	SendErrors      uint64
	DroppedPackets  uint64
	DroppedBytes    uint64

	QueueFlows   int
	QueuePackets int
	QueueBytes   int
	CreditsBytes float64
}

type pacerPacket struct {
	job          ForwardJob
	enqueueAt    time.Time
	enqueueOrder uint64
	seq          uint16
	hasSeq       bool
	sortKey      uint64
}

type packetQueue struct {
	items []pacerPacket
}

func (q *packetQueue) Len() int {
	return len(q.items)
}

func (q *packetQueue) Less(i int, j int) bool {
	return pacerPacketLess(q.items[i], q.items[j])
}

func (q *packetQueue) Swap(i int, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

func (q *packetQueue) Push(x any) {
	q.items = append(q.items, x.(pacerPacket))
}

func (q *packetQueue) Pop() any {
	n := len(q.items)
	if n == 0 {
		return pacerPacket{}
	}
	item := q.items[n-1]
	q.items = q.items[:n-1]
	return item
}

func (q *packetQueue) Enqueue(pkt pacerPacket) {
	heap.Push(q, pkt)
}

func (q *packetQueue) Peek() (pacerPacket, bool) {
	if q.Len() == 0 {
		return pacerPacket{}, false
	}
	return q.items[0], true
}

func (q *packetQueue) Dequeue() (pacerPacket, bool) {
	if q.Len() == 0 {
		return pacerPacket{}, false
	}
	pkt := heap.Pop(q).(pacerPacket)
	return pkt, true
}

func pacerPacketLess(a pacerPacket, b pacerPacket) bool {
	if a.hasSeq && b.hasSeq {
		if a.sortKey == b.sortKey {
			return a.enqueueOrder < b.enqueueOrder
		}
		return a.sortKey < b.sortKey
	}
	if a.hasSeq != b.hasSeq {
		return a.hasSeq
	}
	return a.enqueueOrder < b.enqueueOrder
}

func parseRTPSequence(pkt []byte) (uint16, bool) {
	if len(pkt) < 12 {
		return 0, false
	}
	if (pkt[0] >> 6) != 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(pkt[2:4]), true
}

type pacerFlow struct {
	key        string
	highQ      packetQueue
	normalQ    packetQueue
	queueBytes int
	seqRefSet  bool
	seqRef     uint64
}

func (f *pacerFlow) queuePackets() int {
	return f.highQ.Len() + f.normalQ.Len()
}

func (f *pacerFlow) push(pkt pacerPacket, prio PacerPriority) {
	if prio == PacerPriorityHigh {
		f.highQ.Enqueue(pkt)
	} else {
		f.normalQ.Enqueue(pkt)
	}
	f.queueBytes += len(pkt.job.pkt)
}

func (f *pacerFlow) peek() (pacerPacket, PacerPriority, bool) {
	if pkt, ok := f.highQ.Peek(); ok {
		return pkt, PacerPriorityHigh, true
	}
	if pkt, ok := f.normalQ.Peek(); ok {
		return pkt, PacerPriorityNormal, true
	}
	return pacerPacket{}, PacerPriorityNormal, false
}

func (f *pacerFlow) pop() (pacerPacket, PacerPriority, bool) {
	if pkt, ok := f.highQ.Dequeue(); ok {
		f.queueBytes -= len(pkt.job.pkt)
		if f.queueBytes < 0 {
			f.queueBytes = 0
		}
		return pkt, PacerPriorityHigh, true
	}
	if pkt, ok := f.normalQ.Dequeue(); ok {
		f.queueBytes -= len(pkt.job.pkt)
		if f.queueBytes < 0 {
			f.queueBytes = 0
		}
		return pkt, PacerPriorityNormal, true
	}
	return pacerPacket{}, PacerPriorityNormal, false
}

func (f *pacerFlow) dropOldestNormal() (pacerPacket, bool) {
	pkt, ok := f.normalQ.Dequeue()
	if !ok {
		return pacerPacket{}, false
	}
	f.queueBytes -= len(pkt.job.pkt)
	if f.queueBytes < 0 {
		f.queueBytes = 0
	}
	return pkt, true
}

func (f *pacerFlow) decoratePacketForQueue(pkt *pacerPacket) {
	seq, ok := parseRTPSequence(pkt.job.pkt)
	if !ok {
		pkt.hasSeq = false
		return
	}
	pkt.seq = seq
	pkt.hasSeq = true
	if !f.seqRefSet {
		f.seqRefSet = true
		f.seqRef = uint64(seq)
		pkt.sortKey = f.seqRef
		return
	}

	ref := int64(f.seqRef)
	refLow := int64(uint16(f.seqRef))
	diff := int64(seq) - refLow
	if diff > 32767 {
		diff -= 65536
	} else if diff < -32768 {
		diff += 65536
	}

	candidate := ref + diff
	if candidate < 0 {
		candidate = 0
	}
	pkt.sortKey = uint64(candidate)
	if pkt.sortKey > f.seqRef {
		f.seqRef = pkt.sortKey
	}
}

func normalizePacerConfig(cfg PacerConfig) PacerConfig {
	if cfg.Tick <= 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.RateBps <= 0 {
		if cfg.RateBytesPerSec > 0 {
			cfg.RateBps = cfg.RateBytesPerSec * 8
		} else {
			cfg.RateBps = 20_000_000
		}
	}
	if cfg.RateBytesPerSec <= 0 {
		cfg.RateBytesPerSec = cfg.RateBps / 8
	}
	if cfg.RateBytesPerSec <= 0 {
		cfg.RateBytesPerSec = 1
	}
	if cfg.AdminCapBps <= 0 {
		cfg.AdminCapBps = cfg.RateBps
	}
	if cfg.FloorRateBps <= 0 {
		// Conservative video floor; audio-only can override to lower value.
		cfg.FloorRateBps = 300_000
	}
	if cfg.ShortEWMA <= 0 {
		cfg.ShortEWMA = 250 * time.Millisecond
	}
	if cfg.LongEWMA <= 0 {
		cfg.LongEWMA = 1500 * time.Millisecond
	}
	if cfg.LongEWMA < cfg.ShortEWMA {
		cfg.LongEWMA = cfg.ShortEWMA * 2
	}
	if cfg.ShortHeadroom <= 0 {
		cfg.ShortHeadroom = 1.15
	}
	if cfg.ShortHeadroom < 1 {
		cfg.ShortHeadroom = 1
	}
	if cfg.LongHeadroom <= 0 {
		cfg.LongHeadroom = 1.05
	}
	if cfg.LongHeadroom < 1 {
		cfg.LongHeadroom = 1
	}
	if cfg.RTXReservePct < 0 {
		cfg.RTXReservePct = 0
	}
	if cfg.RTXReservePct > 0.5 {
		cfg.RTXReservePct = 0.5
	}
	if cfg.RTCPOverheadPct <= 0 {
		cfg.RTCPOverheadPct = 0.04
	}
	if cfg.RTCPOverheadPct > 0.2 {
		cfg.RTCPOverheadPct = 0.2
	}
	if cfg.StartRateBps < 0 {
		cfg.StartRateBps = 0
	}
	if cfg.QueueDelayTarget <= 0 {
		cfg.QueueDelayTarget = 100 * time.Millisecond
	}
	if cfg.QueueHighWatermark <= 0 {
		cfg.QueueHighWatermark = 2 * cfg.QueueDelayTarget
	}
	if cfg.QueueHighWatermark < cfg.QueueDelayTarget {
		cfg.QueueHighWatermark = cfg.QueueDelayTarget
	}
	if cfg.QueueSustain <= 0 {
		cfg.QueueSustain = 1200 * time.Millisecond
	}
	if cfg.QueueBoostFactor <= 1 {
		cfg.QueueBoostFactor = 1.15
	}
	if cfg.ForceHardCapQueueDelay <= 0 {
		cfg.ForceHardCapQueueDelay = 1200 * time.Millisecond
	}
	if cfg.TargetMaxUpPerSec <= 1 {
		cfg.TargetMaxUpPerSec = 1.20
	}
	if cfg.FeedbackLossHigh <= 0 || cfg.FeedbackLossHigh >= 1 {
		cfg.FeedbackLossHigh = 0.10
	}
	if cfg.FeedbackLossMid <= 0 || cfg.FeedbackLossMid >= cfg.FeedbackLossHigh {
		cfg.FeedbackLossMid = 0.05
	}
	if cfg.FeedbackDropHigh <= 0 || cfg.FeedbackDropHigh > 1 {
		cfg.FeedbackDropHigh = 0.70
	}
	if cfg.FeedbackDropMid <= 0 || cfg.FeedbackDropMid > 1 {
		cfg.FeedbackDropMid = 0.85
	}
	if cfg.FeedbackMinScale <= 0 || cfg.FeedbackMinScale > 1 {
		cfg.FeedbackMinScale = 0.30
	}
	if cfg.FeedbackRecoverGain <= 1 || cfg.FeedbackRecoverGain > 1.5 {
		cfg.FeedbackRecoverGain = 1.04
	}
	if cfg.FeedbackRecoverMS <= 0 {
		cfg.FeedbackRecoverMS = 8 * time.Second
	}
	if cfg.FeedbackStepMS <= 0 {
		cfg.FeedbackStepMS = 2 * time.Second
	}
	if cfg.FeedbackMidCount <= 0 {
		cfg.FeedbackMidCount = 2
	}
	if cfg.BurstBytes <= 0 {
		floorBytesPerSec := cfg.FloorRateBps / 8
		cfg.BurstBytes = floorBytesPerSec / 20 // 50ms floor-rate burst.
	}
	if cfg.BurstBytes < 1500 {
		cfg.BurstBytes = 1500
	}
	if cfg.PerFlowQueuePackets < 0 {
		cfg.PerFlowQueuePackets = 0
	}
	if cfg.MaxQueuePackets <= 0 {
		cfg.MaxQueuePackets = 20000
	}
	if cfg.MaxQueueBytes <= 0 {
		cfg.MaxQueueBytes = 64 * 1024 * 1024
	}
	if cfg.MaxFlows <= 0 {
		cfg.MaxFlows = 4096
	}
	// Flush budget can stay zero to enable adaptive per-tick budgeting.
	if cfg.FlushBudgetBytes < 0 {
		cfg.FlushBudgetBytes = 0
	}
	if cfg.LogPeriod < 0 {
		cfg.LogPeriod = 0
	}
	if cfg.LogEnabled && cfg.LogPeriod == 0 {
		cfg.LogPeriod = 2 * time.Second
	}
	return cfg
}

func cfgHardCapBps(cfg PacerConfig) int {
	capBps := 0
	pickMin := func(v int) {
		if v <= 0 {
			return
		}
		if capBps <= 0 || v < capBps {
			capBps = v
		}
	}
	pickMin(cfg.NegotiatedCapBps)
	pickMin(cfg.AdminCapBps)
	pickMin(cfg.EgressCapBps)
	if capBps <= 0 {
		pickMin(cfg.RateBps)
	}
	if capBps <= 0 && cfg.RateBytesPerSec > 0 {
		capBps = cfg.RateBytesPerSec * 8
	}
	if capBps <= 0 {
		capBps = 1
	}
	return capBps
}

func cfgHardCapBytesPerSec(cfg PacerConfig) float64 {
	capBps := cfgHardCapBps(cfg)
	if capBps <= 0 {
		capBps = 1
	}
	return float64(capBps) / 8.0
}

func cfgFloorBytesPerSec(cfg PacerConfig) float64 {
	floorBps := cfg.FloorRateBps
	if floorBps <= 0 {
		floorBps = 300_000
	}
	floor := float64(floorBps) / 8.0
	if floor < 1 {
		floor = 1
	}
	return floor
}

func cfgStartRateBytesPerSec(cfg PacerConfig) float64 {
	floor := cfgFloorBytesPerSec(cfg)
	hardCap := cfgHardCapBytesPerSec(cfg)
	if floor > hardCap {
		floor = hardCap
	}

	start := floor
	if cfg.StartRateBps > 0 {
		start = float64(cfg.StartRateBps) / 8.0
		if start < floor {
			start = floor
		}
		if start > hardCap {
			start = hardCap
		}
	} else {
		start = hardCap * pacerDefaultStartRateRatio
		if start < floor {
			start = floor
		}
		if start > hardCap {
			start = hardCap
		}
	}

	if start < 1 {
		start = 1
	}
	return start
}

type pacedSend struct {
	pkt      pacerPacket
	prio     PacerPriority
	queueDur time.Duration
}

type rollingByteBuckets struct {
	bucketDur   time.Duration
	count       int
	values      []uint64
	lastSlot    int64
	initialized bool
}

func newRollingByteBuckets(bucketDur time.Duration, count int) rollingByteBuckets {
	if bucketDur <= 0 {
		bucketDur = time.Second
	}
	if count <= 0 {
		count = 1
	}
	return rollingByteBuckets{
		bucketDur: bucketDur,
		count:     count,
		values:    make([]uint64, count),
	}
}

func (rb *rollingByteBuckets) slotAt(t time.Time) int64 {
	return t.UnixNano() / int64(rb.bucketDur)
}

func (rb *rollingByteBuckets) advanceTo(t time.Time) {
	if rb.count <= 0 {
		return
	}
	slot := rb.slotAt(t)
	if !rb.initialized {
		rb.lastSlot = slot
		rb.initialized = true
		return
	}
	if slot <= rb.lastSlot {
		return
	}
	step := slot - rb.lastSlot
	if step >= int64(rb.count) {
		for i := range rb.values {
			rb.values[i] = 0
		}
		rb.lastSlot = slot
		return
	}
	for i := int64(1); i <= step; i++ {
		rb.values[(rb.lastSlot+i)%int64(rb.count)] = 0
	}
	rb.lastSlot = slot
}

func (rb *rollingByteBuckets) add(t time.Time, bytes int) {
	rb.advanceTo(t)
	if !rb.initialized || bytes <= 0 || rb.count <= 0 {
		return
	}
	rb.values[rb.lastSlot%int64(rb.count)] += uint64(bytes)
}

func (rb *rollingByteBuckets) snapshot(t time.Time) []uint64 {
	rb.advanceTo(t)
	out := make([]uint64, rb.count)
	if !rb.initialized || rb.count <= 0 {
		return out
	}
	start := rb.lastSlot - int64(rb.count) + 1
	for i := 0; i < rb.count; i++ {
		slot := start + int64(i)
		out[i] = rb.values[slot%int64(rb.count)]
	}
	return out
}

// Pacer provides flow-fair egress pacing for RTP relay packets.
type Pacer struct {
	writer UDPPacketWriter

	mu sync.Mutex

	cfg PacerConfig

	flows       map[string]*pacerFlow
	flowOrder   []string
	nextFlowIdx int

	queuePackets int
	queueBytes   int
	enqueueOrder uint64

	credits    float64
	lastRefill time.Time

	// Runtime rate control: base comes from config, adjusted is temporary
	// target used by token refill for egress shaping.
	adjustedRateBytesPerSec  float64
	minRateNeededBytesPerSec float64
	followRateBytesPerSec    float64
	targetRateBytesPerSec    float64
	avgQueueDelaySec         float64

	ingressShortEWMA   adaptiveRateEWMA
	ingressLongEWMA    adaptiveRateEWMA
	ingressSampleAt    time.Time
	ingressSampleBytes int

	feedbackScale      float64
	highLossStreak     int
	lowLossStableSince time.Time
	lastRecoveryStep   time.Time

	queueHighSince     time.Time
	lastTargetUpdateAt time.Time

	estDebugWindowStart time.Time
	estDebugSamples     []pacerEstimatorDebugSample

	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	lastLogAt time.Time

	stats PacerStats

	rate500ms   rollingByteBuckets
	rate1s      rollingByteBuckets
	instanceKey string
	direction   string

	onSent func(job ForwardJob, sentBytes int)
}

func NewPacer(writer UDPPacketWriter, cfg PacerConfig) *Pacer {
	norm := normalizePacerConfig(cfg)
	startRate := cfgStartRateBytesPerSec(norm)
	p := &Pacer{
		writer:                   writer,
		cfg:                      norm,
		flows:                    make(map[string]*pacerFlow),
		rate500ms:                newRollingByteBuckets(500*time.Millisecond, 10),
		rate1s:                   newRollingByteBuckets(time.Second, 5),
		adjustedRateBytesPerSec:  startRate,
		minRateNeededBytesPerSec: startRate,
		followRateBytesPerSec:    startRate,
		targetRateBytesPerSec:    startRate,
		ingressShortEWMA:         newAdaptiveRateEWMA(norm.ShortEWMA),
		ingressLongEWMA:          newAdaptiveRateEWMA(norm.LongEWMA),
		feedbackScale:            1.0,
		instanceKey:              "unknown",
		direction:                "egress",
	}
	if p.cfg.Enabled {
		p.startLoopLocked(time.Now())
	}
	return p
}

func (p *Pacer) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running {
		return
	}
	p.cfg.Enabled = true
	p.startLoopLocked(time.Now())
}

func (p *Pacer) Stop() {
	p.stopLoop(true)
}

func (p *Pacer) UpdateConfig(cfg PacerConfig) {
	cfg = normalizePacerConfig(cfg)

	p.mu.Lock()
	oldTick := p.cfg.Tick
	wasRunning := p.running
	wasEnabled := p.cfg.Enabled
	p.cfg = cfg
	p.ingressShortEWMA.setTau(cfg.ShortEWMA)
	p.ingressLongEWMA.setTau(cfg.LongEWMA)
	if p.feedbackScale <= 0 || p.feedbackScale > 1 {
		p.feedbackScale = 1
	}
	hardCap := cfgHardCapBytesPerSec(cfg)
	if hardCap < 1 {
		hardCap = 1
	}
	floorRate := cfgFloorBytesPerSec(cfg)
	if floorRate > hardCap {
		floorRate = hardCap
	}
	defaultRate := cfgStartRateBytesPerSec(cfg)
	if p.adjustedRateBytesPerSec <= 0 {
		p.adjustedRateBytesPerSec = defaultRate
	}
	if p.adjustedRateBytesPerSec > hardCap {
		p.adjustedRateBytesPerSec = hardCap
	}
	if p.minRateNeededBytesPerSec < 0 {
		p.minRateNeededBytesPerSec = 0
	}
	if p.followRateBytesPerSec < 0 {
		p.followRateBytesPerSec = 0
	}
	if p.targetRateBytesPerSec <= 0 {
		p.targetRateBytesPerSec = defaultRate
	}
	if p.targetRateBytesPerSec > hardCap {
		p.targetRateBytesPerSec = hardCap
	}
	if !cfg.EnableAdaptiveEstimator {
		p.feedbackScale = 1
		p.minRateNeededBytesPerSec = hardCap
		p.followRateBytesPerSec = hardCap
		p.targetRateBytesPerSec = hardCap
		p.adjustedRateBytesPerSec = hardCap
		p.queueHighSince = time.Time{}
	}
	needRestart := wasRunning && oldTick != cfg.Tick
	p.mu.Unlock()

	if !cfg.Enabled {
		p.stopLoop(true)
		return
	}
	if !wasEnabled || !wasRunning {
		p.Start()
		return
	}
	if needRestart {
		p.stopLoop(false)
		p.Start()
	}
}

func (p *Pacer) Config() PacerConfig {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg
}

func (p *Pacer) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

func (p *Pacer) SetOnSent(cb func(job ForwardJob, sentBytes int)) {
	p.mu.Lock()
	p.onSent = cb
	p.mu.Unlock()
}

func (p *Pacer) SetMeta(instanceKey string, direction string) {
	p.mu.Lock()
	if instanceKey != "" {
		p.instanceKey = instanceKey
	}
	if direction != "" {
		p.direction = direction
	}
	p.mu.Unlock()
}

func (p *Pacer) hardCapBytesPerSecLocked() float64 {
	capRate := cfgHardCapBytesPerSec(p.cfg)
	if capRate < 1 {
		return 1
	}
	return capRate
}

func (p *Pacer) floorRateBytesPerSecLocked() float64 {
	floor := cfgFloorBytesPerSec(p.cfg)
	hardCap := p.hardCapBytesPerSecLocked()
	if floor > hardCap {
		return hardCap
	}
	return floor
}

func (p *Pacer) clampRateLocked(rate float64) float64 {
	hardCap := p.hardCapBytesPerSecLocked()
	if hardCap < 1 {
		hardCap = 1
	}
	if rate < 1 {
		rate = 1
	}
	if rate > hardCap {
		rate = hardCap
	}
	return rate
}

// Enqueue adds a packet to pacer queue.
// Returns true when packet is accepted (or directly sent in passthrough mode).
func (p *Pacer) Enqueue(job ForwardJob, prio PacerPriority) bool {
	ok, _ := p.EnqueueWithReason(job, prio)
	return ok
}

// EnqueueWithReason adds a packet to pacer queue and returns rejection reason when it fails.
func (p *Pacer) EnqueueWithReason(job ForwardJob, prio PacerPriority) (bool, string) {
	if job.target == nil || len(job.pkt) == 0 {
		return false, "invalid_job"
	}

	p.mu.Lock()
	cfg := p.cfg
	running := p.running
	p.mu.Unlock()

	if !cfg.Enabled || !running {
		if p.sendDirect(job) {
			return true, ""
		}
		return false, "udp_write_error"
	}

	now := time.Now()
	copyJob := cloneForwardJob(job)
	pkt := pacerPacket{job: copyJob, enqueueAt: now}
	pktLen := len(copyJob.pkt)
	if pktLen == 0 {
		return false, "invalid_job"
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	flowKey := makePacerFlowKey(copyJob)
	flow := p.flows[flowKey]
	if p.cfg.MaxQueuePackets > 0 && p.queuePackets >= p.cfg.MaxQueuePackets {
		p.markDropLocked(pktLen)
		return false, "queue_packets_full"
	}
	if p.cfg.MaxQueueBytes > 0 && (p.queueBytes+pktLen) > p.cfg.MaxQueueBytes {
		p.markDropLocked(pktLen)
		return false, "queue_bytes_full"
	}
	if flow == nil {
		if len(p.flows) >= p.cfg.MaxFlows {
			p.markDropLocked(pktLen)
			return false, "max_flows"
		}
		flow = &pacerFlow{key: flowKey}
		p.flows[flowKey] = flow
		p.flowOrder = append(p.flowOrder, flowKey)
	}

	p.enqueueOrder++
	pkt.enqueueOrder = p.enqueueOrder
	flow.decoratePacketForQueue(&pkt)
	flow.push(pkt, prio)
	p.queuePackets++
	p.queueBytes += pktLen
	p.stats.EnqueuedPackets++
	p.stats.EnqueuedBytes += uint64(pktLen)
	if p.cfg.EnableAdaptiveEstimator {
		p.observeIngressLocked(now, pktLen)
	}
	return true, ""
}

func (p *Pacer) Stats() PacerStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	s := p.stats
	s.QueueFlows = len(p.flows)
	s.QueuePackets = p.queuePackets
	s.QueueBytes = p.queueBytes
	s.CreditsBytes = p.credits
	return s
}

func (p *Pacer) sendDirect(job ForwardJob) bool {
	if p.writer == nil {
		return false
	}

	n, err := p.writer.WriteToUDP(job.pkt, job.target)
	var onSent func(job ForwardJob, sentBytes int)
	p.mu.Lock()
	if err != nil {
		p.stats.SendErrors++
		p.mu.Unlock()
		return false
	}
	if n < 0 {
		n = 0
	}
	p.stats.SentPackets++
	p.stats.SentBytes += uint64(n)
	onSent = p.onSent
	p.mu.Unlock()
	if onSent != nil {
		onSent(job, n)
	}
	return true
}

func (p *Pacer) startLoopLocked(now time.Time) {
	if p.running || !p.cfg.Enabled {
		return
	}
	if p.feedbackScale <= 0 || p.feedbackScale > 1 {
		p.feedbackScale = 1
	}
	startRate := cfgStartRateBytesPerSec(p.cfg)
	if p.adjustedRateBytesPerSec <= 0 {
		p.adjustedRateBytesPerSec = startRate
	}
	p.adjustedRateBytesPerSec = p.clampRateLocked(p.adjustedRateBytesPerSec)
	if p.targetRateBytesPerSec <= 0 {
		p.targetRateBytesPerSec = p.adjustedRateBytesPerSec
	}
	if p.followRateBytesPerSec < 0 {
		p.followRateBytesPerSec = 0
	}
	if p.minRateNeededBytesPerSec < 0 {
		p.minRateNeededBytesPerSec = 0
	}
	p.running = true
	p.stopCh = make(chan struct{})
	p.doneCh = make(chan struct{})
	p.lastRefill = now
	if p.lastTargetUpdateAt.IsZero() {
		p.lastTargetUpdateAt = now
	}
	if p.credits > float64(p.cfg.BurstBytes) {
		p.credits = float64(p.cfg.BurstBytes)
	}

	tick := p.cfg.Tick
	if tick <= 0 {
		tick = 10 * time.Millisecond
	}
	go p.loop(tick, p.stopCh, p.doneCh)
}

func (p *Pacer) stopLoop(clearQueue bool) {
	var stopCh chan struct{}
	var doneCh chan struct{}

	p.mu.Lock()
	if p.running {
		stopCh = p.stopCh
		doneCh = p.doneCh
		p.running = false
		p.stopCh = nil
		p.doneCh = nil
	}
	if clearQueue {
		p.clearQueueLocked()
		p.credits = 0
	}
	p.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if doneCh != nil {
		<-doneCh
	}
}

func (p *Pacer) clearQueueLocked() {
	if p.queuePackets > 0 || p.queueBytes > 0 {
		p.stats.DroppedPackets += uint64(p.queuePackets)
		p.stats.DroppedBytes += uint64(p.queueBytes)
	}
	p.flows = make(map[string]*pacerFlow)
	p.flowOrder = nil
	p.nextFlowIdx = 0
	p.queuePackets = 0
	p.queueBytes = 0
	p.avgQueueDelaySec = 0
	p.queueHighSince = time.Time{}
	p.lastTargetUpdateAt = time.Time{}
	p.estDebugWindowStart = time.Time{}
	if len(p.estDebugSamples) > 0 {
		p.estDebugSamples = p.estDebugSamples[:0]
	}
	p.ingressSampleAt = time.Time{}
	p.ingressSampleBytes = 0
	resetRate := cfgStartRateBytesPerSec(p.cfg)
	if !p.cfg.EnableAdaptiveEstimator {
		p.feedbackScale = 1
	}
	p.minRateNeededBytesPerSec = resetRate
	p.followRateBytesPerSec = resetRate
	p.targetRateBytesPerSec = resetRate
	p.adjustedRateBytesPerSec = resetRate
}

func (p *Pacer) loop(tick time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			p.flushOnce(now)
		case <-stop:
			return
		}
	}
}

func (p *Pacer) flushOnce(now time.Time) {
	batch := p.dequeueBatch(now)
	if len(batch) == 0 {
		p.maybeLog(now, 0, 0, 0, 0)
		return
	}

	var sentPackets int
	var sentBytes int
	var sendErrs int
	p.mu.Lock()
	onSent := p.onSent
	p.mu.Unlock()

	for _, s := range batch {
		n, err := p.writer.WriteToUDP(s.pkt.job.pkt, s.pkt.job.target)
		if err != nil {
			sendErrs++
			continue
		}
		if n > 0 {
			sentPackets++
			sentBytes += n
			if onSent != nil {
				onSent(s.pkt.job, n)
			}
		}
	}

	p.mu.Lock()
	if sentPackets > 0 {
		p.stats.SentPackets += uint64(sentPackets)
		p.stats.SentBytes += uint64(sentBytes)
	}
	if sendErrs > 0 {
		p.stats.SendErrors += uint64(sendErrs)
	}
	queuePackets := p.queuePackets
	queueBytes := p.queueBytes
	credits := p.credits
	p.mu.Unlock()

	p.maybeLog(now, sentPackets, sentBytes, queuePackets, queueBytes)
	_ = credits
}

func (p *Pacer) dequeueBatch(now time.Time) []pacedSend {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running || !p.cfg.Enabled {
		return nil
	}
	p.updateAdjustedRateLocked(now)
	p.refillCreditLocked(now)
	if p.queuePackets <= 0 || p.credits <= 0 || len(p.flowOrder) == 0 {
		return nil
	}

	budgetBytes := p.effectiveFlushBudgetBytesLocked()
	batch := make([]pacedSend, 0, 32)
	usedBytes := 0

	stalled := 0
	for usedBytes < budgetBytes && len(p.flowOrder) > 0 {
		if stalled >= len(p.flowOrder) {
			break
		}
		if p.nextFlowIdx >= len(p.flowOrder) {
			p.nextFlowIdx = 0
		}

		idx := p.nextFlowIdx
		flowKey := p.flowOrder[idx]
		flow := p.flows[flowKey]
		if flow == nil || flow.queuePackets() == 0 {
			p.removeFlowAtLocked(idx)
			stalled = 0
			continue
		}

		peekPkt, _, ok := flow.peek()
		if !ok {
			p.removeFlowAtLocked(idx)
			stalled = 0
			continue
		}
		pktLen := len(peekPkt.job.pkt)
		if pktLen <= 0 {
			_, _, _ = flow.pop()
			p.queuePackets--
			p.markDropLocked(0)
			if flow.queuePackets() == 0 {
				p.removeFlowAtLocked(idx)
			} else {
				p.nextFlowIdx = idx + 1
			}
			stalled = 0
			continue
		}

		if float64(pktLen) > p.credits {
			p.nextFlowIdx = idx + 1
			stalled++
			continue
		}
		if usedBytes+pktLen > budgetBytes && len(batch) > 0 {
			p.nextFlowIdx = idx + 1
			stalled++
			continue
		}

		pkt, prio, ok := flow.pop()
		if !ok {
			p.removeFlowAtLocked(idx)
			stalled = 0
			continue
		}

		p.queuePackets--
		p.queueBytes -= pktLen
		if p.queueBytes < 0 {
			p.queueBytes = 0
		}
		p.credits -= float64(pktLen)
		if p.credits < 0 {
			p.credits = 0
		}
		queueDur := now.Sub(pkt.enqueueAt)
		p.observeQueueDelayLocked(queueDur)
		batch = append(batch, pacedSend{
			pkt:      pkt,
			prio:     prio,
			queueDur: queueDur,
		})
		usedBytes += pktLen

		if flow.queuePackets() == 0 {
			p.removeFlowAtLocked(idx)
		} else {
			p.nextFlowIdx = idx + 1
		}
		stalled = 0
	}
	return batch
}

func (p *Pacer) refillCreditLocked(now time.Time) {
	if p.lastRefill.IsZero() {
		p.lastRefill = now
		return
	}
	elapsed := now.Sub(p.lastRefill)
	if elapsed <= 0 {
		return
	}
	p.lastRefill = now

	rateBytesPerSec := p.adjustedRateBytesPerSec
	if rateBytesPerSec < 1 {
		rateBytesPerSec = 1
	}
	p.credits += elapsed.Seconds() * rateBytesPerSec

	maxCredits := float64(p.cfg.BurstBytes)
	tickSec := p.cfg.Tick.Seconds()
	if tickSec <= 0 {
		tickSec = 0.01
	}
	// Keep at least a few ticks of credit so temporary boosted rate can
	// actually drain backlog instead of being clipped by a too-small burst.
	minBurstForAdjusted := rateBytesPerSec * tickSec * 4
	if minBurstForAdjusted > maxCredits {
		maxCredits = minBurstForAdjusted
	}
	if p.credits > maxCredits {
		p.credits = maxCredits
	}
}

func (p *Pacer) baseRateBytesPerSecLocked() float64 {
	return p.hardCapBytesPerSecLocked()
}

func (p *Pacer) oldestQueueAgeSecLocked(now time.Time) float64 {
	var oldest time.Time
	for _, key := range p.flowOrder {
		flow := p.flows[key]
		if flow == nil {
			continue
		}
		candidate := flowOldestEnqueueAt(flow)
		if candidate.IsZero() {
			continue
		}
		if oldest.IsZero() || candidate.Before(oldest) {
			oldest = candidate
		}
	}
	if oldest.IsZero() {
		return 0
	}
	age := now.Sub(oldest).Seconds()
	if age < 0 {
		return 0
	}
	return age
}

func flowOldestEnqueueAt(flow *pacerFlow) time.Time {
	var oldest time.Time
	if high, ok := flow.highQ.Peek(); ok {
		oldest = high.enqueueAt
	}
	if normal, ok := flow.normalQ.Peek(); ok {
		if oldest.IsZero() || normal.enqueueAt.Before(oldest) {
			oldest = normal.enqueueAt
		}
	}
	return oldest
}

func (p *Pacer) observeQueueDelayLocked(queueDur time.Duration) {
	sample := queueDur.Seconds()
	if sample < 0 {
		sample = 0
	}
	const alpha = 0.1
	if p.avgQueueDelaySec <= 0 {
		p.avgQueueDelaySec = sample
		return
	}
	p.avgQueueDelaySec = p.avgQueueDelaySec*(1-alpha) + sample*alpha
}

func (p *Pacer) effectiveFlushBudgetBytesLocked() int {
	if p.cfg.FlushBudgetBytes > 0 {
		if p.cfg.FlushBudgetBytes < 1500 {
			return 1500
		}
		return p.cfg.FlushBudgetBytes
	}
	tickSec := p.cfg.Tick.Seconds()
	if tickSec <= 0 {
		tickSec = 0.01
	}
	// Allow a few ticks worth to absorb packet-size granularity while
	// keeping bursts bounded to the current shaped target rate.
	budget := int(p.adjustedRateBytesPerSec * tickSec * 4)
	if budget < 1500 {
		budget = 1500
	}
	return budget
}

func (p *Pacer) removeFlowAtLocked(idx int) {
	if idx < 0 || idx >= len(p.flowOrder) {
		return
	}
	key := p.flowOrder[idx]
	delete(p.flows, key)

	copy(p.flowOrder[idx:], p.flowOrder[idx+1:])
	p.flowOrder = p.flowOrder[:len(p.flowOrder)-1]
	if len(p.flowOrder) == 0 {
		p.nextFlowIdx = 0
		return
	}
	if idx < p.nextFlowIdx {
		p.nextFlowIdx--
	}
	if p.nextFlowIdx >= len(p.flowOrder) {
		p.nextFlowIdx = 0
	}
}

func (p *Pacer) markDropLocked(bytes int) {
	p.stats.DroppedPackets++
	if bytes > 0 {
		p.stats.DroppedBytes += uint64(bytes)
	}
}

func (p *Pacer) maybeLog(now time.Time, sentPackets int, sentBytes int, queuePackets int, queueBytes int) {
	p.mu.Lock()
	p.rate500ms.add(now, sentBytes)
	p.rate1s.add(now, sentBytes)
	bytes500ms := p.rate500ms.snapshot(now)
	bytes1s := p.rate1s.snapshot(now)
	kbps500ms := bytesSeriesToKbps(bytes500ms, 500*time.Millisecond)
	kbps1s := bytesSeriesToKbps(bytes1s, time.Second)
	avg500ms := avgFloat64(kbps500ms)
	avg1s := avgFloat64(kbps1s)
	avg5s := bytesToKbps(sumBytes(bytes1s), 5*time.Second)

	enabled := p.cfg.LogEnabled
	pacerEnabled := p.cfg.Enabled
	running := p.running
	period := p.cfg.LogPeriod
	if !enabled || !pacerEnabled || !running {
		p.mu.Unlock()
		return
	}
	if period > 0 {
		if !p.lastLogAt.IsZero() && now.Sub(p.lastLogAt) < period {
			p.mu.Unlock()
			return
		}
		p.lastLogAt = now
	}
	flows := len(p.flows)
	credits := p.credits
	errors := p.stats.SendErrors
	dropped := p.stats.DroppedPackets
	baseRateBps := int64(p.baseRateBytesPerSecLocked() * 8)
	adjustedRateBps := int64(p.adjustedRateBytesPerSec * 8)
	minRateNeededBps := int64(p.minRateNeededBytesPerSec * 8)
	followRateBps := int64(p.followRateBytesPerSec * 8)
	targetRateBps := int64(p.targetRateBytesPerSec * 8)
	inShortBps := int64(p.ingressShortEWMA.current() * 8)
	inLongBps := int64(p.ingressLongEWMA.current() * 8)
	baseRate := formatBitrateFromBps(baseRateBps)
	adjustedRate := formatBitrateFromBps(adjustedRateBps)
	minRateNeeded := formatBitrateFromBps(minRateNeededBps)
	inShortRate := formatBitrateFromBps(inShortBps)
	inLongRate := formatBitrateFromBps(inLongBps)
	followRate := formatBitrateFromBps(followRateBps)
	targetRate := formatBitrateFromBps(targetRateBps)
	feedbackScale := p.feedbackScale
	avgQueueMs := int64(p.avgQueueDelaySec * 1000)
	instanceKey := p.instanceKey
	direction := p.direction
	p.mu.Unlock()

	log.Printf("[pacer] key=%s dir=%s sent=%d bytes=%d queue=%d/%d flows=%d credit=%.0f rate(base/adj/min)=%s/%s/%s in(short/long)=%s/%s follow/target=%s/%s fb=%.2f qwait_avg=%dms rate_500ms_kbps=%v rate_500ms_avg_kbps=%.1f rate_1s_kbps=%v rate_1s_avg_kbps=%.1f rate_5s_avg_kbps=%.1f err=%d drop=%d",
		instanceKey, direction,
		sentPackets, sentBytes, queuePackets, queueBytes, flows, credits,
		baseRate, adjustedRate, minRateNeeded,
		inShortRate, inLongRate, followRate, targetRate, feedbackScale, avgQueueMs,
		kbps500ms, avg500ms,
		kbps1s, avg1s, avg5s,
		errors, dropped,
	)
}

func makePacerFlowKey(job ForwardJob) string {
	target := "nil"
	if job.target != nil {
		target = job.target.String()
	}
	return target + "|" + itoaU32(job.ssrc)
}

func cloneForwardJob(job ForwardJob) ForwardJob {
	var targetCopy *net.UDPAddr
	if job.target != nil {
		t := *job.target
		targetCopy = &t
	}
	pktCopy := append([]byte(nil), job.pkt...)
	return ForwardJob{
		ssrc:    job.ssrc,
		srcAddr: job.srcAddr,
		target:  targetCopy,
		pkt:     pktCopy,
		prio:    job.prio,
		counted: job.counted,
	}
}

func itoaU32(v uint32) string {
	if v == 0 {
		return "0"
	}
	var b [10]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func bytesSeriesToKbps(series []uint64, bucketDur time.Duration) []float64 {
	out := make([]float64, len(series))
	for i, b := range series {
		out[i] = bytesToKbps(b, bucketDur)
	}
	return out
}

func bytesToKbps(bytes uint64, dur time.Duration) float64 {
	secs := dur.Seconds()
	if secs <= 0 {
		return 0
	}
	return float64(bytes) * 8.0 / secs / 1000.0
}

func sumBytes(series []uint64) uint64 {
	var total uint64
	for _, b := range series {
		total += b
	}
	return total
}

func avgFloat64(series []float64) float64 {
	if len(series) == 0 {
		return 0
	}
	var total float64
	for _, v := range series {
		total += v
	}
	return total / float64(len(series))
}
