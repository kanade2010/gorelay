package main

import (
	"fmt"
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
	// Upper bound on live flows to avoid unbounded memory growth.
	MaxFlows int
	// Max bytes emitted in one flush round.
	FlushBudgetBytes int

	LogEnabled bool
	LogPeriod  time.Duration
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
	job       ForwardJob
	enqueueAt time.Time
}

type packetQueue struct {
	items []pacerPacket
	head  int
}

func (q *packetQueue) Len() int {
	return len(q.items) - q.head
}

func (q *packetQueue) Push(pkt pacerPacket) {
	q.items = append(q.items, pkt)
}

func (q *packetQueue) Peek() (pacerPacket, bool) {
	if q.Len() == 0 {
		return pacerPacket{}, false
	}
	return q.items[q.head], true
}

func (q *packetQueue) Pop() (pacerPacket, bool) {
	if q.Len() == 0 {
		return pacerPacket{}, false
	}
	pkt := q.items[q.head]
	q.head++
	q.maybeCompact()
	return pkt, true
}

func (q *packetQueue) maybeCompact() {
	if q.head <= 0 {
		return
	}
	if q.head < 128 && q.head*2 < len(q.items) {
		return
	}
	copy(q.items, q.items[q.head:])
	q.items = q.items[:len(q.items)-q.head]
	q.head = 0
}

type pacerFlow struct {
	key        string
	highQ      packetQueue
	normalQ    packetQueue
	queueBytes int
}

func (f *pacerFlow) queuePackets() int {
	return f.highQ.Len() + f.normalQ.Len()
}

func (f *pacerFlow) push(pkt pacerPacket, prio PacerPriority) {
	if prio == PacerPriorityHigh {
		f.highQ.Push(pkt)
	} else {
		f.normalQ.Push(pkt)
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
	if pkt, ok := f.highQ.Pop(); ok {
		f.queueBytes -= len(pkt.job.pkt)
		if f.queueBytes < 0 {
			f.queueBytes = 0
		}
		return pkt, PacerPriorityHigh, true
	}
	if pkt, ok := f.normalQ.Pop(); ok {
		f.queueBytes -= len(pkt.job.pkt)
		if f.queueBytes < 0 {
			f.queueBytes = 0
		}
		return pkt, PacerPriorityNormal, true
	}
	return pacerPacket{}, PacerPriorityNormal, false
}

func (f *pacerFlow) dropOldestNormal() (pacerPacket, bool) {
	pkt, ok := f.normalQ.Pop()
	if !ok {
		return pacerPacket{}, false
	}
	f.queueBytes -= len(pkt.job.pkt)
	if f.queueBytes < 0 {
		f.queueBytes = 0
	}
	return pkt, true
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
	if cfg.BurstBytes <= 0 {
		cfg.BurstBytes = cfg.RateBytesPerSec / 10
	}
	if cfg.BurstBytes < 1500 {
		cfg.BurstBytes = 1500
	}
	if cfg.PerFlowQueuePackets < 0 {
		cfg.PerFlowQueuePackets = 0
	}
	if cfg.MaxFlows <= 0 {
		cfg.MaxFlows = 4096
	}
	if cfg.FlushBudgetBytes <= 0 {
		cfg.FlushBudgetBytes = int(float64(cfg.RateBytesPerSec) * cfg.Tick.Seconds())
	}
	if cfg.FlushBudgetBytes < 1500 {
		cfg.FlushBudgetBytes = 1500
	}
	if cfg.LogPeriod < 0 {
		cfg.LogPeriod = 0
	}
	if cfg.LogEnabled && cfg.LogPeriod == 0 {
		cfg.LogPeriod = 2 * time.Second
	}
	return cfg
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

	credits    float64
	lastRefill time.Time

	// Runtime rate control: base comes from config, adjusted is temporary
	// boost used to drain queue when backlog grows.
	adjustedRateBytesPerSec  float64
	minRateNeededBytesPerSec float64
	avgQueueDelaySec         float64

	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	lastLogAt time.Time

	stats PacerStats

	rate500ms  rollingByteBuckets
	rate1s     rollingByteBuckets
	nextRateAt time.Time

	onSent func(job ForwardJob, sentBytes int)
}

func NewPacer(writer UDPPacketWriter, cfg PacerConfig) *Pacer {
	norm := normalizePacerConfig(cfg)
	p := &Pacer{
		writer:                   writer,
		cfg:                      norm,
		flows:                    make(map[string]*pacerFlow),
		rate500ms:                newRollingByteBuckets(500*time.Millisecond, 10),
		rate1s:                   newRollingByteBuckets(time.Second, 5),
		adjustedRateBytesPerSec:  float64(norm.RateBytesPerSec),
		minRateNeededBytesPerSec: float64(norm.RateBytesPerSec),
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
	baseRate := float64(cfg.RateBytesPerSec)
	if baseRate < 1 {
		baseRate = 1
	}
	if p.adjustedRateBytesPerSec < baseRate {
		p.adjustedRateBytesPerSec = baseRate
	}
	if p.minRateNeededBytesPerSec < baseRate {
		p.minRateNeededBytesPerSec = baseRate
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
	if flow == nil {
		if len(p.flows) >= p.cfg.MaxFlows {
			p.markDropLocked(pktLen)
			return false, "max_flows"
		}
		flow = &pacerFlow{key: flowKey}
		p.flows[flowKey] = flow
		p.flowOrder = append(p.flowOrder, flowKey)
	}

	flow.push(pkt, prio)
	p.queuePackets++
	p.queueBytes += pktLen
	p.stats.EnqueuedPackets++
	p.stats.EnqueuedBytes += uint64(pktLen)
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
	baseRate := float64(p.cfg.RateBytesPerSec)
	if baseRate < 1 {
		baseRate = 1
	}
	if p.adjustedRateBytesPerSec < baseRate {
		p.adjustedRateBytesPerSec = baseRate
	}
	if p.minRateNeededBytesPerSec < baseRate {
		p.minRateNeededBytesPerSec = baseRate
	}
	p.running = true
	p.stopCh = make(chan struct{})
	p.doneCh = make(chan struct{})
	p.lastRefill = now
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
	baseRate := float64(p.cfg.RateBytesPerSec)
	if baseRate < 1 {
		baseRate = 1
	}
	p.minRateNeededBytesPerSec = baseRate
	p.adjustedRateBytesPerSec = baseRate
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
		rateLog := p.buildRateLog(now, 0)
		if rateLog != "" {
			log.Println(rateLog)
		}
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
	rateLog := p.buildRateLogLocked(now, sentBytes)
	p.mu.Unlock()

	if rateLog != "" {
		log.Println(rateLog)
	}
	p.maybeLog(now, sentPackets, sentBytes, queuePackets, queueBytes)
	_ = credits
}

func (p *Pacer) buildRateLog(now time.Time, sentBytes int) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buildRateLogLocked(now, sentBytes)
}

func (p *Pacer) buildRateLogLocked(now time.Time, sentBytes int) string {
	if p.cfg.LogEnabled == false || p.cfg.Enabled == false || p.running == false {
		return ""
	}

	p.rate500ms.add(now, sentBytes)
	p.rate1s.add(now, sentBytes)

	if p.nextRateAt.IsZero() {
		p.nextRateAt = now.Add(5 * time.Second)
		return ""
	}
	if now.Before(p.nextRateAt) {
		return ""
	}
	for !now.Before(p.nextRateAt) {
		p.nextRateAt = p.nextRateAt.Add(5 * time.Second)
	}

	bytes500ms := p.rate500ms.snapshot(now)
	bytes1s := p.rate1s.snapshot(now)

	kbps500ms := bytesSeriesToKbps(bytes500ms, 500*time.Millisecond)
	kbps1s := bytesSeriesToKbps(bytes1s, time.Second)
	avg5s := bytesToKbps(sumBytes(bytes1s), 5*time.Second)

	return fmt.Sprintf("[pacer][rate] 近5s 码率/500ms(kbps)=%v 码率/1s(kbps)=%v 平均=%.1f",
		kbps500ms, kbps1s, avg5s)
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
	baseRate := float64(p.cfg.RateBytesPerSec)
	if baseRate < 1 {
		baseRate = 1
	}
	if rateBytesPerSec < baseRate {
		rateBytesPerSec = baseRate
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
	base := float64(p.cfg.RateBytesPerSec)
	if base < 1 {
		base = 1
	}
	return base
}

func (p *Pacer) updateAdjustedRateLocked(now time.Time) {
	base := p.baseRateBytesPerSecLocked()
	if p.queuePackets <= 0 || p.queueBytes <= 0 || len(p.flowOrder) == 0 {
		p.minRateNeededBytesPerSec = base
		p.adjustedRateBytesPerSec = base
		p.avgQueueDelaySec = 0
		return
	}

	avgQueueSec := p.avgQueueDelaySec
	oldestAgeSec := p.oldestQueueAgeSecLocked(now)
	if oldestAgeSec > avgQueueSec {
		avgQueueSec = oldestAgeSec
	}
	tickSec := p.cfg.Tick.Seconds()
	if tickSec <= 0 {
		tickSec = 0.01
	}
	if avgQueueSec < tickSec {
		avgQueueSec = tickSec
	}

	minRate := float64(p.queueBytes) / avgQueueSec
	if minRate < base {
		minRate = base
	}
	p.minRateNeededBytesPerSec = minRate

	adj := p.adjustedRateBytesPerSec
	if adj < base {
		adj = base
	}
	if minRate > adj {
		adj = minRate
	} else {
		// Downscale conservatively to avoid rate oscillation.
		adj = adj*0.9 + minRate*0.1
		if adj < minRate {
			adj = minRate
		}
	}
	if adj < base {
		adj = base
	}
	p.adjustedRateBytesPerSec = adj
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
	baseBudget := p.cfg.FlushBudgetBytes
	if baseBudget <= 0 {
		baseBudget = int(p.baseRateBytesPerSecLocked() * p.cfg.Tick.Seconds())
	}
	if baseBudget < 1500 {
		baseBudget = 1500
	}

	baseRate := p.baseRateBytesPerSecLocked()
	adjustedRate := p.adjustedRateBytesPerSec
	if adjustedRate <= baseRate {
		return baseBudget
	}

	scaled := int(float64(baseBudget) * (adjustedRate / baseRate))
	if scaled < baseBudget {
		scaled = baseBudget
	}
	return scaled
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
	avgQueueMs := int64(p.avgQueueDelaySec * 1000)
	p.mu.Unlock()

	log.Printf("[pacer] sent=%d bytes=%d queue=%d/%d flows=%d credit=%.0f rate(base/adj/min)=%d/%d/%d bps qwait_avg=%dms err=%d drop=%d",
		sentPackets, sentBytes, queuePackets, queueBytes, flows, credits,
		baseRateBps, adjustedRateBps, minRateNeededBps, avgQueueMs, errors, dropped,
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
