package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	jitterFastResyncGapPackets    = 64
	jitterFastResyncBufferPackets = 512
)

type jitterPacketBucket struct {
	slot    int64
	packets []jitterBucketPacket
}

type jitterBucketPacket struct {
	seq uint16
	raw []byte
}

type JitterBuffer struct {
	mu sync.Mutex

	buffer map[uint16][]byte

	pendingBuckets  []jitterPacketBucket
	bucketedPackets int
	lastSlot        int64
	lastSlotSet     bool

	expectedSeq uint16
	expectedSet bool
	gapSince    time.Time

	maxSeenSeq uint16
	maxSeenSet bool

	enabled      bool
	minDelay     time.Duration
	maxDelay     time.Duration
	currentDelay time.Duration
	tick         time.Duration
	upStep       time.Duration
	downStep     time.Duration
	ticker       *time.Ticker
	out          chan []byte
	stop         chan struct{}

	debugTag    string
	lastLogAt   time.Time
	lastBoostAt time.Time
	lastDecayAt time.Time
	logEnabled  bool
	logPeriod   time.Duration

	inPackets    uint64
	flushRounds  uint64
	outPackets   uint64
	dropPackets  uint64
	timeoutSkips uint64
	reorderCount uint64
}

func (jb *JitterBuffer) SafeAdd(seq uint16, pkt []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("[WARN] jitter buffer panic, disabling jitter:", r)
			jb.enabled = false
		}
	}()
	jb.Add(seq, pkt)
}

func NewJitterBuffer(
	enabled bool,
	minDelayMs int,
	maxDelayMs int,
	tickMs int,
	upStepMs int,
	downStepMs int,
	logEnabled bool,
	logPeriodMs int,
	debugTag string,
) *JitterBuffer {
	if minDelayMs <= 0 {
		minDelayMs = 20
	}
	if maxDelayMs <= 0 {
		maxDelayMs = 200
	}
	if maxDelayMs < minDelayMs {
		maxDelayMs = minDelayMs
	}
	if tickMs <= 0 {
		tickMs = 10
	}
	if upStepMs <= 0 {
		upStepMs = 20
	}
	if downStepMs <= 0 {
		downStepMs = 5
	}
	if logPeriodMs < 0 {
		logPeriodMs = 0
	}

	jb := &JitterBuffer{
		buffer:       make(map[uint16][]byte),
		enabled:      enabled,
		minDelay:     time.Duration(minDelayMs) * time.Millisecond,
		maxDelay:     time.Duration(maxDelayMs) * time.Millisecond,
		currentDelay: time.Duration(minDelayMs) * time.Millisecond,
		tick:         time.Duration(tickMs) * time.Millisecond,
		upStep:       time.Duration(upStepMs) * time.Millisecond,
		downStep:     time.Duration(downStepMs) * time.Millisecond,
		out:          make(chan []byte, 1024),
		stop:         make(chan struct{}),
		debugTag:     debugTag,
		logEnabled:   logEnabled,
		logPeriod:    time.Duration(logPeriodMs) * time.Millisecond,
	}
	if jb.enabled {
		jb.ticker = time.NewTicker(jb.tick)
		go jb.flushLoop()
		jb.logf(false,
			"[jitter][%s] enabled min=%s max=%s tick=%s up=%s down=%s",
			jb.tag(), jb.minDelay, jb.maxDelay, jb.tick, jb.upStep, jb.downStep,
		)
	} else {
		jb.logf(false, "[jitter][%s] disabled (passthrough)", jb.tag())
	}
	return jb
}

func (jb *JitterBuffer) Add(seq uint16, pkt []byte) {
	if !jb.enabled {
		select {
		case jb.out <- pkt:
			jb.mu.Lock()
			jb.outPackets++
			jb.mu.Unlock()
		default:
			jb.mu.Lock()
			jb.dropPackets++
			jb.mu.Unlock()
		}
		return
	}
	jb.mu.Lock()
	jb.buffer[seq] = append([]byte(nil), pkt...)
	jb.inPackets++

	now := time.Now()
	if !jb.expectedSet {
		jb.expectedSeq = seq
		jb.expectedSet = true
	}
	if !jb.maxSeenSet {
		jb.maxSeenSeq = seq
		jb.maxSeenSet = true
	} else if seqAhead(seq, jb.maxSeenSeq) {
		jb.maxSeenSeq = seq
	} else if seqBehind(seq, jb.maxSeenSeq) {
		jb.reorderCount++
		jb.boostDelayLocked(now)
	}

	buffered := len(jb.buffer)
	buffered += jb.bucketedPackets
	inPackets := jb.inPackets
	reorderCount := jb.reorderCount
	delay := jb.currentDelay
	shouldLog := jb.shouldLogLocked(now)
	jb.mu.Unlock()

	if shouldLog {
		jb.logf(false,
			"[jitter][%s] buffer in_total=%d buffered=%d delay=%s reorder=%d",
			jb.tag(), inPackets, buffered, delay, reorderCount,
		)
	}
}

func (jb *JitterBuffer) flushLoop() {
	for {
		select {
		case <-jb.ticker.C:
			jb.flushOnce(time.Now())
		case <-jb.stop:
			if jb.ticker != nil {
				jb.ticker.Stop()
			}
			return
		}
	}
}

func (jb *JitterBuffer) flushOnce(now time.Time) {
	jb.mu.Lock()
	currentSlot := jb.slotForTimeLocked(now)
	jb.ensureTimelineToSlotLocked(currentSlot)

	latePackets := make([]jitterBucketPacket, 0, 8)
	skipsThisRound := 0
	packets := make([]jitterBucketPacket, 0, 64)
	seqStart := uint16(0)
	seqEnd := uint16(0)
	hasSeq := false

	if len(jb.buffer) > 0 && jb.expectedSet {
		latePackets = jb.popLatePacketsLocked()

		for i := 0; i < 256; i++ {
			if pkt, ok := jb.buffer[jb.expectedSeq]; ok {
				if !hasSeq {
					seqStart = jb.expectedSeq
					hasSeq = true
				}
				seqEnd = jb.expectedSeq
				packets = append(packets, jitterBucketPacket{
					seq: jb.expectedSeq,
					raw: pkt,
				})
				delete(jb.buffer, jb.expectedSeq)
				jb.expectedSeq++
				jb.gapSince = time.Time{}
				continue
			}

			if len(jb.buffer) == 0 {
				break
			}

			if !jb.hasAheadPacketLocked(jb.expectedSeq) {
				break
			}

			if nextSeq, gap, ok := jb.closestAheadSeqLocked(jb.expectedSeq); ok &&
				(gap >= jitterFastResyncGapPackets || len(jb.buffer) >= jitterFastResyncBufferPackets) {
				jb.timeoutSkips += uint64(gap)
				skipsThisRound += gap
				jb.expectedSeq = nextSeq
				jb.gapSince = time.Time{}
				jb.boostDelayLocked(now)
				continue
			}

			if jb.gapSince.IsZero() {
				jb.gapSince = now
				jb.boostDelayLocked(now)
				break
			}

			if now.Sub(jb.gapSince) < jb.currentDelay {
				break
			}

			if nextSeq, gap, ok := jb.closestAheadSeqLocked(jb.expectedSeq); ok {
				jb.timeoutSkips += uint64(gap)
				skipsThisRound += gap
				jb.expectedSeq = nextSeq
				jb.boostDelayLocked(now)
				continue
			}

			jb.timeoutSkips++
			skipsThisRound++
			jb.expectedSeq++
			jb.gapSince = now
			jb.boostDelayLocked(now)
		}

		if len(packets) > 0 {
			jb.enqueuePacketsLocked(currentSlot, packets)
			jb.decayDelayLocked(now)
		}
	}

	emitPackets := jb.popReadyBucketLocked(currentSlot)

	jb.flushRounds++
	flushRound := jb.flushRounds
	shouldLog := jb.shouldLogLocked(now)
	delay := jb.currentDelay
	buffered := len(jb.buffer) + jb.bucketedPackets
	readyBuckets := len(jb.pendingBuckets)
	reorderCount := jb.reorderCount
	timeoutSkips := jb.timeoutSkips
	jb.mu.Unlock()

	lateForwarded := 0
	lateDropped := 0
	for _, pkt := range latePackets {
		select {
		case jb.out <- pkt.raw:
			lateForwarded++
		default:
			lateDropped++
		}
	}

	emitSent := 0
	emitDropped := 0
	for _, pkt := range emitPackets {
		select {
		case jb.out <- pkt.raw:
			emitSent++
		default:
			emitDropped++
		}
	}
	sent := lateForwarded + emitSent
	dropped := lateDropped + emitDropped

	jb.mu.Lock()
	jb.outPackets += uint64(sent)
	jb.dropPackets += uint64(dropped)
	outPackets := jb.outPackets
	dropPackets := jb.dropPackets
	jb.mu.Unlock()

	anomaly := dropped > 0 || skipsThisRound > 0
	if shouldLog || anomaly {
		if hasSeq {
			jb.logf(anomaly,
				"[jitter][%s] flush round=%d packets=%d seq=%d-%d emit=%d sent=%d dropped=%d late_forward=%d late_drop=%d buffered=%d buckets=%d delay=%s reorder=%d gap_skips=%d(+%d) out_total=%d drop_total=%d",
				jb.tag(),
				flushRound,
				len(packets),
				seqStart,
				seqEnd,
				len(emitPackets),
				sent,
				dropped,
				lateForwarded,
				lateDropped,
				buffered,
				readyBuckets,
				delay,
				reorderCount,
				timeoutSkips,
				skipsThisRound,
				outPackets,
				dropPackets,
			)
		} else {
			jb.logf(anomaly,
				"[jitter][%s] flush round=%d packets=0 emit=%d skipped=%d late_forward=%d late_drop=%d buffered=%d buckets=%d delay=%s reorder=%d gap_skips=%d out_total=%d drop_total=%d",
				jb.tag(),
				flushRound,
				len(emitPackets),
				skipsThisRound,
				lateForwarded,
				lateDropped,
				buffered,
				readyBuckets,
				delay,
				reorderCount,
				timeoutSkips,
				outPackets,
				dropPackets,
			)
		}
	}
}

func (jb *JitterBuffer) Close() {
	select {
	case <-jb.stop:
		return
	default:
		close(jb.stop)
	}
}

func (jb *JitterBuffer) Out() <-chan []byte {
	return jb.out
}

func (jb *JitterBuffer) UpdateParams(minDelayMs int, maxDelayMs int, tickMs int, upStepMs int, downStepMs int, logEnabled bool, logPeriodMs int) {
	if minDelayMs <= 0 {
		minDelayMs = 20
	}
	if maxDelayMs <= 0 {
		maxDelayMs = 200
	}
	if maxDelayMs < minDelayMs {
		maxDelayMs = minDelayMs
	}
	if tickMs <= 0 {
		tickMs = 10
	}
	if upStepMs <= 0 {
		upStepMs = 20
	}
	if downStepMs <= 0 {
		downStepMs = 5
	}
	if logPeriodMs < 0 {
		logPeriodMs = 0
	}

	jb.mu.Lock()
	jb.minDelay = time.Duration(minDelayMs) * time.Millisecond
	jb.maxDelay = time.Duration(maxDelayMs) * time.Millisecond
	jb.upStep = time.Duration(upStepMs) * time.Millisecond
	jb.downStep = time.Duration(downStepMs) * time.Millisecond

	newTick := time.Duration(tickMs) * time.Millisecond
	if jb.ticker != nil && newTick != jb.tick {
		jb.ticker.Reset(newTick)
	}
	jb.tick = newTick
	jb.logEnabled = logEnabled
	jb.logPeriod = time.Duration(logPeriodMs) * time.Millisecond

	if jb.currentDelay < jb.minDelay {
		jb.currentDelay = jb.minDelay
	}
	if jb.currentDelay > jb.maxDelay {
		jb.currentDelay = jb.maxDelay
	}

	minDelay := jb.minDelay
	maxDelay := jb.maxDelay
	tick := jb.tick
	up := jb.upStep
	down := jb.downStep
	current := jb.currentDelay
	lp := jb.logPeriod
	le := jb.logEnabled
	jb.mu.Unlock()

	jb.logf(false, "[jitter][%s] params updated min=%s max=%s tick=%s up=%s down=%s current=%s log_enabled=%t log_period=%s", jb.tag(), minDelay, maxDelay, tick, up, down, current, le, lp)
}

func (jb *JitterBuffer) SetEnabled(enabled bool) {
	jb.mu.Lock()
	wasEnabled := jb.enabled
	jb.enabled = enabled

	if enabled {
		if jb.ticker == nil {
			jb.ticker = time.NewTicker(jb.tick)
			go jb.flushLoop()
		}
	} else if wasEnabled {
		dropped := len(jb.buffer)
		dropped += jb.bucketedPackets
		if dropped > 0 {
			jb.dropPackets += uint64(dropped)
		}
		jb.buffer = make(map[uint16][]byte)
		jb.pendingBuckets = nil
		jb.bucketedPackets = 0
		jb.lastSlotSet = false
		jb.lastSlot = 0
		jb.expectedSet = false
		jb.maxSeenSet = false
		jb.gapSince = time.Time{}
	}

	tag := jb.tag()
	minDelay := jb.minDelay
	maxDelay := jb.maxDelay
	tick := jb.tick
	up := jb.upStep
	down := jb.downStep
	jb.mu.Unlock()

	if enabled && !wasEnabled {
		jb.logf(false, "[jitter][%s] enabled min=%s max=%s tick=%s up=%s down=%s", tag, minDelay, maxDelay, tick, up, down)
		return
	}
	if !enabled && wasEnabled {
		jb.logf(false, "[jitter][%s] disabled (passthrough)", tag)
	}
}

func (jb *JitterBuffer) shouldLogLocked(now time.Time) bool {
	if !jb.logEnabled {
		return false
	}
	if jb.logPeriod == 0 {
		return true
	}
	if jb.lastLogAt.IsZero() || now.Sub(jb.lastLogAt) >= jb.logPeriod {
		jb.lastLogAt = now
		return true
	}
	return false
}

func (jb *JitterBuffer) tag() string {
	if jb.debugTag != "" {
		return jb.debugTag
	}
	return "unknown-ssrc"
}

func (jb *JitterBuffer) boostDelayLocked(now time.Time) {
	if !jb.enabled || jb.currentDelay >= jb.maxDelay {
		return
	}
	if !jb.lastBoostAt.IsZero() && now.Sub(jb.lastBoostAt) < 50*time.Millisecond {
		return
	}
	next := jb.currentDelay + jb.upStep
	if next > jb.maxDelay {
		next = jb.maxDelay
	}
	if next != jb.currentDelay {
		jb.currentDelay = next
		jb.lastBoostAt = now
	}
}

func (jb *JitterBuffer) decayDelayLocked(now time.Time) {
	if !jb.enabled || jb.currentDelay <= jb.minDelay {
		return
	}
	if !jb.lastDecayAt.IsZero() && now.Sub(jb.lastDecayAt) < time.Second {
		return
	}
	next := jb.currentDelay - jb.downStep
	if next < jb.minDelay {
		next = jb.minDelay
	}
	if next != jb.currentDelay {
		jb.currentDelay = next
		jb.lastDecayAt = now
	}
}

func (jb *JitterBuffer) hasAheadPacketLocked(seq uint16) bool {
	for k := range jb.buffer {
		if seqAhead(k, seq) {
			return true
		}
	}
	return false
}

func (jb *JitterBuffer) closestAheadSeqLocked(seq uint16) (uint16, int, bool) {
	var best uint16
	bestGap := 0
	found := false
	for k := range jb.buffer {
		gap, ok := seqAheadDistance(k, seq)
		if !ok {
			continue
		}
		if !found || gap < bestGap {
			found = true
			best = k
			bestGap = gap
		}
	}
	return best, bestGap, found
}

func (jb *JitterBuffer) maxBucketCountLocked() int {
	if jb.tick <= 0 {
		return 1
	}
	n := int((jb.currentDelay + jb.tick - 1) / jb.tick)
	if n < 1 {
		n = 1
	}
	return n
}

func (jb *JitterBuffer) maxBucketCapLocked() int {
	if jb.tick <= 0 {
		return 1
	}
	n := int((jb.maxDelay + jb.tick - 1) / jb.tick)
	if n < 1 {
		n = 1
	}
	return n
}

func (jb *JitterBuffer) slotForTimeLocked(now time.Time) int64 {
	tick := jb.tick
	if tick <= 0 {
		tick = 10 * time.Millisecond
	}
	return now.UnixNano() / int64(tick)
}

func insertPacketBySeq(pkts []jitterBucketPacket, pkt jitterBucketPacket) ([]jitterBucketPacket, bool) {
	for i := range pkts {
		if pkts[i].seq == pkt.seq {
			pkts[i].raw = pkt.raw
			return pkts, false
		}
		if seqAhead(pkts[i].seq, pkt.seq) {
			pkts = append(pkts, jitterBucketPacket{})
			copy(pkts[i+1:], pkts[i:])
			pkts[i] = pkt
			return pkts, true
		}
	}
	pkts = append(pkts, pkt)
	return pkts, true
}

func (jb *JitterBuffer) ensureTimelineToSlotLocked(slot int64) {
	if !jb.lastSlotSet {
		jb.pendingBuckets = append(jb.pendingBuckets, jitterPacketBucket{slot: slot})
		jb.lastSlot = slot
		jb.lastSlotSet = true
		return
	}
	if slot <= jb.lastSlot {
		return
	}
	for s := jb.lastSlot + 1; s <= slot; s++ {
		jb.pendingBuckets = append(jb.pendingBuckets, jitterPacketBucket{slot: s})
	}
	jb.lastSlot = slot
}

func (jb *JitterBuffer) findSlotBucketLocked(slot int64) int {
	if len(jb.pendingBuckets) == 0 {
		jb.pendingBuckets = append(jb.pendingBuckets, jitterPacketBucket{slot: slot})
		return 0
	}
	for i := len(jb.pendingBuckets) - 1; i >= 0; i-- {
		if jb.pendingBuckets[i].slot == slot {
			return i
		}
		if jb.pendingBuckets[i].slot < slot {
			break
		}
	}
	jb.pendingBuckets = append(jb.pendingBuckets, jitterPacketBucket{slot: slot})
	return len(jb.pendingBuckets) - 1
}

func (jb *JitterBuffer) enqueuePacketsLocked(slot int64, packets []jitterBucketPacket) {
	if len(packets) == 0 {
		return
	}
	idx := jb.findSlotBucketLocked(slot)
	b := &jb.pendingBuckets[idx]
	for _, pkt := range packets {
		var inserted bool
		b.packets, inserted = insertPacketBySeq(b.packets, pkt)
		if inserted {
			jb.bucketedPackets++
		}
	}
}

func (jb *JitterBuffer) popReadyBucketLocked(currentSlot int64) []jitterBucketPacket {
	if len(jb.pendingBuckets) == 0 {
		return nil
	}
	head := jb.pendingBuckets[0]
	targetBuckets := jb.maxBucketCountLocked()
	capBuckets := jb.maxBucketCapLocked()
	age := currentSlot - head.slot
	if age < 0 {
		age = 0
	}
	if int(age) < targetBuckets && len(jb.pendingBuckets) <= capBuckets {
		return nil
	}
	jb.pendingBuckets = jb.pendingBuckets[1:]
	jb.bucketedPackets -= len(head.packets)
	if jb.bucketedPackets < 0 {
		jb.bucketedPackets = 0
	}
	return head.packets
}

func (jb *JitterBuffer) popLatePacketsLocked() []jitterBucketPacket {
	if !jb.expectedSet || len(jb.buffer) == 0 {
		return nil
	}
	late := make([]jitterBucketPacket, 0, 8)
	for seq, raw := range jb.buffer {
		if seqBehind(seq, jb.expectedSeq) {
			delete(jb.buffer, seq)
			var inserted bool
			late, inserted = insertPacketBySeq(late, jitterBucketPacket{
				seq: seq,
				raw: raw,
			})
			_ = inserted
		}
	}
	return late
}

func seqAhead(a uint16, b uint16) bool {
	return int16(a-b) > 0
}

func seqAheadDistance(a uint16, b uint16) (int, bool) {
	d := uint16(a - b)
	if d == 0 || d >= 0x8000 {
		return 0, false
	}
	return int(d), true
}

func seqBehind(a uint16, b uint16) bool {
	return int16(a-b) < 0
}

func (jb *JitterBuffer) logf(red bool, format string, args ...any) {
	jb.mu.Lock()
	enabled := jb.logEnabled
	jb.mu.Unlock()
	if !enabled {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if red {
		msg = "\033[31m" + msg + "\033[0m"
	}
	log.Println(msg)
}
