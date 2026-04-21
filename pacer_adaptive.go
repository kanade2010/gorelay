package main

import (
	"fmt"
	"log"
	"math"
	"time"
)

const pacerEstimatorDebugWindow = 5 * time.Second
const pacerEstimatorDebugMaxPoints = 5

type pacerEstimatorDebugSample struct {
	at           time.Time
	mode         string
	inShortBps   int64
	inLongBps    int64
	followBps    int64
	targetBps    int64
	adjustedBps  int64
	minNeededBps int64
	queueDelayMs int64
}

type adaptiveRateEWMA struct {
	tau         time.Duration
	value       float64
	initialized bool
	lastAt      time.Time
}

func newAdaptiveRateEWMA(tau time.Duration) adaptiveRateEWMA {
	if tau <= 0 {
		tau = time.Second
	}
	return adaptiveRateEWMA{tau: tau}
}

func (e *adaptiveRateEWMA) setTau(tau time.Duration) {
	if tau <= 0 {
		tau = time.Second
	}
	e.tau = tau
}

func (e *adaptiveRateEWMA) update(now time.Time, sample float64) {
	if sample < 0 {
		sample = 0
	}
	if !e.initialized {
		e.value = sample
		e.initialized = true
		e.lastAt = now
		return
	}
	dt := now.Sub(e.lastAt).Seconds()
	if dt <= 0 {
		return
	}
	tauSec := e.tau.Seconds()
	if tauSec <= 0 {
		tauSec = 1
	}
	alpha := 1 - math.Exp(-dt/tauSec)
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}
	e.value = e.value + alpha*(sample-e.value)
	e.lastAt = now
}

func (e *adaptiveRateEWMA) current() float64 {
	if !e.initialized || e.value < 0 {
		return 0
	}
	return e.value
}

func formatBitrateFromBps(bps int64) string {
	if bps < 0 {
		bps = 0
	}
	kbps := float64(bps) / 1000.0
	if kbps >= 1000.0 {
		return fmt.Sprintf("%.3fMbps", kbps/1000.0)
	}
	return fmt.Sprintf("%.1fKbps", kbps)
}

func formatBitrateSeriesFromBps(values []int64) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, formatBitrateFromBps(v))
	}
	return out
}

func sampleEstimatorIndices(total int, take int) []int {
	if total <= 0 {
		return nil
	}
	if take <= 0 {
		take = 1
	}
	if total <= take {
		indices := make([]int, total)
		for i := 0; i < total; i++ {
			indices[i] = i
		}
		return indices
	}
	if take == 1 {
		return []int{total - 1}
	}

	indices := make([]int, 0, take)
	step := float64(total-1) / float64(take-1)
	last := -1
	for i := 0; i < take; i++ {
		idx := int(math.Round(float64(i) * step))
		if idx < 0 {
			idx = 0
		}
		if idx >= total {
			idx = total - 1
		}
		if idx <= last {
			idx = last + 1
			if idx >= total {
				idx = total - 1
			}
		}
		indices = append(indices, idx)
		last = idx
	}
	return indices
}

func (p *Pacer) appendEstimatorDebugSampleLocked(now time.Time, mode string) {
	if !p.cfg.LogEnabled || !p.cfg.Enabled || !p.running {
		p.estDebugWindowStart = time.Time{}
		if len(p.estDebugSamples) > 0 {
			p.estDebugSamples = p.estDebugSamples[:0]
		}
		return
	}
	if p.estDebugWindowStart.IsZero() {
		p.estDebugWindowStart = now
	}
	if len(p.estDebugSamples) >= 4096 {
		// Keep bounded memory while preserving the newest estimator samples.
		copy(p.estDebugSamples, p.estDebugSamples[1:])
		p.estDebugSamples = p.estDebugSamples[:len(p.estDebugSamples)-1]
	}
	p.estDebugSamples = append(p.estDebugSamples, pacerEstimatorDebugSample{
		at:           now,
		mode:         mode,
		inShortBps:   int64(p.ingressShortEWMA.current() * 8),
		inLongBps:    int64(p.ingressLongEWMA.current() * 8),
		followBps:    int64(p.followRateBytesPerSec * 8),
		targetBps:    int64(p.targetRateBytesPerSec * 8),
		adjustedBps:  int64(p.adjustedRateBytesPerSec * 8),
		minNeededBps: int64(p.minRateNeededBytesPerSec * 8),
		queueDelayMs: int64(p.queueDelaySecLocked(now) * 1000),
	})
}

func (p *Pacer) maybeLogEstimatorDebugLocked(now time.Time) {
	if p.estDebugWindowStart.IsZero() {
		p.estDebugWindowStart = now
		return
	}
	if now.Sub(p.estDebugWindowStart) < pacerEstimatorDebugWindow {
		return
	}
	if len(p.estDebugSamples) == 0 {
		p.estDebugWindowStart = now
		return
	}

	start := p.estDebugWindowStart
	samples := p.estDebugSamples
	totalSamples := len(samples)
	p.estDebugWindowStart = now
	p.estDebugSamples = p.estDebugSamples[:0]

	sampleIdx := sampleEstimatorIndices(totalSamples, pacerEstimatorDebugMaxPoints)
	offsetMs := make([]int64, 0, len(sampleIdx))
	modes := make([]string, 0, len(sampleIdx))
	inShort := make([]int64, 0, len(sampleIdx))
	inLong := make([]int64, 0, len(sampleIdx))
	follow := make([]int64, 0, len(sampleIdx))
	target := make([]int64, 0, len(sampleIdx))
	adjusted := make([]int64, 0, len(sampleIdx))
	minNeeded := make([]int64, 0, len(sampleIdx))
	qDelay := make([]int64, 0, len(sampleIdx))
	for _, idx := range sampleIdx {
		s := samples[idx]
		offsetMs = append(offsetMs, s.at.Sub(start).Milliseconds())
		modes = append(modes, s.mode)
		inShort = append(inShort, s.inShortBps)
		inLong = append(inLong, s.inLongBps)
		follow = append(follow, s.followBps)
		target = append(target, s.targetBps)
		adjusted = append(adjusted, s.adjustedBps)
		minNeeded = append(minNeeded, s.minNeededBps)
		qDelay = append(qDelay, s.queueDelayMs)
	}
	log.Printf("[pacer-est-debug] key=%s dir=%s window_ms=%d samples=%d sampled=%d sampled_idx=%v offsets_ms=%v mode=%v in_short=%v in_long=%v follow=%v target=%v adjusted=%v min_needed=%v queue_delay_ms=%v",
		p.instanceKey, p.direction, now.Sub(start).Milliseconds(), totalSamples, len(sampleIdx), sampleIdx,
		offsetMs, modes,
		formatBitrateSeriesFromBps(inShort),
		formatBitrateSeriesFromBps(inLong),
		formatBitrateSeriesFromBps(follow),
		formatBitrateSeriesFromBps(target),
		formatBitrateSeriesFromBps(adjusted),
		formatBitrateSeriesFromBps(minNeeded),
		qDelay,
	)
}

func (p *Pacer) observeIngressLocked(now time.Time, bytes int) {
	if bytes <= 0 {
		return
	}
	if p.ingressSampleAt.IsZero() {
		p.ingressSampleAt = now
	}
	p.ingressSampleBytes += bytes
	p.flushIngressSampleLocked(now, false)
}

func (p *Pacer) flushIngressSampleLocked(now time.Time, force bool) {
	if p.ingressSampleAt.IsZero() {
		p.ingressSampleAt = now
		return
	}
	elapsed := now.Sub(p.ingressSampleAt)
	const sampleStep = 50 * time.Millisecond
	if !force && elapsed < sampleStep {
		return
	}
	if elapsed <= 0 {
		return
	}
	sampleRate := 0.0
	if p.ingressSampleBytes > 0 {
		sampleRate = float64(p.ingressSampleBytes) / elapsed.Seconds()
	}
	p.ingressShortEWMA.update(now, sampleRate)
	p.ingressLongEWMA.update(now, sampleRate)
	p.ingressSampleBytes = 0
	p.ingressSampleAt = now
}

func (p *Pacer) queueDelaySecLocked(now time.Time) float64 {
	delay := p.avgQueueDelaySec
	oldest := p.oldestQueueAgeSecLocked(now)
	if oldest > delay {
		delay = oldest
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}

func (p *Pacer) queueStableForRecoveryLocked(now time.Time) bool {
	if p.queuePackets == 0 || p.queueBytes == 0 {
		return true
	}
	target := p.cfg.QueueDelayTarget.Seconds()
	if target <= 0 {
		target = 0.1
	}
	return p.queueDelaySecLocked(now) <= target*1.2
}

func (p *Pacer) ApplyRTCPFractionLoss(fractionLost uint8) {
	if !p.cfg.EnableAdaptiveEstimator {
		return
	}
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.cfg.FeedbackEnabled {
		p.feedbackScale = 1
		p.highLossStreak = 0
		p.lowLossStableSince = time.Time{}
		p.lastRecoveryStep = time.Time{}
		return
	}

	if p.feedbackScale <= 0 || p.feedbackScale > 1 {
		p.feedbackScale = 1
	}
	lossHigh := p.cfg.FeedbackLossHigh
	if lossHigh <= 0 || lossHigh >= 1 {
		lossHigh = 0.10
	}
	lossMid := p.cfg.FeedbackLossMid
	if lossMid <= 0 || lossMid >= lossHigh {
		lossMid = 0.05
	}
	dropHigh := p.cfg.FeedbackDropHigh
	if dropHigh <= 0 || dropHigh > 1 {
		dropHigh = 0.70
	}
	dropMid := p.cfg.FeedbackDropMid
	if dropMid <= 0 || dropMid > 1 {
		dropMid = 0.85
	}
	minScale := p.cfg.FeedbackMinScale
	if minScale <= 0 || minScale > 1 {
		minScale = 0.30
	}
	recoverGain := p.cfg.FeedbackRecoverGain
	if recoverGain <= 1 || recoverGain > 1.5 {
		recoverGain = 1.04
	}
	recoverStable := p.cfg.FeedbackRecoverMS
	if recoverStable <= 0 {
		recoverStable = 8 * time.Second
	}
	recoverStep := p.cfg.FeedbackStepMS
	if recoverStep <= 0 {
		recoverStep = 2 * time.Second
	}
	midCount := p.cfg.FeedbackMidCount
	if midCount <= 0 {
		midCount = 2
	}

	loss := float64(fractionLost) / 256.0
	switch {
	case loss > lossHigh:
		p.feedbackScale *= dropHigh
		if p.feedbackScale < minScale {
			p.feedbackScale = minScale
		}
		p.highLossStreak = 0
		p.lowLossStableSince = time.Time{}
		p.lastRecoveryStep = time.Time{}
	case loss > lossMid:
		p.highLossStreak++
		p.lowLossStableSince = time.Time{}
		p.lastRecoveryStep = time.Time{}
		if p.highLossStreak >= midCount {
			p.feedbackScale *= dropMid
			if p.feedbackScale < minScale {
				p.feedbackScale = minScale
			}
			p.highLossStreak = 0
		}
	default:
		p.highLossStreak = 0
		if !p.queueStableForRecoveryLocked(now) {
			p.lowLossStableSince = time.Time{}
			return
		}
		if p.lowLossStableSince.IsZero() {
			p.lowLossStableSince = now
		}
		if now.Sub(p.lowLossStableSince) < recoverStable {
			return
		}
		if !p.lastRecoveryStep.IsZero() && now.Sub(p.lastRecoveryStep) < recoverStep {
			return
		}
		p.feedbackScale *= recoverGain
		if p.feedbackScale > 1 {
			p.feedbackScale = 1
		}
		p.lastRecoveryStep = now
	}
}

func (p *Pacer) updateAdjustedRateLocked(now time.Time) {
	if p.cfg.EnableAdaptiveEstimator {
		p.updateAdaptiveRateLocked(now)
		return
	}
	p.updateLegacyRateLocked(now)
}

func (p *Pacer) updateAdaptiveRateLocked(now time.Time) {
	// Update ingress EWMAs from accepted input bytes.
	p.flushIngressSampleLocked(now, true)
	if p.queuePackets <= 0 || len(p.flowOrder) == 0 {
		p.avgQueueDelaySec = 0
		p.queueHighSince = time.Time{}
	}

	shortIn := p.ingressShortEWMA.current()
	longIn := p.ingressLongEWMA.current()
	follow := shortIn * p.cfg.ShortHeadroom
	longFollow := longIn * p.cfg.LongHeadroom
	if longFollow > follow {
		follow = longFollow
	}
	if follow < 0 {
		follow = 0
	}
	p.followRateBytesPerSec = follow

	overheadMul := 1.0 + p.cfg.RTXReservePct + p.cfg.RTCPOverheadPct
	if overheadMul < 1 {
		overheadMul = 1
	}
	target := follow * overheadMul

	floorRate := p.floorRateBytesPerSecLocked()
	hardCap := p.hardCapBytesPerSecLocked()
	if floorRate > hardCap {
		floorRate = hardCap
	}
	if target < floorRate {
		target = floorRate
	}
	if target > hardCap {
		target = hardCap
	}
	p.minRateNeededBytesPerSec = target

	if !p.cfg.FeedbackEnabled {
		p.feedbackScale = 1
	} else {
		if p.feedbackScale <= 0 || p.feedbackScale > 1 {
			p.feedbackScale = 1
		}
		target *= p.feedbackScale
	}
	if target < floorRate {
		target = floorRate
	}
	if target > hardCap {
		target = hardCap
	}

	queueDelay := p.queueDelaySecLocked(now)
	forceHardCapQueueDelaySec := p.cfg.ForceHardCapQueueDelay.Seconds()
	if forceHardCapQueueDelaySec <= 0 {
		forceHardCapQueueDelaySec = 1.2
	}
	targetMaxUpPerSec := p.cfg.TargetMaxUpPerSec
	if targetMaxUpPerSec <= 1 {
		targetMaxUpPerSec = 1.20
	}
	highWatermark := p.cfg.QueueHighWatermark.Seconds()
	if highWatermark <= 0 {
		highWatermark = 0.2
	}
	forceHardCap := false
	if queueDelay > highWatermark {
		if p.queueHighSince.IsZero() {
			p.queueHighSince = now
		}
		target *= p.cfg.QueueBoostFactor
		if target > hardCap {
			target = hardCap
		}
		if now.Sub(p.queueHighSince) >= p.cfg.QueueSustain && queueDelay > forceHardCapQueueDelaySec {
			target = hardCap
			forceHardCap = true
		}
	} else {
		p.queueHighSince = time.Time{}
	}

	if !forceHardCap {
		prevTarget := p.targetRateBytesPerSec
		if prevTarget < floorRate {
			prevTarget = floorRate
		}
		if prevTarget > hardCap {
			prevTarget = hardCap
		}
		if target > prevTarget && prevTarget > 0 {
			dtSec := 0.0
			if !p.lastTargetUpdateAt.IsZero() {
				dtSec = now.Sub(p.lastTargetUpdateAt).Seconds()
			}
			if dtSec <= 0 {
				dtSec = p.cfg.Tick.Seconds()
			}
			if dtSec <= 0 {
				dtSec = 0.01
			}
			maxTarget := prevTarget * math.Pow(targetMaxUpPerSec, dtSec)
			if maxTarget < floorRate {
				maxTarget = floorRate
			}
			if maxTarget > hardCap {
				maxTarget = hardCap
			}
			if target > maxTarget {
				target = maxTarget
			}
		}
	}
	if target < floorRate {
		target = floorRate
	}
	if target > hardCap {
		target = hardCap
	}

	p.lastTargetUpdateAt = now
	p.targetRateBytesPerSec = target
	p.adjustedRateBytesPerSec = p.clampRateLocked(target)
	p.appendEstimatorDebugSampleLocked(now, "adaptive")
	p.maybeLogEstimatorDebugLocked(now)
}

func (p *Pacer) updateLegacyRateLocked(now time.Time) {
	base := p.hardCapBytesPerSecLocked()
	if base < 1 {
		base = 1
	}
	if p.queuePackets <= 0 || p.queueBytes <= 0 || len(p.flowOrder) == 0 {
		p.minRateNeededBytesPerSec = base
		p.followRateBytesPerSec = base
		p.targetRateBytesPerSec = base
		p.adjustedRateBytesPerSec = base
		p.avgQueueDelaySec = 0
		p.queueHighSince = time.Time{}
		p.feedbackScale = 1
		p.appendEstimatorDebugSampleLocked(now, "legacy")
		p.maybeLogEstimatorDebugLocked(now)
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
		adj = adj*0.9 + minRate*0.1
		if adj < minRate {
			adj = minRate
		}
	}
	if adj < base {
		adj = base
	}
	p.feedbackScale = 1
	p.followRateBytesPerSec = base
	p.targetRateBytesPerSec = adj
	p.adjustedRateBytesPerSec = adj
	p.appendEstimatorDebugSampleLocked(now, "legacy")
	p.maybeLogEstimatorDebugLocked(now)
}
