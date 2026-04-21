package main

import (
	"net"
	"time"
)

type PacerResolvedParams struct {
	EnablePacer         bool                `json:"enable_pacer"`
	EnablePacerAdaptive bool                `json:"enable_pacer_adaptive"`
	SendRateBps         int                 `json:"send_rate_bps"`
	TickMS              int                 `json:"tick_ms"`
	LogEnabled          bool                `json:"log_enabled"`
	LogPeriodMS         int                 `json:"log_period_ms"`
	Adaptive            PacerAdaptiveConfig `json:"pacer_adaptive"`
}

func boolPtr(v bool) *bool {
	x := v
	return &x
}

func intPtr(v int) *int {
	x := v
	return &x
}

func defaultPacerResolvedParams(cfg *Config) PacerResolvedParams {
	rate := cfg.SendRateBps
	if rate <= 0 {
		rate = 20_000_000
	}
	tickMS := cfg.JitterTickMS
	if tickMS <= 0 {
		tickMS = 10
	}
	return PacerResolvedParams{
		EnablePacer:         cfg.EnablePacer,
		EnablePacerAdaptive: cfg.EnablePacerAdaptive,
		SendRateBps:         rate,
		TickMS:              tickMS,
		LogEnabled:          true,
		LogPeriodMS:         2000,
		Adaptive:            cfg.PacerAdaptive,
	}
}

func applyPacerTargetOverride(base PacerResolvedParams, ov PacerTargetConfig) PacerResolvedParams {
	if ov.EnablePacer != nil {
		base.EnablePacer = *ov.EnablePacer
	}
	if ov.SendRateBps != nil {
		base.SendRateBps = *ov.SendRateBps
	}
	if ov.TickMS != nil {
		base.TickMS = *ov.TickMS
	}
	if ov.LogEnabled != nil {
		base.LogEnabled = *ov.LogEnabled
	}
	if ov.LogPeriodMS != nil {
		base.LogPeriodMS = *ov.LogPeriodMS
	}

	if base.SendRateBps <= 0 {
		base.SendRateBps = 20_000_000
	}
	if base.TickMS <= 0 {
		base.TickMS = 10
	}
	if base.LogPeriodMS < 0 {
		base.LogPeriodMS = 0
	}
	return base
}

func applyPacerIPProfile(base PacerResolvedParams, profile PacerIPProfile) PacerResolvedParams {
	base.EnablePacer = profile.EnablePacer
	base.SendRateBps = profile.SendRateBps
	if base.SendRateBps <= 0 {
		base.SendRateBps = 20_000_000
	}
	return base
}

func ipFromTargetKey(targetKey string) (string, bool) {
	if targetKey == "" {
		return "", false
	}
	host, _, err := net.SplitHostPort(targetKey)
	if err != nil {
		return "", false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", false
	}
	return ip.String(), true
}

func resolvedToOverride(p PacerResolvedParams) PacerTargetConfig {
	return PacerTargetConfig{
		EnablePacer: boolPtr(p.EnablePacer),
		SendRateBps: intPtr(p.SendRateBps),
		TickMS:      intPtr(p.TickMS),
		LogEnabled:  boolPtr(p.LogEnabled),
		LogPeriodMS: intPtr(p.LogPeriodMS),
	}
}

func (r *Relay) resolvePacerParamsForTargetKey(targetKey string) PacerResolvedParams {
	r.pacerCfgMux.RLock()
	defer r.pacerCfgMux.RUnlock()

	base := defaultPacerResolvedParams(r.cfg)

	if targetKey != "" && r.cfg.PacerIPProfiles != nil {
		if ip, ok := ipFromTargetKey(targetKey); ok {
			if profile, found := r.cfg.PacerIPProfiles[ip]; found {
				base = applyPacerIPProfile(base, profile)
			}
		}
	}

	if targetKey != "" && r.pacerOverrides != nil {
		if ov, ok := r.pacerOverrides[targetKey]; ok {
			return applyPacerTargetOverride(base, ov)
		}
	}
	return base
}

func (r *Relay) getPacerIPProfile(ip string) (PacerIPProfile, bool) {
	r.pacerCfgMux.RLock()
	defer r.pacerCfgMux.RUnlock()
	if r.cfg.PacerIPProfiles == nil {
		return PacerIPProfile{}, false
	}
	profile, ok := r.cfg.PacerIPProfiles[ip]
	return profile, ok
}

func (r *Relay) setPacerIPProfile(ip string, profile PacerIPProfile) {
	r.pacerCfgMux.Lock()
	if r.cfg.PacerIPProfiles == nil {
		r.cfg.PacerIPProfiles = make(map[string]PacerIPProfile)
	}
	r.cfg.PacerIPProfiles[ip] = profile
	r.pacerCfgMux.Unlock()
}

func (r *Relay) deletePacerIPProfile(ip string) {
	r.pacerCfgMux.Lock()
	if r.cfg.PacerIPProfiles != nil {
		delete(r.cfg.PacerIPProfiles, ip)
	}
	r.pacerCfgMux.Unlock()
}

func (r *Relay) snapshotPacerIPProfiles() map[string]PacerIPProfile {
	r.pacerCfgMux.RLock()
	defer r.pacerCfgMux.RUnlock()
	out := make(map[string]PacerIPProfile, len(r.cfg.PacerIPProfiles))
	for ip, profile := range r.cfg.PacerIPProfiles {
		out[ip] = profile
	}
	return out
}

func (r *Relay) applyPacerIPProfileToExistingPacers(ip string) {
	r.pacerMux.RLock()
	keys := make([]string, 0, len(r.pacers))
	for key := range r.pacers {
		keys = append(keys, key)
	}
	r.pacerMux.RUnlock()

	for _, key := range keys {
		keyIP, ok := ipFromTargetKey(key)
		if !ok || keyIP != ip {
			continue
		}
		params := r.resolvePacerParamsForTargetKey(key)
		r.applyResolvedParamsToPacer(key, params)
	}
}

func pacerConfigFromResolved(p PacerResolvedParams) PacerConfig {
	cfg := PacerConfig{
		Enabled:                 p.EnablePacer,
		EnableAdaptiveEstimator: p.EnablePacerAdaptive,
		RateBps:                 p.SendRateBps,
		RateBytesPerSec:         p.SendRateBps / 8,
		AdminCapBps:             p.SendRateBps,
		Tick:                    time.Duration(p.TickMS) * time.Millisecond,
		LogEnabled:              p.LogEnabled,
		LogPeriod:               time.Duration(p.LogPeriodMS) * time.Millisecond,
	}
	cfg.ShortEWMA = time.Duration(p.Adaptive.ShortEWMAMS) * time.Millisecond
	cfg.LongEWMA = time.Duration(p.Adaptive.LongEWMAMS) * time.Millisecond
	cfg.ShortHeadroom = p.Adaptive.ShortHeadroom
	cfg.LongHeadroom = p.Adaptive.LongHeadroom
	cfg.RTXReservePct = p.Adaptive.RTXReservePct
	cfg.RTCPOverheadPct = p.Adaptive.RTCPOverheadPct
	cfg.StartRateBps = p.Adaptive.StartRateBps
	cfg.FloorRateBps = p.Adaptive.FloorRateBps
	cfg.QueueDelayTarget = time.Duration(p.Adaptive.QueueDelayTargetMS) * time.Millisecond
	cfg.QueueHighWatermark = time.Duration(p.Adaptive.QueueHighWatermarkMS) * time.Millisecond
	cfg.QueueSustain = time.Duration(p.Adaptive.QueueSustainMS) * time.Millisecond
	cfg.QueueBoostFactor = p.Adaptive.QueueBoostFactor
	cfg.ForceHardCapQueueDelay = time.Duration(p.Adaptive.ForceHardCapQueueDelayMS) * time.Millisecond
	cfg.TargetMaxUpPerSec = p.Adaptive.TargetMaxUpPerSec
	cfg.FeedbackLossHigh = p.Adaptive.FeedbackLossHigh
	cfg.FeedbackLossMid = p.Adaptive.FeedbackLossMid
	cfg.FeedbackDropHigh = p.Adaptive.FeedbackDropHigh
	cfg.FeedbackDropMid = p.Adaptive.FeedbackDropMid
	cfg.FeedbackMinScale = p.Adaptive.FeedbackMinScale
	cfg.FeedbackRecoverGain = p.Adaptive.FeedbackRecoverGain
	cfg.FeedbackRecoverMS = time.Duration(p.Adaptive.FeedbackRecoverMS) * time.Millisecond
	cfg.FeedbackStepMS = time.Duration(p.Adaptive.FeedbackStepMS) * time.Millisecond
	cfg.FeedbackMidCount = p.Adaptive.FeedbackMidCount
	cfg.FeedbackEnabled = p.Adaptive.FeedbackEnabled
	if cfg.Tick <= 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	return cfg
}

func (r *Relay) getPacerByKey(targetKey string) *Pacer {
	r.pacerMux.RLock()
	p := r.pacers[targetKey]
	r.pacerMux.RUnlock()
	return p
}

func (r *Relay) getOrCreatePacerForTarget(target *net.UDPAddr) (*Pacer, string, error) {
	if target == nil {
		return nil, "", nil
	}
	targetKey := target.String()
	if targetKey == "" {
		return nil, "", nil
	}

	if p := r.getPacerByKey(targetKey); p != nil {
		return p, targetKey, nil
	}

	params := r.resolvePacerParamsForTargetKey(targetKey)
	pcfg := pacerConfigFromResolved(params)
	p := NewPacer(r.rtpConn, pcfg)
	p.SetMeta(targetKey, "egress")
	p.SetOnSent(r.onPacerSent)

	r.pacerMux.Lock()
	if exist := r.pacers[targetKey]; exist != nil {
		r.pacerMux.Unlock()
		p.Stop()
		return exist, targetKey, nil
	}
	r.pacers[targetKey] = p
	r.pacerMux.Unlock()

	return p, targetKey, nil
}

func (r *Relay) getPacerStatsByKey(targetKey string) (PacerStats, bool, bool) {
	r.pacerMux.RLock()
	p := r.pacers[targetKey]
	r.pacerMux.RUnlock()
	if p == nil {
		return PacerStats{}, false, false
	}
	return p.Stats(), p.IsRunning(), true
}

func (r *Relay) getPacerStatsForTarget(target *net.UDPAddr) (PacerStats, bool, bool) {
	if target == nil {
		return PacerStats{}, false, false
	}
	return r.getPacerStatsByKey(target.String())
}

func (r *Relay) applyResolvedParamsToPacer(targetKey string, params PacerResolvedParams) bool {
	p := r.getPacerByKey(targetKey)
	if p == nil {
		return false
	}
	pcfg := p.Config()
	pcfg.Enabled = params.EnablePacer
	pcfg.EnableAdaptiveEstimator = params.EnablePacerAdaptive
	pcfg.RateBps = params.SendRateBps
	pcfg.RateBytesPerSec = params.SendRateBps / 8
	pcfg.AdminCapBps = params.SendRateBps
	pcfg.Tick = time.Duration(params.TickMS) * time.Millisecond
	pcfg.LogEnabled = params.LogEnabled
	pcfg.LogPeriod = time.Duration(params.LogPeriodMS) * time.Millisecond
	pcfg.ShortEWMA = time.Duration(params.Adaptive.ShortEWMAMS) * time.Millisecond
	pcfg.LongEWMA = time.Duration(params.Adaptive.LongEWMAMS) * time.Millisecond
	pcfg.ShortHeadroom = params.Adaptive.ShortHeadroom
	pcfg.LongHeadroom = params.Adaptive.LongHeadroom
	pcfg.RTXReservePct = params.Adaptive.RTXReservePct
	pcfg.RTCPOverheadPct = params.Adaptive.RTCPOverheadPct
	pcfg.StartRateBps = params.Adaptive.StartRateBps
	pcfg.FloorRateBps = params.Adaptive.FloorRateBps
	pcfg.QueueDelayTarget = time.Duration(params.Adaptive.QueueDelayTargetMS) * time.Millisecond
	pcfg.QueueHighWatermark = time.Duration(params.Adaptive.QueueHighWatermarkMS) * time.Millisecond
	pcfg.QueueSustain = time.Duration(params.Adaptive.QueueSustainMS) * time.Millisecond
	pcfg.QueueBoostFactor = params.Adaptive.QueueBoostFactor
	pcfg.ForceHardCapQueueDelay = time.Duration(params.Adaptive.ForceHardCapQueueDelayMS) * time.Millisecond
	pcfg.TargetMaxUpPerSec = params.Adaptive.TargetMaxUpPerSec
	pcfg.FeedbackLossHigh = params.Adaptive.FeedbackLossHigh
	pcfg.FeedbackLossMid = params.Adaptive.FeedbackLossMid
	pcfg.FeedbackDropHigh = params.Adaptive.FeedbackDropHigh
	pcfg.FeedbackDropMid = params.Adaptive.FeedbackDropMid
	pcfg.FeedbackMinScale = params.Adaptive.FeedbackMinScale
	pcfg.FeedbackRecoverGain = params.Adaptive.FeedbackRecoverGain
	pcfg.FeedbackRecoverMS = time.Duration(params.Adaptive.FeedbackRecoverMS) * time.Millisecond
	pcfg.FeedbackStepMS = time.Duration(params.Adaptive.FeedbackStepMS) * time.Millisecond
	pcfg.FeedbackMidCount = params.Adaptive.FeedbackMidCount
	pcfg.FeedbackEnabled = params.Adaptive.FeedbackEnabled
	p.UpdateConfig(pcfg)
	return true
}

func (r *Relay) applyResolvedParamsToAllPacers() {
	r.pacerMux.RLock()
	keys := make([]string, 0, len(r.pacers))
	for key := range r.pacers {
		keys = append(keys, key)
	}
	r.pacerMux.RUnlock()

	for _, key := range keys {
		params := r.resolvePacerParamsForTargetKey(key)
		r.applyResolvedParamsToPacer(key, params)
	}
}

func (r *Relay) getPacerOverride(targetKey string) (PacerTargetConfig, bool) {
	r.pacerCfgMux.RLock()
	defer r.pacerCfgMux.RUnlock()
	if r.pacerOverrides == nil {
		return PacerTargetConfig{}, false
	}
	ov, ok := r.pacerOverrides[targetKey]
	if !ok {
		return PacerTargetConfig{}, false
	}
	return clonePacerTargetConfig(ov), true
}

func (r *Relay) setPacerOverride(targetKey string, ov PacerTargetConfig) {
	r.pacerCfgMux.Lock()
	if r.pacerOverrides == nil {
		r.pacerOverrides = make(map[string]PacerTargetConfig)
	}
	r.pacerOverrides[targetKey] = clonePacerTargetConfig(ov)
	r.pacerCfgMux.Unlock()
}

func (r *Relay) deletePacerOverride(targetKey string) {
	r.pacerCfgMux.Lock()
	if r.pacerOverrides != nil {
		delete(r.pacerOverrides, targetKey)
	}
	r.pacerCfgMux.Unlock()
}

func (r *Relay) snapshotPacerOverrides() map[string]PacerTargetConfig {
	r.pacerCfgMux.RLock()
	defer r.pacerCfgMux.RUnlock()

	out := make(map[string]PacerTargetConfig, len(r.pacerOverrides))
	for key, ov := range r.pacerOverrides {
		out[key] = clonePacerTargetConfig(ov)
	}
	return out
}

func (r *Relay) setGlobalPacerDefaults(enable bool, adaptive bool, rateBps int) {
	r.pacerCfgMux.Lock()
	r.cfg.EnablePacer = enable
	r.cfg.EnablePacerAdaptive = adaptive
	r.cfg.SendRateBps = rateBps
	r.pacerCfgMux.Unlock()
}

func (r *Relay) getGlobalPacerDefaults() (bool, int) {
	r.pacerCfgMux.RLock()
	defer r.pacerCfgMux.RUnlock()
	return r.cfg.EnablePacer, r.cfg.SendRateBps
}
