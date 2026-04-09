package main

import (
	"net"
	"time"
)

type PacerResolvedParams struct {
	EnablePacer bool `json:"enable_pacer"`
	SendRateBps int  `json:"send_rate_bps"`
	TickMS      int  `json:"tick_ms"`
	LogEnabled  bool `json:"log_enabled"`
	LogPeriodMS int  `json:"log_period_ms"`
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
		EnablePacer: cfg.EnablePacer,
		SendRateBps: rate,
		TickMS:      tickMS,
		LogEnabled:  true,
		LogPeriodMS: 2000,
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
	if targetKey == "" || r.pacerOverrides == nil {
		return base
	}
	if ov, ok := r.pacerOverrides[targetKey]; ok {
		return applyPacerTargetOverride(base, ov)
	}
	return base
}

func pacerConfigFromResolved(p PacerResolvedParams) PacerConfig {
	cfg := PacerConfig{
		Enabled:         p.EnablePacer,
		RateBps:         p.SendRateBps,
		RateBytesPerSec: p.SendRateBps / 8,
		Tick:            time.Duration(p.TickMS) * time.Millisecond,
		LogEnabled:      p.LogEnabled,
		LogPeriod:       time.Duration(p.LogPeriodMS) * time.Millisecond,
	}
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
	pcfg.RateBps = params.SendRateBps
	pcfg.RateBytesPerSec = params.SendRateBps / 8
	pcfg.Tick = time.Duration(params.TickMS) * time.Millisecond
	pcfg.LogEnabled = params.LogEnabled
	pcfg.LogPeriod = time.Duration(params.LogPeriodMS) * time.Millisecond
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

func (r *Relay) setGlobalPacerDefaults(enable bool, rateBps int) {
	r.pacerCfgMux.Lock()
	r.cfg.EnablePacer = enable
	r.cfg.SendRateBps = rateBps
	r.pacerCfgMux.Unlock()
}

func (r *Relay) getGlobalPacerDefaults() (bool, int) {
	r.pacerCfgMux.RLock()
	defer r.pacerCfgMux.RUnlock()
	return r.cfg.EnablePacer, r.cfg.SendRateBps
}
