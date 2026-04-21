package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type UpdateAddrMappingRequest struct {
	Key        string `json:"key"`
	TargetIP   string `json:"target_ip"`
	TargetPort int    `json:"target_port"`
}

type RemoveMappingRequest struct {
	Key string `json:"key"`
}

type ForwardDebugTargetRequest struct {
	IP string `json:"ip"`
}

type UpdateJitterParamsRequest struct {
	EnableJitter           *bool     `json:"enable_jitter"`
	JitterSourceIPsEnabled *bool     `json:"jitter_source_ips_enabled"`
	PacketHistoryMS        *int      `json:"packet_history_ms"`
	JitterBufferMS         *int      `json:"jitter_buffer_ms"`
	JitterMinDelayMS       *int      `json:"jitter_min_delay_ms"`
	JitterMaxDelayMS       *int      `json:"jitter_max_delay_ms"`
	JitterTickMS           *int      `json:"jitter_tick_ms"`
	JitterUpStepMS         *int      `json:"jitter_up_step_ms"`
	JitterDownStepMS       *int      `json:"jitter_down_step_ms"`
	JitterLogEnabled       *bool     `json:"jitter_log_enabled"`
	JitterLogPeriodMS      *int      `json:"jitter_log_period_ms"`
	JitterSourceIPs        *[]string `json:"jitter_source_ips"`
}

type UpdatePacerRequest struct {
	Target              string `json:"target"`
	DeleteTarget        *bool  `json:"delete_target"`
	EnablePacer         *bool  `json:"enable_pacer"`
	EnablePacerAdaptive *bool  `json:"enable_pacer_adaptive"`
	RateBps             *int   `json:"rate_bps"`
	SendRateBps         *int   `json:"send_rate_bps"`
	TickMS              *int   `json:"tick_ms"`
	LogEnabled          *bool  `json:"log_enabled"`
	LogPeriodMS         *int   `json:"log_period_ms"`
}

type UpdatePacerIPProfileRequest struct {
	IP          string `json:"ip"`
	Delete      *bool  `json:"delete"`
	EnablePacer *bool  `json:"enable_pacer"`
	RateBps     *int   `json:"rate_bps"`
	SendRateBps *int   `json:"send_rate_bps"`
}

type UpdatePacerAdaptiveRequest struct {
	ShortEWMAMS              *int     `json:"short_ewma_ms"`
	LongEWMAMS               *int     `json:"long_ewma_ms"`
	ShortHeadroom            *float64 `json:"short_headroom"`
	LongHeadroom             *float64 `json:"long_headroom"`
	RTXReservePct            *float64 `json:"rtx_reserve_pct"`
	RTCPOverheadPct          *float64 `json:"rtcp_overhead_pct"`
	StartRateBps             *int     `json:"start_rate_bps"`
	FloorRateBps             *int     `json:"floor_rate_bps"`
	QueueDelayTargetMS       *int     `json:"queue_delay_target_ms"`
	QueueHighWatermarkMS     *int     `json:"queue_high_watermark_ms"`
	QueueSustainMS           *int     `json:"queue_sustain_ms"`
	QueueBoostFactor         *float64 `json:"queue_boost_factor"`
	ForceHardCapQueueDelayMS *int     `json:"force_hard_cap_queue_delay_ms"`
	TargetMaxUpPerSec        *float64 `json:"target_max_up_per_sec"`
	FeedbackLossHigh         *float64 `json:"feedback_loss_high"`
	FeedbackLossMid          *float64 `json:"feedback_loss_mid"`
	FeedbackDropHigh         *float64 `json:"feedback_drop_high"`
	FeedbackDropMid          *float64 `json:"feedback_drop_mid"`
	FeedbackMinScale         *float64 `json:"feedback_min_scale"`
	FeedbackRecoverGain      *float64 `json:"feedback_recover_gain"`
	FeedbackRecoverMS        *int     `json:"feedback_recover_ms"`
	FeedbackStepMS           *int     `json:"feedback_step_ms"`
	FeedbackMidCount         *int     `json:"feedback_mid_count"`
	FeedbackEnabled          *bool    `json:"feedback_enabled"`
}

type JitterParams struct {
	EnableJitter           bool     `json:"enable_jitter"`
	JitterSourceIPsEnabled bool     `json:"jitter_source_ips_enabled"`
	PacketHistoryMS        int      `json:"packet_history_ms"`
	JitterBufferMS         int      `json:"jitter_buffer_ms"`
	JitterMinDelayMS       int      `json:"jitter_min_delay_ms"`
	JitterMaxDelayMS       int      `json:"jitter_max_delay_ms"`
	JitterTickMS           int      `json:"jitter_tick_ms"`
	JitterUpStepMS         int      `json:"jitter_up_step_ms"`
	JitterDownStepMS       int      `json:"jitter_down_step_ms"`
	JitterLogEnabled       bool     `json:"jitter_log_enabled"`
	JitterLogPeriodMS      int      `json:"jitter_log_period_ms"`
	JitterSourceIPs        []string `json:"jitter_source_ips"`
}

func (r *Relay) applyConfigToRuntime(cfg *Config) {
	if cfg == nil {
		return
	}
	r.cfg = cfg

	// sendLimiter is disabled; pacing is handled by per-target pacers.

	startWorkers := make([]struct {
		ssrc uint32
		jb   *JitterBuffer
	}, 0)

	r.historyMutex.Lock()
	for _, ph := range r.history {
		if ph == nil {
			continue
		}
		ph.UpdateMaxMS(cfg.PacketHistoryMS)
	}
	for ssrc, jb := range r.jitter {
		if jb == nil {
			continue
		}
		jb.SetEnabled(cfg.EnableJitter)
		jb.UpdateParams(
			cfg.JitterMinDelayMS,
			cfg.JitterMaxDelayMS,
			cfg.JitterTickMS,
			cfg.JitterUpStepMS,
			cfg.JitterDownStepMS,
			cfg.JitterLogEnabled,
			cfg.JitterLogPeriodMS,
		)
		if cfg.EnableJitter && !r.jitterForwarders[ssrc] {
			r.jitterForwarders[ssrc] = true
			startWorkers = append(startWorkers, struct {
				ssrc uint32
				jb   *JitterBuffer
			}{ssrc: ssrc, jb: jb})
		}
	}
	r.historyMutex.Unlock()

	for _, wkr := range startWorkers {
		log.Printf("[jitter][ssrc=%d] start forward worker", wkr.ssrc)
		go r.forwardFromJitter(wkr.ssrc, wkr.jb)
	}

	r.applyResolvedParamsToAllPacers()
}

func currentJitterParams(cfg *Config) JitterParams {
	p := JitterParams{
		EnableJitter:           cfg.EnableJitter,
		JitterSourceIPsEnabled: cfg.JitterSourceIPsEnabled,
		PacketHistoryMS:        cfg.PacketHistoryMS,
		JitterBufferMS:         cfg.JitterBufferMS,
		JitterMinDelayMS:       cfg.JitterMinDelayMS,
		JitterMaxDelayMS:       cfg.JitterMaxDelayMS,
		JitterTickMS:           cfg.JitterTickMS,
		JitterUpStepMS:         cfg.JitterUpStepMS,
		JitterDownStepMS:       cfg.JitterDownStepMS,
		JitterLogEnabled:       cfg.JitterLogEnabled,
		JitterLogPeriodMS:      cfg.JitterLogPeriodMS,
		JitterSourceIPs:        append([]string(nil), cfg.JitterSourceIPs...),
	}
	normalizeJitterParams(&p)
	return p
}

func normalizeJitterParams(p *JitterParams) {
	if p.PacketHistoryMS <= 0 {
		p.PacketHistoryMS = 1500
	}
	if p.JitterBufferMS <= 0 {
		p.JitterBufferMS = 200
	}
	if p.JitterMinDelayMS <= 0 {
		p.JitterMinDelayMS = 20
	}
	if p.JitterMaxDelayMS <= 0 {
		p.JitterMaxDelayMS = p.JitterBufferMS
	}
	if p.JitterMaxDelayMS < p.JitterMinDelayMS {
		p.JitterMaxDelayMS = p.JitterMinDelayMS
	}
	if p.JitterTickMS <= 0 {
		p.JitterTickMS = 10
	}
	if p.JitterUpStepMS <= 0 {
		p.JitterUpStepMS = 20
	}
	if p.JitterDownStepMS <= 0 {
		p.JitterDownStepMS = 5
	}
	if p.JitterLogPeriodMS < 0 {
		p.JitterLogPeriodMS = 0
	}
	if p.JitterSourceIPs == nil {
		p.JitterSourceIPs = []string{}
	}
}

func currentPacerAdaptiveParams(cfg *Config) PacerAdaptiveConfig {
	p := cfg.PacerAdaptive
	normalizePacerAdaptiveConfig(&p)
	return p
}

func mergePacerAdaptiveParams(base PacerAdaptiveConfig, req *UpdatePacerAdaptiveRequest) PacerAdaptiveConfig {
	p := base
	if req.ShortEWMAMS != nil {
		p.ShortEWMAMS = *req.ShortEWMAMS
	}
	if req.LongEWMAMS != nil {
		p.LongEWMAMS = *req.LongEWMAMS
	}
	if req.ShortHeadroom != nil {
		p.ShortHeadroom = *req.ShortHeadroom
	}
	if req.LongHeadroom != nil {
		p.LongHeadroom = *req.LongHeadroom
	}
	if req.RTXReservePct != nil {
		p.RTXReservePct = *req.RTXReservePct
	}
	if req.RTCPOverheadPct != nil {
		p.RTCPOverheadPct = *req.RTCPOverheadPct
	}
	if req.StartRateBps != nil {
		p.StartRateBps = *req.StartRateBps
	}
	if req.FloorRateBps != nil {
		p.FloorRateBps = *req.FloorRateBps
	}
	if req.QueueDelayTargetMS != nil {
		p.QueueDelayTargetMS = *req.QueueDelayTargetMS
	}
	if req.QueueHighWatermarkMS != nil {
		p.QueueHighWatermarkMS = *req.QueueHighWatermarkMS
	}
	if req.QueueSustainMS != nil {
		p.QueueSustainMS = *req.QueueSustainMS
	}
	if req.QueueBoostFactor != nil {
		p.QueueBoostFactor = *req.QueueBoostFactor
	}
	if req.ForceHardCapQueueDelayMS != nil {
		p.ForceHardCapQueueDelayMS = *req.ForceHardCapQueueDelayMS
	}
	if req.TargetMaxUpPerSec != nil {
		p.TargetMaxUpPerSec = *req.TargetMaxUpPerSec
	}
	if req.FeedbackLossHigh != nil {
		p.FeedbackLossHigh = *req.FeedbackLossHigh
	}
	if req.FeedbackLossMid != nil {
		p.FeedbackLossMid = *req.FeedbackLossMid
	}
	if req.FeedbackDropHigh != nil {
		p.FeedbackDropHigh = *req.FeedbackDropHigh
	}
	if req.FeedbackDropMid != nil {
		p.FeedbackDropMid = *req.FeedbackDropMid
	}
	if req.FeedbackMinScale != nil {
		p.FeedbackMinScale = *req.FeedbackMinScale
	}
	if req.FeedbackRecoverGain != nil {
		p.FeedbackRecoverGain = *req.FeedbackRecoverGain
	}
	if req.FeedbackRecoverMS != nil {
		p.FeedbackRecoverMS = *req.FeedbackRecoverMS
	}
	if req.FeedbackStepMS != nil {
		p.FeedbackStepMS = *req.FeedbackStepMS
	}
	if req.FeedbackMidCount != nil {
		p.FeedbackMidCount = *req.FeedbackMidCount
	}
	if req.FeedbackEnabled != nil {
		p.FeedbackEnabled = *req.FeedbackEnabled
	}
	normalizePacerAdaptiveConfig(&p)
	return p
}

func pacerAdaptiveToPatchMap(p PacerAdaptiveConfig) map[string]any {
	return map[string]any{
		"short_ewma_ms":                 p.ShortEWMAMS,
		"long_ewma_ms":                  p.LongEWMAMS,
		"short_headroom":                p.ShortHeadroom,
		"long_headroom":                 p.LongHeadroom,
		"rtx_reserve_pct":               p.RTXReservePct,
		"rtcp_overhead_pct":             p.RTCPOverheadPct,
		"start_rate_bps":                p.StartRateBps,
		"floor_rate_bps":                p.FloorRateBps,
		"queue_delay_target_ms":         p.QueueDelayTargetMS,
		"queue_high_watermark_ms":       p.QueueHighWatermarkMS,
		"queue_sustain_ms":              p.QueueSustainMS,
		"queue_boost_factor":            p.QueueBoostFactor,
		"force_hard_cap_queue_delay_ms": p.ForceHardCapQueueDelayMS,
		"target_max_up_per_sec":         p.TargetMaxUpPerSec,
		"feedback_loss_high":            p.FeedbackLossHigh,
		"feedback_loss_mid":             p.FeedbackLossMid,
		"feedback_drop_high":            p.FeedbackDropHigh,
		"feedback_drop_mid":             p.FeedbackDropMid,
		"feedback_min_scale":            p.FeedbackMinScale,
		"feedback_recover_gain":         p.FeedbackRecoverGain,
		"feedback_recover_ms":           p.FeedbackRecoverMS,
		"feedback_step_ms":              p.FeedbackStepMS,
		"feedback_mid_count":            p.FeedbackMidCount,
		"feedback_enabled":              p.FeedbackEnabled,
	}
}

func mergeJitterParams(base JitterParams, req *UpdateJitterParamsRequest) (JitterParams, error) {
	p := base
	if req.EnableJitter != nil {
		p.EnableJitter = *req.EnableJitter
	}
	if req.JitterSourceIPsEnabled != nil {
		p.JitterSourceIPsEnabled = *req.JitterSourceIPsEnabled
	}
	if req.PacketHistoryMS != nil {
		p.PacketHistoryMS = *req.PacketHistoryMS
	}
	if req.JitterBufferMS != nil {
		p.JitterBufferMS = *req.JitterBufferMS
	}
	if req.JitterMinDelayMS != nil {
		p.JitterMinDelayMS = *req.JitterMinDelayMS
	}
	if req.JitterMaxDelayMS != nil {
		p.JitterMaxDelayMS = *req.JitterMaxDelayMS
	}
	if req.JitterTickMS != nil {
		p.JitterTickMS = *req.JitterTickMS
	}
	if req.JitterUpStepMS != nil {
		p.JitterUpStepMS = *req.JitterUpStepMS
	}
	if req.JitterDownStepMS != nil {
		p.JitterDownStepMS = *req.JitterDownStepMS
	}
	if req.JitterLogEnabled != nil {
		p.JitterLogEnabled = *req.JitterLogEnabled
	}
	if req.JitterLogPeriodMS != nil {
		p.JitterLogPeriodMS = *req.JitterLogPeriodMS
	}
	if req.JitterSourceIPs != nil {
		ips, err := normalizeJitterSourceIPs(*req.JitterSourceIPs)
		if err != nil {
			return p, err
		}
		p.JitterSourceIPs = ips
	}
	normalizeJitterParams(&p)
	return p, nil
}

func (r *Relay) handleUpdateJitterParams(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if req.Method == http.MethodGet {
		out := currentJitterParams(r.cfg)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"jitter":  out,
			"enabled": r.cfg.EnableJitter,
			"path":    r.cfg.ConfigPath,
		})
		return
	}

	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body UpdateJitterParamsRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	p, err := mergeJitterParams(currentJitterParams(r.cfg), &body)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid jitter_source_ips: %v", err), http.StatusBadRequest)
		return
	}

	r.cfg.EnableJitter = p.EnableJitter
	r.cfg.JitterSourceIPsEnabled = p.JitterSourceIPsEnabled
	r.cfg.PacketHistoryMS = p.PacketHistoryMS
	r.cfg.JitterBufferMS = p.JitterBufferMS
	r.cfg.JitterMinDelayMS = p.JitterMinDelayMS
	r.cfg.JitterMaxDelayMS = p.JitterMaxDelayMS
	r.cfg.JitterTickMS = p.JitterTickMS
	r.cfg.JitterUpStepMS = p.JitterUpStepMS
	r.cfg.JitterDownStepMS = p.JitterDownStepMS
	r.cfg.JitterLogEnabled = p.JitterLogEnabled
	r.cfg.JitterLogPeriodMS = p.JitterLogPeriodMS
	r.cfg.JitterSourceIPs = append([]string(nil), p.JitterSourceIPs...)

	startWorkers := make([]struct {
		ssrc uint32
		jb   *JitterBuffer
	}, 0)

	r.historyMutex.Lock()
	for _, ph := range r.history {
		if ph == nil {
			continue
		}
		ph.UpdateMaxMS(r.cfg.PacketHistoryMS)
	}
	for ssrc, jb := range r.jitter {
		if jb == nil {
			continue
		}
		jb.SetEnabled(r.cfg.EnableJitter)
		jb.UpdateParams(
			r.cfg.JitterMinDelayMS,
			r.cfg.JitterMaxDelayMS,
			r.cfg.JitterTickMS,
			r.cfg.JitterUpStepMS,
			r.cfg.JitterDownStepMS,
			r.cfg.JitterLogEnabled,
			r.cfg.JitterLogPeriodMS,
		)
		if r.cfg.EnableJitter && !r.jitterForwarders[ssrc] {
			r.jitterForwarders[ssrc] = true
			startWorkers = append(startWorkers, struct {
				ssrc uint32
				jb   *JitterBuffer
			}{ssrc: ssrc, jb: jb})
		}
	}
	r.historyMutex.Unlock()

	for _, wkr := range startWorkers {
		log.Printf("[jitter][ssrc=%d] start forward worker", wkr.ssrc)
		go r.forwardFromJitter(wkr.ssrc, wkr.jb)
	}

	if err := saveConfig(r.cfg.ConfigPath, r.cfg); err != nil {
		http.Error(w, fmt.Sprintf("save config failed: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("[jitter] params updated and persisted: enable=%t src_ips_enabled=%t src_ips=%v packet_history_ms=%d min=%d max=%d tick=%d up=%d down=%d log_enabled=%t log_period_ms=%d path=%s",
		r.cfg.EnableJitter, r.cfg.JitterSourceIPsEnabled, r.cfg.JitterSourceIPs, r.cfg.PacketHistoryMS, r.cfg.JitterMinDelayMS, r.cfg.JitterMaxDelayMS, r.cfg.JitterTickMS, r.cfg.JitterUpStepMS, r.cfg.JitterDownStepMS, r.cfg.JitterLogEnabled, r.cfg.JitterLogPeriodMS, r.cfg.ConfigPath)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"saved":   true,
		"path":    r.cfg.ConfigPath,
		"enabled": r.cfg.EnableJitter,
		"jitter": map[string]any{
			"enable_jitter":             r.cfg.EnableJitter,
			"jitter_source_ips_enabled": r.cfg.JitterSourceIPsEnabled,
			"packet_history_ms":         r.cfg.PacketHistoryMS,
			"jitter_buffer_ms":          r.cfg.JitterBufferMS,
			"jitter_min_delay_ms":       r.cfg.JitterMinDelayMS,
			"jitter_max_delay_ms":       r.cfg.JitterMaxDelayMS,
			"jitter_tick_ms":            r.cfg.JitterTickMS,
			"jitter_up_step_ms":         r.cfg.JitterUpStepMS,
			"jitter_down_step_ms":       r.cfg.JitterDownStepMS,
			"jitter_log_enabled":        r.cfg.JitterLogEnabled,
			"jitter_log_period_ms":      r.cfg.JitterLogPeriodMS,
			"jitter_source_ips":         r.cfg.JitterSourceIPs,
		},
	})
}

func (r *Relay) handleUpdatePacerAdaptive(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if req.Method == http.MethodGet {
		out := currentPacerAdaptiveParams(r.cfg)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":             true,
			"pacer_adaptive": out,
			"path":           r.cfg.ConfigPath,
		})
		return
	}

	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body UpdatePacerAdaptiveRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	next := mergePacerAdaptiveParams(currentPacerAdaptiveParams(r.cfg), &body)

	r.pacerCfgMux.Lock()
	r.cfg.PacerAdaptive = next
	r.pacerCfgMux.Unlock()
	r.applyResolvedParamsToAllPacers()

	if err := saveConfigPatch(r.cfg.ConfigPath, map[string]any{
		"pacer_adaptive": pacerAdaptiveToPatchMap(next),
	}); err != nil {
		http.Error(w, fmt.Sprintf("save config failed: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("[pacer-adaptive] params updated and persisted: short=%dms long=%dms headroom(short/long)=%.3f/%.3f reserve(rtx/rtcp)=%.3f/%.3f start=%dbps floor=%dbps queue(target/high/sustain)=%d/%d/%dms boost=%.3f force_cap_qdelay=%dms target_max_up_per_sec=%.3f feedback(enabled)=%t loss(high/mid)=%.3f/%.3f drop(high/mid)=%.3f/%.3f min_scale=%.3f recover(gain/ms/step)=%.3f/%d/%d mid_count=%d path=%s",
		next.ShortEWMAMS, next.LongEWMAMS,
		next.ShortHeadroom, next.LongHeadroom,
		next.RTXReservePct, next.RTCPOverheadPct,
		next.StartRateBps,
		next.FloorRateBps,
		next.QueueDelayTargetMS, next.QueueHighWatermarkMS, next.QueueSustainMS, next.QueueBoostFactor, next.ForceHardCapQueueDelayMS, next.TargetMaxUpPerSec,
		next.FeedbackEnabled,
		next.FeedbackLossHigh, next.FeedbackLossMid,
		next.FeedbackDropHigh, next.FeedbackDropMid, next.FeedbackMinScale,
		next.FeedbackRecoverGain, next.FeedbackRecoverMS, next.FeedbackStepMS, next.FeedbackMidCount,
		r.cfg.ConfigPath,
	)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":             true,
		"saved":          true,
		"path":           r.cfg.ConfigPath,
		"pacer_adaptive": next,
	})
}

func (r *Relay) handleForwardDebugTarget(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if req.Method == http.MethodGet {
		r.forwardDebugMux.Lock()
		ip := r.debugForwardIP
		r.forwardDebugMux.Unlock()
		json.NewEncoder(w).Encode(map[string]any{
			"enabled": ip != "",
			"ip":      ip,
		})
		return
	}
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body ForwardDebugTargetRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	ip := strings.TrimSpace(body.IP)
	if ip != "" && net.ParseIP(ip) == nil {
		http.Error(w, "invalid ip", http.StatusBadRequest)
		return
	}
	r.forwardDebugMux.Lock()
	r.debugForwardIP = ip
	r.forwardDebug = ForwardDebugState{}
	r.forwardDebugMux.Unlock()
	json.NewEncoder(w).Encode(map[string]any{
		"enabled": ip != "",
		"ip":      ip,
	})
}

func (r *Relay) handleUpdateConfig(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if req.Method == http.MethodGet {
		data, err := os.ReadFile(r.cfg.ConfigPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("read config failed: %v", err), http.StatusInternalServerError)
			return
		}
		// Return exactly what is in config file.
		w.Write(data)
		return
	}

	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var patch map[string]any
	if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if len(patch) == 0 {
		http.Error(w, "empty patch", http.StatusBadRequest)
		return
	}

	if err := saveConfigPatch(r.cfg.ConfigPath, patch); err != nil {
		http.Error(w, fmt.Sprintf("save config failed: %v", err), http.StatusInternalServerError)
		return
	}

	cfg, err := loadConfig(r.cfg.ConfigPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("reload config failed: %v", err), http.StatusInternalServerError)
		return
	}
	r.applyConfigToRuntime(cfg)

	keys := make([]string, 0, len(patch))
	for k := range patch {
		keys = append(keys, k)
	}
	log.Printf("[config] patched and reloaded: keys=%v path=%s", keys, r.cfg.ConfigPath)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":    true,
		"saved": true,
		"path":  r.cfg.ConfigPath,
		"keys":  keys,
		"config": map[string]any{
			"listen_rtp":            r.cfg.ListenRTP,
			"listen_rtcp":           r.cfg.ListenRTCP,
			"listen_control":        r.cfg.ListenControl,
			"send_rate_bps":         r.cfg.SendRateBps,
			"enable_pacer":          r.cfg.EnablePacer,
			"enable_pacer_adaptive": r.cfg.EnablePacerAdaptive,
			"pacer_adaptive":        r.cfg.PacerAdaptive,
			"pacer_ip_profiles":     r.cfg.PacerIPProfiles,
			"jitter": map[string]any{
				"enable_jitter":             r.cfg.EnableJitter,
				"jitter_source_ips_enabled": r.cfg.JitterSourceIPsEnabled,
				"packet_history_ms":         r.cfg.PacketHistoryMS,
				"jitter_buffer_ms":          r.cfg.JitterBufferMS,
				"jitter_min_delay_ms":       r.cfg.JitterMinDelayMS,
				"jitter_max_delay_ms":       r.cfg.JitterMaxDelayMS,
				"jitter_tick_ms":            r.cfg.JitterTickMS,
				"jitter_up_step_ms":         r.cfg.JitterUpStepMS,
				"jitter_down_step_ms":       r.cfg.JitterDownStepMS,
				"jitter_log_enabled":        r.cfg.JitterLogEnabled,
				"jitter_log_period_ms":      r.cfg.JitterLogPeriodMS,
				"jitter_source_ips":         r.cfg.JitterSourceIPs,
			},
		},
	})
}

func (r *Relay) handleReplaceConfig(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var full map[string]any
	if err := json.NewDecoder(req.Body).Decode(&full); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if len(full) == 0 {
		http.Error(w, "empty config", http.StatusBadRequest)
		return
	}

	cfg, err := saveConfigReplace(r.cfg.ConfigPath, full)
	if err != nil {
		http.Error(w, fmt.Sprintf("replace config failed: %v", err), http.StatusBadRequest)
		return
	}
	r.applyConfigToRuntime(cfg)

	keys := make([]string, 0, len(full))
	for k := range full {
		keys = append(keys, k)
	}
	log.Printf("[config] replaced and reloaded: keys=%v path=%s", keys, r.cfg.ConfigPath)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":    true,
		"saved": true,
		"mode":  "replace",
		"path":  r.cfg.ConfigPath,
		"keys":  keys,
		"config": map[string]any{
			"listen_rtp":            r.cfg.ListenRTP,
			"listen_rtcp":           r.cfg.ListenRTCP,
			"listen_control":        r.cfg.ListenControl,
			"send_rate_bps":         r.cfg.SendRateBps,
			"enable_pacer":          r.cfg.EnablePacer,
			"enable_pacer_adaptive": r.cfg.EnablePacerAdaptive,
			"pacer_adaptive":        r.cfg.PacerAdaptive,
			"pacer_ip_profiles":     r.cfg.PacerIPProfiles,
			"jitter": map[string]any{
				"enable_jitter":             r.cfg.EnableJitter,
				"jitter_source_ips_enabled": r.cfg.JitterSourceIPsEnabled,
				"packet_history_ms":         r.cfg.PacketHistoryMS,
				"jitter_buffer_ms":          r.cfg.JitterBufferMS,
				"jitter_min_delay_ms":       r.cfg.JitterMinDelayMS,
				"jitter_max_delay_ms":       r.cfg.JitterMaxDelayMS,
				"jitter_tick_ms":            r.cfg.JitterTickMS,
				"jitter_up_step_ms":         r.cfg.JitterUpStepMS,
				"jitter_down_step_ms":       r.cfg.JitterDownStepMS,
				"jitter_log_enabled":        r.cfg.JitterLogEnabled,
				"jitter_log_period_ms":      r.cfg.JitterLogPeriodMS,
				"jitter_source_ips":         r.cfg.JitterSourceIPs,
			},
		},
	})
}

func (r *Relay) handleUpdatePacer(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	mergePacerParams := func(base PacerResolvedParams, body *UpdatePacerRequest) (PacerResolvedParams, error) {
		p := base
		if body.EnablePacer != nil {
			p.EnablePacer = *body.EnablePacer
		}
		if body.EnablePacerAdaptive != nil {
			p.EnablePacerAdaptive = *body.EnablePacerAdaptive
		}
		if body.RateBps != nil {
			p.SendRateBps = *body.RateBps
		} else if body.SendRateBps != nil {
			p.SendRateBps = *body.SendRateBps
		}
		if body.TickMS != nil {
			p.TickMS = *body.TickMS
		}
		if body.LogEnabled != nil {
			p.LogEnabled = *body.LogEnabled
		}
		if body.LogPeriodMS != nil {
			p.LogPeriodMS = *body.LogPeriodMS
		}
		if p.SendRateBps <= 0 {
			return p, fmt.Errorf("invalid rate_bps")
		}
		if p.TickMS <= 0 {
			return p, fmt.Errorf("invalid tick_ms")
		}
		if p.LogPeriodMS < 0 {
			return p, fmt.Errorf("invalid log_period_ms")
		}
		return p, nil
	}

	aggregateRuntime := func() (int, int, int, int, int) {
		r.pacerMux.RLock()
		keys := make([]string, 0, len(r.pacers))
		for key := range r.pacers {
			keys = append(keys, key)
		}
		r.pacerMux.RUnlock()

		runningCount := 0
		totalPackets := 0
		totalBytes := 0
		totalFlows := 0
		for _, key := range keys {
			st, running, exists := r.getPacerStatsByKey(key)
			if !exists {
				continue
			}
			if running {
				runningCount++
			}
			totalPackets += st.QueuePackets
			totalBytes += st.QueueBytes
			totalFlows += st.QueueFlows
		}
		return len(keys), runningCount, totalPackets, totalBytes, totalFlows
	}

	if req.Method == http.MethodGet {
		targetRaw := strings.TrimSpace(req.URL.Query().Get("target"))
		if targetRaw != "" {
			targetKey, err := normalizePacerTargetKey(targetRaw)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			effective := r.resolvePacerParamsForTargetKey(targetKey)
			override, hasOverride := r.getPacerOverride(targetKey)
			st, running, exists := r.getPacerStatsByKey(targetKey)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":              true,
				"mode":            "per-target",
				"target":          targetKey,
				"override_exists": hasOverride,
				"override":        override,
				"effective":       effective,
				"pacer_exists":    exists,
				"pacer_running":   running,
				"queue_packets":   st.QueuePackets,
				"queue_bytes":     st.QueueBytes,
				"queue_flows":     st.QueueFlows,
				"path":            r.cfg.ConfigPath,
			})
			return
		}

		defaults := r.resolvePacerParamsForTargetKey("")
		overrides := r.snapshotPacerOverrides()
		activeCount, runningCount, queuePackets, queueBytes, queueFlows := aggregateRuntime()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":                    true,
			"mode":                  "per-target",
			"default":               defaults,
			"enable_pacer":          defaults.EnablePacer,
			"enable_pacer_adaptive": defaults.EnablePacerAdaptive,
			"rate_bps":              defaults.SendRateBps,
			"target_overrides":      overrides,
			"active_pacers":         activeCount,
			"running_pacers":        runningCount,
			"queue_packets":         queuePackets,
			"queue_bytes":           queueBytes,
			"queue_flows":           queueFlows,
			"path":                  r.cfg.ConfigPath,
		})
		return
	}

	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body UpdatePacerRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	targetRaw := strings.TrimSpace(body.Target)
	if targetRaw != "" {
		targetKey, err := normalizePacerTargetKey(targetRaw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if body.EnablePacerAdaptive != nil {
			http.Error(w, "enable_pacer_adaptive requires global default update (without target)", http.StatusBadRequest)
			return
		}

		if body.DeleteTarget != nil && *body.DeleteTarget {
			r.deletePacerOverride(targetKey)
			effective := r.resolvePacerParamsForTargetKey(targetKey)
			r.applyResolvedParamsToPacer(targetKey, effective)

			st, running, exists := r.getPacerStatsByKey(targetKey)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":              true,
				"saved":           false,
				"in_memory_only":  true,
				"path":            r.cfg.ConfigPath,
				"target":          targetKey,
				"deleted":         true,
				"override_exists": false,
				"effective":       effective,
				"pacer_exists":    exists,
				"pacer_running":   running,
				"queue_packets":   st.QueuePackets,
				"queue_bytes":     st.QueueBytes,
				"queue_flows":     st.QueueFlows,
			})
			return
		}

		base := r.resolvePacerParamsForTargetKey(targetKey)
		next, err := mergePacerParams(base, &body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		override := resolvedToOverride(next)
		if err := validatePacerTargetConfig(targetKey, override); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.setPacerOverride(targetKey, override)
		if addr, err := net.ResolveUDPAddr("udp", targetKey); err == nil {
			if _, _, err := r.getOrCreatePacerForTarget(addr); err != nil {
				http.Error(w, fmt.Sprintf("create pacer failed: %v", err), http.StatusInternalServerError)
				return
			}
		}
		r.applyResolvedParamsToPacer(targetKey, next)

		log.Printf("[pacer] target updated in-memory: target=%s enable=%t rate=%dbps tick=%dms log=%t/%dms",
			targetKey, next.EnablePacer, next.SendRateBps, next.TickMS, next.LogEnabled, next.LogPeriodMS)

		st, running, exists := r.getPacerStatsByKey(targetKey)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":              true,
			"saved":           false,
			"in_memory_only":  true,
			"path":            r.cfg.ConfigPath,
			"target":          targetKey,
			"override_exists": true,
			"effective":       next,
			"pacer_exists":    exists,
			"pacer_running":   running,
			"queue_packets":   st.QueuePackets,
			"queue_bytes":     st.QueueBytes,
			"queue_flows":     st.QueueFlows,
		})
		return
	}

	if body.TickMS != nil || body.LogEnabled != nil || body.LogPeriodMS != nil || (body.DeleteTarget != nil && *body.DeleteTarget) {
		http.Error(w, "tick/log/delete_target requires target", http.StatusBadRequest)
		return
	}

	base := r.resolvePacerParamsForTargetKey("")
	next, err := mergePacerParams(base, &body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	r.setGlobalPacerDefaults(next.EnablePacer, next.EnablePacerAdaptive, next.SendRateBps)
	// sendLimiter is disabled; pacing is handled by per-target pacers.
	r.applyResolvedParamsToAllPacers()

	if err := saveConfigPatch(r.cfg.ConfigPath, map[string]any{
		"enable_pacer":          next.EnablePacer,
		"enable_pacer_adaptive": next.EnablePacerAdaptive,
		"send_rate_bps":         next.SendRateBps,
	}); err != nil {
		http.Error(w, fmt.Sprintf("save config failed: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("[pacer] default updated and persisted: enable=%t adaptive=%t rate=%dbps path=%s", next.EnablePacer, next.EnablePacerAdaptive, next.SendRateBps, r.cfg.ConfigPath)
	activeCount, runningCount, queuePackets, queueBytes, queueFlows := aggregateRuntime()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":                    true,
		"saved":                 true,
		"path":                  r.cfg.ConfigPath,
		"mode":                  "per-target",
		"default":               next,
		"enable_pacer":          next.EnablePacer,
		"enable_pacer_adaptive": next.EnablePacerAdaptive,
		"rate_bps":              next.SendRateBps,
		"active_pacers":         activeCount,
		"running_pacers":        runningCount,
		"queue_packets":         queuePackets,
		"queue_bytes":           queueBytes,
		"queue_flows":           queueFlows,
	})
}

func (r *Relay) handleUpdatePacerIPProfile(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if req.Method == http.MethodGet {
		ipRaw := strings.TrimSpace(req.URL.Query().Get("ip"))
		if ipRaw != "" {
			ip, err := normalizePacerProfileIP(ipRaw)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			profile, exists := r.getPacerIPProfile(ip)
			enableDefault, rateDefault := r.getGlobalPacerDefaults()
			effective := PacerIPProfile{
				EnablePacer: enableDefault,
				SendRateBps: rateDefault,
			}
			if exists {
				effective = profile
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":             true,
				"ip":             ip,
				"profile_exists": exists,
				"profile":        profile,
				"effective":      effective,
				"path":           r.cfg.ConfigPath,
			})
			return
		}
		profiles := r.snapshotPacerIPProfiles()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"profiles": profiles,
			"count":    len(profiles),
			"path":     r.cfg.ConfigPath,
		})
		return
	}

	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body UpdatePacerIPProfileRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	ip, err := normalizePacerProfileIP(body.IP)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.Delete != nil && *body.Delete {
		r.deletePacerIPProfile(ip)
		r.applyPacerIPProfileToExistingPacers(ip)
		if err := saveConfigPatch(r.cfg.ConfigPath, map[string]any{
			"pacer_ip_profiles": r.snapshotPacerIPProfiles(),
		}); err != nil {
			http.Error(w, fmt.Sprintf("save config failed: %v", err), http.StatusInternalServerError)
			return
		}
		enableDefault, rateDefault := r.getGlobalPacerDefaults()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"saved":   true,
			"deleted": true,
			"ip":      ip,
			"effective": PacerIPProfile{
				EnablePacer: enableDefault,
				SendRateBps: rateDefault,
			},
			"path": r.cfg.ConfigPath,
		})
		return
	}

	enableDefault, rateDefault := r.getGlobalPacerDefaults()
	next := PacerIPProfile{
		EnablePacer: enableDefault,
		SendRateBps: rateDefault,
	}
	if existing, ok := r.getPacerIPProfile(ip); ok {
		next = existing
	}
	if body.EnablePacer != nil {
		next.EnablePacer = *body.EnablePacer
	}
	if body.RateBps != nil {
		next.SendRateBps = *body.RateBps
	} else if body.SendRateBps != nil {
		next.SendRateBps = *body.SendRateBps
	}
	if err := validatePacerIPProfile(ip, next); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	r.setPacerIPProfile(ip, next)
	r.applyPacerIPProfileToExistingPacers(ip)

	if err := saveConfigPatch(r.cfg.ConfigPath, map[string]any{
		"pacer_ip_profiles": r.snapshotPacerIPProfiles(),
	}); err != nil {
		http.Error(w, fmt.Sprintf("save config failed: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("[pacer] ip profile updated and persisted: ip=%s enable=%t rate=%dbps path=%s",
		ip, next.EnablePacer, next.SendRateBps, r.cfg.ConfigPath)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"saved":   true,
		"ip":      ip,
		"profile": next,
		"path":    r.cfg.ConfigPath,
	})
}

func (r *Relay) handleUpdateMapping(w http.ResponseWriter, req *http.Request) {
	var m ControlMapping
	if err := json.NewDecoder(req.Body).Decode(&m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	r.mappingsMux.Lock()
	r.mappings[m.SrcSSRC] = &m
	r.mappingsMux.Unlock()

	r.AddTarget(m.SrcSSRC, &net.UDPAddr{
		IP:   net.ParseIP(m.DstIP),
		Port: m.DstPort,
	})

	log.Printf("new mappings Targets: %d To %s:%d\n", m.SrcSSRC, m.DstIP, m.DstPort)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (r *Relay) handleUpdateAddrMapping(w http.ResponseWriter, req *http.Request) {
	var body UpdateAddrMappingRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON", 400)
		return
	}

	addr, err := net.ResolveUDPAddr("udp",
		fmt.Sprintf("%s:%d", body.TargetIP, body.TargetPort))
	if err != nil {
		http.Error(w, "Bad IP/Port", 400)
		return
	}

	r.AddAddrTarget(body.Key, addr)

	log.Printf("new addr mappings Targets: %s To %s:%d\n", body.Key, body.TargetIP, body.TargetPort)

	w.Write([]byte("OK"))
}

func (r *Relay) handleRemoveAddrMapping(w http.ResponseWriter, req *http.Request) {
	var body RemoveMappingRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON", 400)
		return
	}

	r.RemoveAddrTarget(body.Key)

	w.Write([]byte("REMOVED"))
}

func (r *Relay) handleDebugMappings(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	fmt.Fprintln(w, "=== mapping (SSRC → []*UDPAddr) ===")
	r.mapMutex.RLock()
	for ssrc, targets := range r.mapping {
		fmt.Fprintf(w, "  SSRC %d → ", ssrc)
		for _, t := range targets {
			fmt.Fprintf(w, "%s ", t.String())
		}
		fmt.Fprintln(w)
	}
	r.mapMutex.RUnlock()

	fmt.Fprintln(w)

	fmt.Fprintln(w, "=== addrMapping (srcAddr → []*UDPAddr) ===")
	r.addrMutex.RLock()
	for addr, targets := range r.addrMapping {
		fmt.Fprintf(w, "  %s → ", addr)
		for _, t := range targets {
			fmt.Fprintf(w, "%s ", t.String())
		}
		fmt.Fprintln(w)
	}
	r.addrMutex.RUnlock()

	fmt.Fprintln(w)

	fmt.Fprintln(w, "=== addrToSSRC (srcAddr → SSRC) ===")
	r.addrSsrcMutex.RLock()
	for addr, ssrc := range r.addrSsrc {
		fmt.Fprintf(w, "  %s → %v\n", addr, ssrc)
	}
	r.addrSsrcMutex.RUnlock()

	fmt.Fprintln(w)

	fmt.Fprintln(w, "=== mappings (ControlMapping) ===")
	r.mappingsMux.RLock()
	for ssrc, m := range r.mappings {
		fmt.Fprintf(w, "  SSRC %d → %+v\n", ssrc, *m)
	}
	r.mappingsMux.RUnlock()

	fmt.Fprintln(w, "=== END ===")
}

func (r *Relay) handleDebugMappingJSON(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	type jsonOut struct {
		Mapping         map[uint32][]string        `json:"mapping"`
		AddrMapping     map[string][]string        `json:"addrMapping"`
		AddrSsrc        map[string][]uint32        `json:"addrSsrc"`
		SsrcDirect      map[uint32]uint32          `json:"ssrcDirectMapping"`
		ControlMappings map[uint32]*ControlMapping `json:"controlMappings"`
	}

	out := jsonOut{
		Mapping:         map[uint32][]string{},
		AddrMapping:     map[string][]string{},
		AddrSsrc:        map[string][]uint32{},
		SsrcDirect:      map[uint32]uint32{},
		ControlMappings: map[uint32]*ControlMapping{},
	}

	r.mapMutex.RLock()
	for ssrc, addrs := range r.mapping {
		for _, a := range addrs {
			out.Mapping[ssrc] = append(out.Mapping[ssrc], a.String())
		}
	}
	r.mapMutex.RUnlock()

	r.addrMutex.RLock()
	for addr, targets := range r.addrMapping {
		for _, t := range targets {
			out.AddrMapping[addr] = append(out.AddrMapping[addr], t.String())
		}
	}
	r.addrMutex.RUnlock()

	r.addrSsrcMutex.RLock()
	for addr, ssrc := range r.addrSsrc {
		out.AddrSsrc[addr] = ssrc
	}
	r.addrSsrcMutex.RUnlock()

	r.ssrcDirectMux.RLock()
	for ssrc, m := range r.ssrcDirectMapping {
		out.SsrcDirect[ssrc] = m
	}
	r.ssrcDirectMux.RUnlock()

	r.mappingsMux.RLock()
	for ssrc, m := range r.mappings {
		out.ControlMappings[ssrc] = m
	}
	r.mappingsMux.RUnlock()

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}

func (r *Relay) handleDebugMappingGraph(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	var sb strings.Builder
	sb.WriteString("RTP Relay Mapping Graph\n")
	sb.WriteString("====================================\n\n")

	r.mapMutex.RLock()
	mappingCopy := make(map[uint32][]*net.UDPAddr)
	for ssrc, v := range r.mapping {
		mappingCopy[ssrc] = append([]*net.UDPAddr{}, v...)
	}
	r.mapMutex.RUnlock()

	r.addrMutex.RLock()
	addrMapCopy := make(map[string][]*net.UDPAddr)
	for k, v := range r.addrMapping {
		addrMapCopy[k] = append([]*net.UDPAddr{}, v...)
	}
	r.addrMutex.RUnlock()

	r.addrSsrcMutex.RLock()
	addrToSsrcCopy := make(map[string][]uint32)
	for k, v := range r.addrSsrc {
		addrToSsrcCopy[k] = v
	}
	r.addrSsrcMutex.RUnlock()

	r.ssrcDirectMux.RLock()
	ssrcDirectCopy := make(map[uint32]uint32)
	for ssrc, addr := range r.ssrcDirectMapping {
		ssrcDirectCopy[ssrc] = addr
	}
	r.ssrcDirectMux.RUnlock()

	for addr, ssrcs := range addrToSsrcCopy {
		for _, ssrc := range ssrcs {
			sb.WriteString(fmt.Sprintf("Source %s\n", addr))
			sb.WriteString(fmt.Sprintf("   └─ SSRC: %d\n", ssrc))

			if direct, ok := ssrcDirectCopy[ssrc]; ok {
				sb.WriteString(fmt.Sprintf("       └─ Direct From: %v\n", direct))
			}

			targets := mappingCopy[ssrc]
			for _, t := range targets {
				sb.WriteString(fmt.Sprintf("           └─→ Target: %s\n", t.String()))
			}

			sb.WriteString("\n")
		}
	}

	w.Write([]byte(sb.String()))
}

func (r *Relay) handleNetworkDebug(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	n := 10
	if v := req.URL.Query().Get("count"); v != "" {
		if iv, err := strconv.Atoi(v); err == nil && iv > 0 {
			n = iv
		}
	}

	out := r.weakStats.Export(n)

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}

func (r *Relay) handleNetworkUpdatePeriods(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	n := 1
	if v := req.URL.Query().Get("periods"); v != "" {
		if iv, err := strconv.Atoi(v); err == nil && iv > 0 {
			n = iv
		}
	}

	r.weakStats.updatePeriod(time.Duration(n) * time.Second)

	w.Write([]byte("OK"))
}

func handleExit(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("exiting service"))
	go func() {
		os.Exit(0)
	}()
}

func handleVersion(w http.ResponseWriter, req *http.Request) {
	resp := struct {
		Version string `json:"version"`
	}{
		Version: version,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (r *Relay) controlLoop() {
	http.HandleFunc("/update-mapping", r.handleUpdateMapping)
	http.HandleFunc("/update-addrmapping", r.handleUpdateAddrMapping)
	http.HandleFunc("/update/jitter", r.handleUpdateJitterParams)
	http.HandleFunc("/update/pacer", r.handleUpdatePacer)
	http.HandleFunc("/update/pacer/adaptive", r.handleUpdatePacerAdaptive)
	http.HandleFunc("/update/pacer/ip", r.handleUpdatePacerIPProfile)
	http.HandleFunc("/update/config", r.handleUpdateConfig)
	http.HandleFunc("/update/config/replace", r.handleReplaceConfig)
	http.HandleFunc("/remove-addrmapping", r.handleRemoveAddrMapping)
	http.HandleFunc("/debug/mappings", r.handleDebugMappings)
	http.HandleFunc("/debug/mapping/json", r.handleDebugMappingJSON)
	http.HandleFunc("/debug/mapping/graph", r.handleDebugMappingGraph)
	http.HandleFunc("/update/install", handleInstallUpdate)
	http.HandleFunc("/debug/network", r.handleNetworkDebug)
	http.HandleFunc("/debug/network/periods", r.handleNetworkUpdatePeriods)
	http.HandleFunc("/debug/network/page", r.handleNetworkPage)
	http.HandleFunc("/debug/stat/print", r.handleStatPrintPage)
	http.HandleFunc("/debug/stat/print/data", r.handleStatPrintData)
	http.HandleFunc("/debug/forward/target", r.handleForwardDebugTarget)
	http.HandleFunc("/debug/pcap/start", r.handlePcapStart)
	http.HandleFunc("/debug/pcap/stop", r.handlePcapStop)
	http.HandleFunc("/debug/pcap/status", r.handlePcapStatus)
	http.HandleFunc("/debug/pcap/download", r.handlePcapDownload)
	http.HandleFunc("/update/exit", handleExit)
	http.HandleFunc("/update/version", handleVersion)

	fmt.Println("ControlLoop listening on", r.cfg.ListenControl)
	go func() {
		if err := http.ListenAndServe(r.cfg.ListenControl, nil); err != nil {
			panic(err)
		}
	}()
}
