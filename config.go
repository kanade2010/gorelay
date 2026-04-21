package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

type ControlMapping struct {
	SrcSSRC     uint32 `json:"src_ssrc"`
	DstIP       string `json:"dst_ip"`
	DstPort     int    `json:"dst_port"`
	DstRtcpPort int    `json:"dst_rtcp_port"`
}

type Mapping struct {
	SrcSSRC uint32   `json:"src_ssrc"`
	Targets []string `json:"targets"`
}

type PacerTargetConfig struct {
	EnablePacer *bool `json:"enable_pacer,omitempty"`
	SendRateBps *int  `json:"send_rate_bps,omitempty"`
	TickMS      *int  `json:"tick_ms,omitempty"`
	LogEnabled  *bool `json:"log_enabled,omitempty"`
	LogPeriodMS *int  `json:"log_period_ms,omitempty"`
}

type PacerIPProfile struct {
	EnablePacer bool `json:"enable_pacer"`
	SendRateBps int  `json:"send_rate_bps"`
}

type PacerAdaptiveConfig struct {
	ShortEWMAMS              int     `json:"short_ewma_ms"`
	LongEWMAMS               int     `json:"long_ewma_ms"`
	ShortHeadroom            float64 `json:"short_headroom"`
	LongHeadroom             float64 `json:"long_headroom"`
	RTXReservePct            float64 `json:"rtx_reserve_pct"`
	RTCPOverheadPct          float64 `json:"rtcp_overhead_pct"`
	StartRateBps             int     `json:"start_rate_bps"`
	FloorRateBps             int     `json:"floor_rate_bps"`
	QueueDelayTargetMS       int     `json:"queue_delay_target_ms"`
	QueueHighWatermarkMS     int     `json:"queue_high_watermark_ms"`
	QueueSustainMS           int     `json:"queue_sustain_ms"`
	QueueBoostFactor         float64 `json:"queue_boost_factor"`
	ForceHardCapQueueDelayMS int     `json:"force_hard_cap_queue_delay_ms"`
	TargetMaxUpPerSec        float64 `json:"target_max_up_per_sec"`
	FeedbackLossHigh         float64 `json:"feedback_loss_high"`
	FeedbackLossMid          float64 `json:"feedback_loss_mid"`
	FeedbackDropHigh         float64 `json:"feedback_drop_high"`
	FeedbackDropMid          float64 `json:"feedback_drop_mid"`
	FeedbackMinScale         float64 `json:"feedback_min_scale"`
	FeedbackRecoverGain      float64 `json:"feedback_recover_gain"`
	FeedbackRecoverMS        int     `json:"feedback_recover_ms"`
	FeedbackStepMS           int     `json:"feedback_step_ms"`
	FeedbackMidCount         int     `json:"feedback_mid_count"`
	FeedbackEnabled          bool    `json:"feedback_enabled"`
}

type JitterConfig struct {
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

type Config struct {
	ListenRTP           string                    `json:"listen_rtp"`
	ListenRTCP          string                    `json:"listen_rtcp"`
	ListenControl       string                    `json:"listen_control"`
	Mappings            []Mapping                 `json:"mappings"`
	SendRateBps         int                       `json:"send_rate_bps"`
	EnablePacer         bool                      `json:"enable_pacer"`
	EnablePacerAdaptive bool                      `json:"enable_pacer_adaptive"`
	PacerAdaptive       PacerAdaptiveConfig       `json:"pacer_adaptive,omitempty"`
	PacerIPProfiles     map[string]PacerIPProfile `json:"pacer_ip_profiles,omitempty"`
	Jitter              JitterConfig              `json:"jitter,omitempty"`
	// Runtime jitter fields (kept for minimal code churn in other modules).
	PacketHistoryMS        int      `json:"-"`
	JitterBufferMS         int      `json:"-"`
	JitterMinDelayMS       int      `json:"-"`
	JitterMaxDelayMS       int      `json:"-"`
	JitterTickMS           int      `json:"-"`
	JitterUpStepMS         int      `json:"-"`
	JitterDownStepMS       int      `json:"-"`
	JitterLogEnabled       bool     `json:"-"`
	JitterLogPeriodMS      int      `json:"-"`
	JitterSourceIPsEnabled bool     `json:"-"`
	JitterSourceIPs        []string `json:"-"`
	EnableJitter           bool     `json:"-"`
	ConfigPath             string   `json:"-"`
}

func normalizeJitterSourceIPs(in []string) ([]string, error) {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		ipText := strings.TrimSpace(raw)
		if ipText == "" {
			continue
		}
		ip := net.ParseIP(ipText)
		if ip == nil {
			return nil, fmt.Errorf("invalid ip: %s", ipText)
		}
		key := ip.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out, nil
}

func normalizePacerTargetKey(raw string) (string, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", fmt.Errorf("empty pacer target")
	}
	addr, err := net.ResolveUDPAddr("udp", text)
	if err != nil {
		return "", fmt.Errorf("invalid pacer target %s: %w", text, err)
	}
	if addr == nil || addr.IP == nil || addr.Port <= 0 {
		return "", fmt.Errorf("invalid pacer target %s", text)
	}
	return addr.String(), nil
}

func validatePacerTargetConfig(target string, cfg PacerTargetConfig) error {
	if cfg.SendRateBps != nil && *cfg.SendRateBps <= 0 {
		return fmt.Errorf("invalid send_rate_bps for %s", target)
	}
	if cfg.TickMS != nil && *cfg.TickMS <= 0 {
		return fmt.Errorf("invalid tick_ms for %s", target)
	}
	if cfg.LogPeriodMS != nil && *cfg.LogPeriodMS < 0 {
		return fmt.Errorf("invalid log_period_ms for %s", target)
	}
	return nil
}

func clonePacerTargetConfig(in PacerTargetConfig) PacerTargetConfig {
	var out PacerTargetConfig
	if in.EnablePacer != nil {
		v := *in.EnablePacer
		out.EnablePacer = &v
	}
	if in.SendRateBps != nil {
		v := *in.SendRateBps
		out.SendRateBps = &v
	}
	if in.TickMS != nil {
		v := *in.TickMS
		out.TickMS = &v
	}
	if in.LogEnabled != nil {
		v := *in.LogEnabled
		out.LogEnabled = &v
	}
	if in.LogPeriodMS != nil {
		v := *in.LogPeriodMS
		out.LogPeriodMS = &v
	}
	return out
}

func normalizePacerProfileIP(raw string) (string, error) {
	ipText := strings.TrimSpace(raw)
	if ipText == "" {
		return "", fmt.Errorf("empty pacer ip")
	}
	ip := net.ParseIP(ipText)
	if ip == nil {
		return "", fmt.Errorf("invalid pacer ip: %s", ipText)
	}
	return ip.String(), nil
}

func validatePacerIPProfile(ip string, p PacerIPProfile) error {
	if p.SendRateBps <= 0 {
		return fmt.Errorf("invalid send_rate_bps for pacer ip %s", ip)
	}
	return nil
}

func normalizePacerAdaptiveConfig(p *PacerAdaptiveConfig) {
	if p.ShortEWMAMS <= 0 {
		p.ShortEWMAMS = 250
	}
	if p.LongEWMAMS <= 0 {
		p.LongEWMAMS = 1500
	}
	if p.LongEWMAMS < p.ShortEWMAMS {
		p.LongEWMAMS = p.ShortEWMAMS * 2
	}
	if p.ShortHeadroom <= 0 {
		p.ShortHeadroom = 1.15
	}
	if p.ShortHeadroom < 1 {
		p.ShortHeadroom = 1
	}
	if p.LongHeadroom <= 0 {
		p.LongHeadroom = 1.05
	}
	if p.LongHeadroom < 1 {
		p.LongHeadroom = 1
	}
	if p.RTXReservePct < 0 {
		p.RTXReservePct = 0
	}
	if p.RTXReservePct > 0.5 {
		p.RTXReservePct = 0.5
	}
	if p.RTCPOverheadPct <= 0 {
		p.RTCPOverheadPct = 0.04
	}
	if p.RTCPOverheadPct > 0.2 {
		p.RTCPOverheadPct = 0.2
	}
	if p.StartRateBps < 0 {
		p.StartRateBps = 0
	}
	if p.FloorRateBps <= 0 {
		p.FloorRateBps = 300_000
	}
	if p.QueueDelayTargetMS <= 0 {
		p.QueueDelayTargetMS = 100
	}
	if p.QueueHighWatermarkMS <= 0 {
		p.QueueHighWatermarkMS = p.QueueDelayTargetMS * 2
	}
	if p.QueueHighWatermarkMS < p.QueueDelayTargetMS {
		p.QueueHighWatermarkMS = p.QueueDelayTargetMS
	}
	if p.QueueSustainMS <= 0 {
		p.QueueSustainMS = 1200
	}
	if p.QueueBoostFactor <= 1 {
		p.QueueBoostFactor = 1.15
	}
	if p.ForceHardCapQueueDelayMS <= 0 {
		p.ForceHardCapQueueDelayMS = 1200
	}
	if p.TargetMaxUpPerSec <= 1 {
		p.TargetMaxUpPerSec = 1.20
	}
	if p.FeedbackLossHigh <= 0 || p.FeedbackLossHigh >= 1 {
		p.FeedbackLossHigh = 0.10
	}
	if p.FeedbackLossMid <= 0 || p.FeedbackLossMid >= p.FeedbackLossHigh {
		p.FeedbackLossMid = 0.05
	}
	if p.FeedbackDropHigh <= 0 || p.FeedbackDropHigh > 1 {
		p.FeedbackDropHigh = 0.70
	}
	if p.FeedbackDropMid <= 0 || p.FeedbackDropMid > 1 {
		p.FeedbackDropMid = 0.85
	}
	if p.FeedbackMinScale <= 0 || p.FeedbackMinScale > 1 {
		p.FeedbackMinScale = 0.30
	}
	if p.FeedbackRecoverGain <= 1 || p.FeedbackRecoverGain > 1.5 {
		p.FeedbackRecoverGain = 1.04
	}
	if p.FeedbackRecoverMS <= 0 {
		p.FeedbackRecoverMS = 8000
	}
	if p.FeedbackStepMS <= 0 {
		p.FeedbackStepMS = 2000
	}
	if p.FeedbackMidCount <= 0 {
		p.FeedbackMidCount = 2
	}
}

var legacyJitterKeys = []string{
	"enable_jitter",
	"jitter_source_ips_enabled",
	"packet_history_ms",
	"jitter_buffer_ms",
	"jitter_min_delay_ms",
	"jitter_max_delay_ms",
	"jitter_tick_ms",
	"jitter_up_step_ms",
	"jitter_down_step_ms",
	"jitter_log_enabled",
	"jitter_log_period_ms",
	"jitter_source_ips",
}

func hasKeyRaw(m map[string]json.RawMessage, key string) bool {
	if m == nil {
		return false
	}
	_, ok := m[key]
	return ok
}

func decodeJitterRaw(raw map[string]json.RawMessage) (map[string]json.RawMessage, bool) {
	if raw == nil {
		return nil, false
	}
	jb, ok := raw["jitter"]
	if !ok || len(jb) == 0 {
		return nil, false
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(jb, &out); err != nil {
		return nil, false
	}
	return out, true
}

func decodePacerAdaptiveRaw(raw map[string]json.RawMessage) (map[string]json.RawMessage, bool) {
	if raw == nil {
		return nil, false
	}
	pb, ok := raw["pacer_adaptive"]
	if !ok || len(pb) == 0 {
		return nil, false
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(pb, &out); err != nil {
		return nil, false
	}
	return out, true
}

func syncRuntimeFromJitterStruct(cfg *Config) {
	cfg.EnableJitter = cfg.Jitter.EnableJitter
	cfg.JitterSourceIPsEnabled = cfg.Jitter.JitterSourceIPsEnabled
	cfg.PacketHistoryMS = cfg.Jitter.PacketHistoryMS
	cfg.JitterBufferMS = cfg.Jitter.JitterBufferMS
	cfg.JitterMinDelayMS = cfg.Jitter.JitterMinDelayMS
	cfg.JitterMaxDelayMS = cfg.Jitter.JitterMaxDelayMS
	cfg.JitterTickMS = cfg.Jitter.JitterTickMS
	cfg.JitterUpStepMS = cfg.Jitter.JitterUpStepMS
	cfg.JitterDownStepMS = cfg.Jitter.JitterDownStepMS
	cfg.JitterLogEnabled = cfg.Jitter.JitterLogEnabled
	cfg.JitterLogPeriodMS = cfg.Jitter.JitterLogPeriodMS
	cfg.JitterSourceIPs = append([]string(nil), cfg.Jitter.JitterSourceIPs...)
}

func syncJitterStructFromRuntime(cfg *Config) {
	cfg.Jitter = JitterConfig{
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
}

func normalizeRawConfigJitterShape(raw map[string]any) {
	if raw == nil {
		return
	}

	jitterAny, _ := raw["jitter"]
	jitter, _ := jitterAny.(map[string]any)
	if jitter == nil {
		jitter = make(map[string]any)
	}

	for _, key := range legacyJitterKeys {
		if v, ok := raw[key]; ok {
			if _, exists := jitter[key]; !exists {
				jitter[key] = v
			}
			delete(raw, key)
		}
	}

	if len(jitter) > 0 {
		raw["jitter"] = jitter
	} else {
		delete(raw, "jitter")
	}
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse config raw: %w", err)
	}

	jitterRaw, hasNestedJitter := decodeJitterRaw(raw)
	pacerAdaptiveRaw, hasNestedPacerAdaptive := decodePacerAdaptiveRaw(raw)
	if hasNestedJitter {
		syncRuntimeFromJitterStruct(&cfg)
	} else {
		var legacy struct {
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
		if err := json.Unmarshal(data, &legacy); err != nil {
			return nil, fmt.Errorf("parse legacy jitter config: %w", err)
		}
		cfg.EnableJitter = legacy.EnableJitter
		cfg.JitterSourceIPsEnabled = legacy.JitterSourceIPsEnabled
		cfg.PacketHistoryMS = legacy.PacketHistoryMS
		cfg.JitterBufferMS = legacy.JitterBufferMS
		cfg.JitterMinDelayMS = legacy.JitterMinDelayMS
		cfg.JitterMaxDelayMS = legacy.JitterMaxDelayMS
		cfg.JitterTickMS = legacy.JitterTickMS
		cfg.JitterUpStepMS = legacy.JitterUpStepMS
		cfg.JitterDownStepMS = legacy.JitterDownStepMS
		cfg.JitterLogEnabled = legacy.JitterLogEnabled
		cfg.JitterLogPeriodMS = legacy.JitterLogPeriodMS
		cfg.JitterSourceIPs = append([]string(nil), legacy.JitterSourceIPs...)
	}

	if cfg.ListenControl == "" {
		cfg.ListenControl = ":8080"
	}
	if cfg.PacketHistoryMS <= 0 {
		cfg.PacketHistoryMS = 1500
	}
	if cfg.JitterBufferMS <= 0 {
		cfg.JitterBufferMS = 200
	}
	if cfg.JitterMinDelayMS <= 0 {
		cfg.JitterMinDelayMS = 20
	}
	if cfg.JitterMaxDelayMS <= 0 {
		cfg.JitterMaxDelayMS = cfg.JitterBufferMS
	}
	if cfg.JitterMaxDelayMS < cfg.JitterMinDelayMS {
		cfg.JitterMaxDelayMS = cfg.JitterMinDelayMS
	}
	if cfg.JitterTickMS <= 0 {
		cfg.JitterTickMS = 10
	}
	if cfg.JitterUpStepMS <= 0 {
		cfg.JitterUpStepMS = 20
	}
	if cfg.JitterDownStepMS <= 0 {
		cfg.JitterDownStepMS = 5
	}
	if hasNestedJitter {
		if !hasKeyRaw(jitterRaw, "jitter_log_enabled") {
			cfg.JitterLogEnabled = true
		}
		if !hasKeyRaw(jitterRaw, "jitter_log_period_ms") {
			cfg.JitterLogPeriodMS = 2000
		}
	} else {
		if _, ok := raw["jitter_log_enabled"]; !ok {
			cfg.JitterLogEnabled = true
		}
		if _, ok := raw["jitter_log_period_ms"]; !ok {
			cfg.JitterLogPeriodMS = 2000
		}
	}
	if _, ok := raw["enable_pacer"]; !ok {
		cfg.EnablePacer = true
	}
	if _, ok := raw["enable_pacer_adaptive"]; !ok {
		cfg.EnablePacerAdaptive = true
	}
	if hasNestedPacerAdaptive {
		if !hasKeyRaw(pacerAdaptiveRaw, "feedback_enabled") {
			cfg.PacerAdaptive.FeedbackEnabled = true
		}
	} else {
		cfg.PacerAdaptive.FeedbackEnabled = true
	}
	normalizePacerAdaptiveConfig(&cfg.PacerAdaptive)
	if cfg.SendRateBps <= 0 {
		cfg.SendRateBps = 20000000
	}
	if cfg.JitterLogPeriodMS < 0 {
		cfg.JitterLogPeriodMS = 0
	}
	if cfg.JitterSourceIPs == nil {
		cfg.JitterSourceIPs = []string{}
	}
	ips, err := normalizeJitterSourceIPs(cfg.JitterSourceIPs)
	if err != nil {
		return nil, fmt.Errorf("parse jitter_source_ips: %w", err)
	}
	cfg.JitterSourceIPs = ips
	syncJitterStructFromRuntime(&cfg)
	if cfg.PacerIPProfiles == nil {
		cfg.PacerIPProfiles = make(map[string]PacerIPProfile)
	} else {
		normalizedProfiles := make(map[string]PacerIPProfile, len(cfg.PacerIPProfiles))
		for rawIP, profile := range cfg.PacerIPProfiles {
			ip, err := normalizePacerProfileIP(rawIP)
			if err != nil {
				return nil, err
			}
			if err := validatePacerIPProfile(ip, profile); err != nil {
				return nil, err
			}
			normalizedProfiles[ip] = profile
		}
		cfg.PacerIPProfiles = normalizedProfiles
	}
	cfg.ConfigPath = path
	return &cfg, nil
}

func saveConfig(path string, cfg *Config) error {
	if path == "" {
		return fmt.Errorf("empty config path")
	}
	if cfg == nil {
		return fmt.Errorf("nil config")
	}

	syncJitterStructFromRuntime(cfg)

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	data = append(data, '\n')

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

func saveConfigPatch(path string, patch map[string]any) error {
	if path == "" {
		return fmt.Errorf("empty config path")
	}
	if len(patch) == 0 {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("open config: %w", err)
	}

	var raw map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("parse config raw: %w", err)
		}
	}
	if raw == nil {
		raw = make(map[string]any)
	}

	jitterAny, _ := raw["jitter"]
	jitter, _ := jitterAny.(map[string]any)
	if jitter == nil {
		jitter = make(map[string]any)
	}
	jitterProvided := false
	for key, value := range patch {
		if key == "jitter" {
			jitterProvided = true
			if vmap, ok := value.(map[string]any); ok {
				jitter = vmap
			} else {
				b, err := json.Marshal(value)
				if err != nil {
					return fmt.Errorf("marshal jitter patch: %w", err)
				}
				var parsed map[string]any
				if err := json.Unmarshal(b, &parsed); err != nil {
					return fmt.Errorf("invalid jitter patch: %w", err)
				}
				jitter = parsed
			}
			continue
		}
		isLegacy := false
		for _, jk := range legacyJitterKeys {
			if key == jk {
				jitter[key] = value
				isLegacy = true
				break
			}
		}
		if isLegacy {
			continue
		}
		raw[key] = value
	}
	if len(jitter) > 0 || jitterProvided {
		raw["jitter"] = jitter
	}
	normalizeRawConfigJitterShape(raw)

	// Validate after patch to avoid persisting broken layout.
	testOut, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshal patched config for validation: %w", err)
	}
	var testCfg Config
	if err := json.Unmarshal(testOut, &testCfg); err != nil {
		return fmt.Errorf("validate patched config json: %w", err)
	}

	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal patched config: %w", err)
	}
	out = append(out, '\n')

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0644); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

// saveConfigReplace replaces the whole config file with the provided raw json map.
// It validates by loading from temp file before committing replace.
func saveConfigReplace(path string, raw map[string]any) (*Config, error) {
	if path == "" {
		return nil, fmt.Errorf("empty config path")
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty config content")
	}
	normalizeRawConfigJitterShape(raw)

	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	out = append(out, '\n')

	tmp := path + ".replace.tmp"
	if err := os.WriteFile(tmp, out, 0644); err != nil {
		return nil, fmt.Errorf("write temp config: %w", err)
	}

	cfg, err := loadConfig(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("validate config failed: %w", err)
	}
	cfg.ConfigPath = path

	if err := os.Rename(tmp, path); err != nil {
		return nil, fmt.Errorf("replace config: %w", err)
	}
	return cfg, nil
}
