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

type Config struct {
	ListenRTP              string    `json:"listen_rtp"`
	ListenRTCP             string    `json:"listen_rtcp"`
	ListenControl          string    `json:"listen_control"`
	Mappings               []Mapping `json:"mappings"`
	PacketHistoryMS        int       `json:"packet_history_ms"`
	JitterBufferMS         int       `json:"jitter_buffer_ms"`
	JitterMinDelayMS       int       `json:"jitter_min_delay_ms"`
	JitterMaxDelayMS       int       `json:"jitter_max_delay_ms"`
	JitterTickMS           int       `json:"jitter_tick_ms"`
	JitterUpStepMS         int       `json:"jitter_up_step_ms"`
	JitterDownStepMS       int       `json:"jitter_down_step_ms"`
	JitterLogEnabled       bool      `json:"jitter_log_enabled"`
	JitterLogPeriodMS      int       `json:"jitter_log_period_ms"`
	JitterSourceIPsEnabled bool      `json:"jitter_source_ips_enabled"`
	JitterSourceIPs        []string  `json:"jitter_source_ips"`
	SendRateBps            int       `json:"send_rate_bps"`
	EnableJitter           bool      `json:"enable_jitter"`
	EnablePacer            bool      `json:"enable_pacer"`
	ConfigPath             string    `json:"-"`
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
	if _, ok := raw["jitter_log_enabled"]; !ok {
		cfg.JitterLogEnabled = true
	}
	if _, ok := raw["jitter_log_period_ms"]; !ok {
		cfg.JitterLogPeriodMS = 2000
	}
	if _, ok := raw["enable_pacer"]; !ok {
		cfg.EnablePacer = true
	}
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
	cfg.ConfigPath = path
	return &cfg, nil
}

func saveConfig(path string, cfg *Config) error {
	if path == "" {
		return fmt.Errorf("empty config path")
	}

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

	var raw map[string]json.RawMessage
	if len(data) > 0 {
		if err := json.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("parse config raw: %w", err)
		}
	}
	if raw == nil {
		raw = make(map[string]json.RawMessage)
	}

	for key, value := range patch {
		b, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("marshal patch key %s: %w", key, err)
		}
		raw[key] = json.RawMessage(b)
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
