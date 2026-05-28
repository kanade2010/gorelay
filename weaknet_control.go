package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type WeakNetEmulationPatch struct {
	Enabled *bool  `json:"enabled"`
	Seed    *int64 `json:"seed"`

	FixedDelayMs  *int    `json:"fixed_delay_ms"`
	JitterDelayMs *int    `json:"jitter_delay_ms"`
	JitterDist    *string `json:"jitter_dist"`

	RandomLossRate *float64 `json:"random_loss_rate"`

	BurstLossEnabled *bool    `json:"burst_loss_enabled"`
	PGoodToBad       *float64 `json:"p_good_to_bad"`
	PBadToGood       *float64 `json:"p_bad_to_good"`
	LossInGood       *float64 `json:"loss_in_good"`
	LossInBad        *float64 `json:"loss_in_bad"`

	FixedDropEveryN *int `json:"fixed_drop_every_n"`

	MaxQueuePackets *int   `json:"max_queue_packets"`
	MaxQueueBytes   *int64 `json:"max_queue_bytes"`
	MaxResidenceMs  *int   `json:"max_residence_ms"`
	DelayCapMs      *int   `json:"delay_cap_ms"`
}

type UpdateWeakNetRequest struct {
	TxPreset  string                 `json:"tx_preset"`
	RxPreset  string                 `json:"rx_preset"`
	TxDefault *WeakNetEmulationPatch `json:"tx_default"`
	RxDefault *WeakNetEmulationPatch `json:"rx_default"`
}

type UpdateWeakNetFlowRequest struct {
	Direction string                 `json:"direction"`
	PeerIP    string                 `json:"peer_ip"`
	PeerPort  int                    `json:"peer_port"`
	StreamID  string                 `json:"stream_id"`
	Delete    *bool                  `json:"delete"`
	Preset    string                 `json:"preset"`
	Config    *WeakNetEmulationPatch `json:"config"`
}

func weakNetPresetNames() []string {
	return []string{"light", "medium", "fixed_drop_test"}
}

func applyWeakNetPatch(base WeakNetEmulationConfig, patch *WeakNetEmulationPatch) WeakNetEmulationConfig {
	if patch == nil {
		out := base
		normalizeWeakNetEmulationConfig(&out)
		return out
	}
	out := base
	if patch.Enabled != nil {
		out.Enabled = *patch.Enabled
	}
	if patch.Seed != nil {
		out.Seed = *patch.Seed
	}
	if patch.FixedDelayMs != nil {
		out.FixedDelayMs = *patch.FixedDelayMs
	}
	if patch.JitterDelayMs != nil {
		out.JitterDelayMs = *patch.JitterDelayMs
	}
	if patch.JitterDist != nil {
		out.JitterDist = *patch.JitterDist
	}
	if patch.RandomLossRate != nil {
		out.RandomLossRate = *patch.RandomLossRate
	}
	if patch.BurstLossEnabled != nil {
		out.BurstLossEnabled = *patch.BurstLossEnabled
	}
	if patch.PGoodToBad != nil {
		out.PGoodToBad = *patch.PGoodToBad
	}
	if patch.PBadToGood != nil {
		out.PBadToGood = *patch.PBadToGood
	}
	if patch.LossInGood != nil {
		out.LossInGood = *patch.LossInGood
	}
	if patch.LossInBad != nil {
		out.LossInBad = *patch.LossInBad
	}
	if patch.FixedDropEveryN != nil {
		out.FixedDropEveryN = *patch.FixedDropEveryN
	}
	if patch.MaxQueuePackets != nil {
		out.MaxQueuePackets = *patch.MaxQueuePackets
	}
	if patch.MaxQueueBytes != nil {
		out.MaxQueueBytes = *patch.MaxQueueBytes
	}
	if patch.MaxResidenceMs != nil {
		out.MaxResidenceMs = *patch.MaxResidenceMs
	}
	if patch.DelayCapMs != nil {
		out.DelayCapMs = *patch.DelayCapMs
	}
	normalizeWeakNetEmulationConfig(&out)
	return out
}

func (r *Relay) requireWeakNet() error {
	if r == nil || r.weaknet == nil {
		return fmt.Errorf("weaknet not initialized")
	}
	return nil
}

func (r *Relay) handleUpdateWeakNet(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := r.requireWeakNet(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if req.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"saved":   true,
			"path":    r.cfg.ConfigPath,
			"presets": weakNetPresetNames(),
			"weaknet": map[string]any{
				"tx_default": r.cfg.WeakNet.TxDefault,
				"rx_default": r.cfg.WeakNet.RxDefault,
			},
		})
		return
	}
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body UpdateWeakNetRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	next := r.cfg.WeakNet
	changed := false

	txCfg := next.TxDefault
	if strings.TrimSpace(body.TxPreset) != "" {
		p, ok := weakNetPreset(body.TxPreset)
		if !ok {
			http.Error(w, "invalid tx_preset", http.StatusBadRequest)
			return
		}
		txCfg = p
		changed = true
	}
	if body.TxDefault != nil {
		txCfg = applyWeakNetPatch(txCfg, body.TxDefault)
		changed = true
	}
	next.TxDefault = txCfg

	rxCfg := next.RxDefault
	if strings.TrimSpace(body.RxPreset) != "" {
		p, ok := weakNetPreset(body.RxPreset)
		if !ok {
			http.Error(w, "invalid rx_preset", http.StatusBadRequest)
			return
		}
		rxCfg = p
		changed = true
	}
	if body.RxDefault != nil {
		rxCfg = applyWeakNetPatch(rxCfg, body.RxDefault)
		changed = true
	}
	next.RxDefault = rxCfg

	if !changed {
		http.Error(w, "empty patch", http.StatusBadRequest)
		return
	}

	if err := saveConfigPatch(r.cfg.ConfigPath, map[string]any{
		"weaknet": map[string]any{
			"tx_default": next.TxDefault,
			"rx_default": next.RxDefault,
		},
	}); err != nil {
		http.Error(w, fmt.Sprintf("save config failed: %v", err), http.StatusInternalServerError)
		return
	}

	r.cfg.WeakNet = next
	r.applyWeakNetDefaultsToRuntime()

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"saved":   true,
		"path":    r.cfg.ConfigPath,
		"presets": weakNetPresetNames(),
		"weaknet": map[string]any{
			"tx_default": r.cfg.WeakNet.TxDefault,
			"rx_default": r.cfg.WeakNet.RxDefault,
		},
	})
}

func (r *Relay) parseWeakNetFlowFromQuery(req *http.Request) (WeakNetFlowKey, error) {
	q := req.URL.Query()
	port, err := strconv.Atoi(strings.TrimSpace(q.Get("peer_port")))
	if err != nil {
		return WeakNetFlowKey{}, fmt.Errorf("invalid peer_port")
	}
	key := WeakNetFlowKey{
		Direction: strings.TrimSpace(q.Get("direction")),
		PeerIP:    strings.TrimSpace(q.Get("peer_ip")),
		PeerPort:  port,
		StreamID:  strings.TrimSpace(q.Get("stream_id")),
	}
	if key.StreamID == "" {
		key.StreamID = "*"
	}
	return normalizeWeakNetFlowKey(key, key.Direction)
}

func (r *Relay) handleUpdateWeakNetFlow(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := r.requireWeakNet(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if req.Method == http.MethodGet {
		q := req.URL.Query()
		if strings.TrimSpace(q.Get("direction")) == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":             true,
				"in_memory_only": true,
				"path":           r.cfg.ConfigPath,
				"tx_overrides":   r.weaknet.ListTxConfig(),
				"rx_overrides":   r.weaknet.ListRxConfig(),
			})
			return
		}

		key, err := r.parseWeakNetFlowFromQuery(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var cfg WeakNetEmulationConfig
		var exactExists bool
		var matchedWildcard bool
		var getErr error

		switch key.Direction {
		case "tx":
			cfg, exactExists, getErr = r.weaknet.GetTxConfig(key)
		case "rx":
			cfg, exactExists, getErr = r.weaknet.GetRxConfig(key)
		default:
			getErr = fmt.Errorf("invalid direction")
		}
		if getErr != nil {
			http.Error(w, getErr.Error(), http.StatusBadRequest)
			return
		}

		effective := r.cfg.WeakNet.TxDefault
		if key.Direction == "rx" {
			effective = r.cfg.WeakNet.RxDefault
		}
		if exactExists {
			effective = cfg
		} else if key.StreamID != "*" {
			wild := key
			wild.StreamID = "*"
			var wcfg WeakNetEmulationConfig
			var wok bool
			if key.Direction == "tx" {
				wcfg, wok, _ = r.weaknet.GetTxConfig(wild)
			} else {
				wcfg, wok, _ = r.weaknet.GetRxConfig(wild)
			}
			if wok {
				effective = wcfg
				matchedWildcard = true
			}
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":               true,
			"in_memory_only":   true,
			"path":             r.cfg.ConfigPath,
			"key":              key,
			"override_exists":  exactExists,
			"matched_wildcard": matchedWildcard,
			"config":           cfg,
			"effective":        effective,
		})
		return
	}

	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body UpdateWeakNetFlowRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	key, err := normalizeWeakNetFlowKey(WeakNetFlowKey{
		Direction: body.Direction,
		PeerIP:    body.PeerIP,
		PeerPort:  body.PeerPort,
		StreamID:  body.StreamID,
	}, body.Direction)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.Delete != nil && *body.Delete {
		if key.Direction == "tx" {
			if err := r.weaknet.DeleteTxConfig(key); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		} else {
			if err := r.weaknet.DeleteRxConfig(key); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":             true,
			"saved":          false,
			"in_memory_only": true,
			"deleted":        true,
			"path":           r.cfg.ConfigPath,
			"key":            key,
		})
		return
	}

	cfg := r.cfg.WeakNet.TxDefault
	if key.Direction == "rx" {
		cfg = r.cfg.WeakNet.RxDefault
	}
	if key.Direction == "tx" {
		if existing, ok, _ := r.weaknet.GetTxConfig(key); ok {
			cfg = existing
		}
	} else {
		if existing, ok, _ := r.weaknet.GetRxConfig(key); ok {
			cfg = existing
		}
	}

	if strings.TrimSpace(body.Preset) != "" {
		p, ok := weakNetPreset(body.Preset)
		if !ok {
			http.Error(w, "invalid preset", http.StatusBadRequest)
			return
		}
		cfg = p
	}
	if body.Config != nil {
		cfg = applyWeakNetPatch(cfg, body.Config)
	}
	if strings.TrimSpace(body.Preset) == "" && body.Config == nil {
		http.Error(w, "empty patch", http.StatusBadRequest)
		return
	}

	if key.Direction == "tx" {
		if err := r.weaknet.UpdateTxConfig(key, cfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		if err := r.weaknet.UpdateRxConfig(key, cfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":             true,
		"saved":          false,
		"in_memory_only": true,
		"path":           r.cfg.ConfigPath,
		"key":            key,
		"config":         cfg,
	})
}

func (r *Relay) handleWeakNetStats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := r.requireWeakNet(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"path":    r.cfg.ConfigPath,
		"weaknet": r.weaknet.SnapshotStats(),
	})
}
