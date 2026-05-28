package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const lossMonitorDefaultMaxSeconds = 300

type LossBucket struct {
	TsSec int64 `json:"ts_sec"`

	TxInputPackets      uint64 `json:"tx_input_packets"`
	TxEmuDroppedPackets uint64 `json:"tx_emu_dropped_packets"`
	TxRealLostPackets   uint64 `json:"tx_real_lost_packets"`

	RxInputPackets      uint64 `json:"rx_input_packets"`
	RxEmuDroppedPackets uint64 `json:"rx_emu_dropped_packets"`
	RxRealLostPackets   uint64 `json:"rx_real_lost_packets"`
}

type LossPoint struct {
	TsSec int64 `json:"ts_sec"`

	TxEmulationLossRate float64 `json:"tx_emulation_loss_rate"`
	RxEmulationLossRate float64 `json:"rx_emulation_loss_rate"`
	TxActualLossRate    float64 `json:"tx_actual_loss_rate"`
	RxActualLossRate    float64 `json:"rx_actual_loss_rate"`

	TxInputPackets      uint64 `json:"tx_input_packets"`
	RxInputPackets      uint64 `json:"rx_input_packets"`
	TxEmuDroppedPackets uint64 `json:"tx_emu_dropped_packets"`
	RxEmuDroppedPackets uint64 `json:"rx_emu_dropped_packets"`
	TxRealLostPackets   uint64 `json:"tx_real_lost_packets"`
	RxRealLostPackets   uint64 `json:"rx_real_lost_packets"`
}

type TxRtcpLossState struct {
	LastFractionLost      uint8
	LastCumulativeLost    uint32
	LastUpdateSec         int64
	EstimatedLostInWindow uint64
	Initialized           bool
}

type lossMonitorSeqState struct {
	LastSeq       uint16
	LastUpdateSec int64
	Initialized   bool
}

type LossMonitorPacket struct {
	Key  WeakNetFlowKey
	Seq  uint16
	SSRC uint32
}

type LossMonitorRTCPReport struct {
	Key            WeakNetFlowKey
	FractionLost   uint8
	CumulativeLost uint32
}

type LossMonitor struct {
	mu sync.RWMutex

	buckets    map[int64]*LossBucket
	flowBucket map[string]map[int64]*LossBucket
	maxSeconds int

	lastRxSeq  map[string]lossMonitorSeqState
	txRtcpStat map[string]*TxRtcpLossState
}

func NewLossMonitor(maxSeconds int) *LossMonitor {
	if maxSeconds <= 0 {
		maxSeconds = lossMonitorDefaultMaxSeconds
	}
	return &LossMonitor{
		buckets:    make(map[int64]*LossBucket),
		flowBucket: make(map[string]map[int64]*LossBucket),
		maxSeconds: maxSeconds,
		lastRxSeq:  make(map[string]lossMonitorSeqState),
		txRtcpStat: make(map[string]*TxRtcpLossState),
	}
}

func normalizeLossMonitorFlowKey(key WeakNetFlowKey, direction string) WeakNetFlowKey {
	out := key
	out.Direction = strings.ToLower(strings.TrimSpace(direction))
	out.PeerIP = strings.TrimSpace(out.PeerIP)
	if out.PeerPort < 0 {
		out.PeerPort = 0
	}
	out.StreamID = strings.TrimSpace(out.StreamID)
	if out.StreamID == "" {
		out.StreamID = "*"
	}
	return out
}

func lossMonitorFlowKeyString(key WeakNetFlowKey) string {
	return key.Direction + "|" + key.PeerIP + "|" + strconv.Itoa(key.PeerPort) + "|" + key.StreamID
}

func parseLossMonitorFlowKeyString(flowKey string) (WeakNetFlowKey, bool) {
	parts := strings.SplitN(flowKey, "|", 4)
	if len(parts) != 4 {
		return WeakNetFlowKey{}, false
	}
	port, err := strconv.Atoi(parts[2])
	if err != nil {
		return WeakNetFlowKey{}, false
	}
	return WeakNetFlowKey{
		Direction: parts[0],
		PeerIP:    parts[1],
		PeerPort:  port,
		StreamID:  parts[3],
	}, true
}

func lossMonitorSeqStateKey(key WeakNetFlowKey, ssrc uint32) string {
	return lossMonitorFlowKeyString(key) + "|ssrc:" + strconv.FormatUint(uint64(ssrc), 10)
}

func lossMonitorTxRRStateKey(key WeakNetFlowKey) string {
	return lossMonitorFlowKeyString(key)
}

func (m *LossMonitor) getOrCreateBucketLocked(bucketMap map[int64]*LossBucket, sec int64) *LossBucket {
	b := bucketMap[sec]
	if b == nil {
		b = &LossBucket{TsSec: sec}
		bucketMap[sec] = b
	}
	return b
}

func (m *LossMonitor) updateBucketsLocked(key WeakNetFlowKey, sec int64, mutator func(*LossBucket)) {
	gb := m.getOrCreateBucketLocked(m.buckets, sec)
	mutator(gb)

	fk := lossMonitorFlowKeyString(key)
	fm := m.flowBucket[fk]
	if fm == nil {
		fm = make(map[int64]*LossBucket)
		m.flowBucket[fk] = fm
	}
	fb := m.getOrCreateBucketLocked(fm, sec)
	mutator(fb)
}

func (m *LossMonitor) cleanupOldLocked(nowSec int64) {
	keepFrom := nowSec - int64(m.maxSeconds) + 1
	for sec := range m.buckets {
		if sec < keepFrom {
			delete(m.buckets, sec)
		}
	}
	for fk, bm := range m.flowBucket {
		for sec := range bm {
			if sec < keepFrom {
				delete(bm, sec)
			}
		}
		if len(bm) == 0 {
			delete(m.flowBucket, fk)
		}
	}

	expireStateSec := keepFrom - int64(m.maxSeconds)
	for sk, st := range m.lastRxSeq {
		if st.LastUpdateSec < expireStateSec {
			delete(m.lastRxSeq, sk)
		}
	}
	for tk, st := range m.txRtcpStat {
		if st == nil || st.LastUpdateSec < expireStateSec {
			delete(m.txRtcpStat, tk)
		}
	}
}

func (m *LossMonitor) OnTxInput(pkt *LossMonitorPacket, now time.Time) {
	if m == nil || pkt == nil {
		return
	}
	sec := now.Unix()
	key := normalizeLossMonitorFlowKey(pkt.Key, "tx")

	m.mu.Lock()
	m.cleanupOldLocked(sec)
	m.updateBucketsLocked(key, sec, func(b *LossBucket) {
		b.TxInputPackets++
	})
	m.mu.Unlock()
}

func (m *LossMonitor) OnTxEmuDrop(pkt *LossMonitorPacket, now time.Time) {
	if m == nil || pkt == nil {
		return
	}
	sec := now.Unix()
	key := normalizeLossMonitorFlowKey(pkt.Key, "tx")

	m.mu.Lock()
	m.cleanupOldLocked(sec)
	m.updateBucketsLocked(key, sec, func(b *LossBucket) {
		b.TxEmuDroppedPackets++
	})
	m.mu.Unlock()
}

func (m *LossMonitor) OnRxInput(pkt *LossMonitorPacket, now time.Time) {
	if m == nil || pkt == nil {
		return
	}
	sec := now.Unix()
	key := normalizeLossMonitorFlowKey(pkt.Key, "rx")

	m.mu.Lock()
	m.cleanupOldLocked(sec)
	m.updateBucketsLocked(key, sec, func(b *LossBucket) {
		b.RxInputPackets++
	})
	m.mu.Unlock()
}

func (m *LossMonitor) OnRxEmuDrop(pkt *LossMonitorPacket, now time.Time) {
	if m == nil || pkt == nil {
		return
	}
	sec := now.Unix()
	key := normalizeLossMonitorFlowKey(pkt.Key, "rx")

	m.mu.Lock()
	m.cleanupOldLocked(sec)
	m.updateBucketsLocked(key, sec, func(b *LossBucket) {
		b.RxEmuDroppedPackets++
	})
	m.mu.Unlock()
}

func (m *LossMonitor) OnRxPacketSeq(pkt *LossMonitorPacket, now time.Time) {
	if m == nil || pkt == nil {
		return
	}
	sec := now.Unix()
	key := normalizeLossMonitorFlowKey(pkt.Key, "rx")
	stateKey := lossMonitorSeqStateKey(key, pkt.SSRC)

	m.mu.Lock()
	m.cleanupOldLocked(sec)

	state := m.lastRxSeq[stateKey]
	lost := uint64(0)

	if !state.Initialized {
		state.Initialized = true
		state.LastSeq = pkt.Seq
		state.LastUpdateSec = sec
		m.lastRxSeq[stateKey] = state
		m.mu.Unlock()
		return
	}

	diff := uint16(pkt.Seq - state.LastSeq)
	switch {
	case diff == 0:
		// duplicate
	case diff < 0x8000:
		if diff > 1 {
			lost = uint64(diff - 1)
		}
		state.LastSeq = pkt.Seq
	default:
		// out-of-order/older packet; ignore
	}
	state.LastUpdateSec = sec
	m.lastRxSeq[stateKey] = state

	if lost > 0 {
		m.updateBucketsLocked(key, sec, func(b *LossBucket) {
			b.RxRealLostPackets += lost
		})
	}
	m.mu.Unlock()
}

func (m *LossMonitor) OnTxRtcpReport(rr *LossMonitorRTCPReport, now time.Time) {
	if m == nil || rr == nil {
		return
	}
	sec := now.Unix()
	key := normalizeLossMonitorFlowKey(rr.Key, "tx")
	stateKey := lossMonitorTxRRStateKey(key)

	m.mu.Lock()
	m.cleanupOldLocked(sec)

	state := m.txRtcpStat[stateKey]
	if state == nil {
		state = &TxRtcpLossState{}
		m.txRtcpStat[stateKey] = state
	}

	state.LastFractionLost = rr.FractionLost
	state.LastUpdateSec = sec

	if !state.Initialized {
		state.Initialized = true
		state.LastCumulativeLost = rr.CumulativeLost
		m.mu.Unlock()
		return
	}

	delta := uint64(0)
	if rr.CumulativeLost >= state.LastCumulativeLost {
		delta = uint64(rr.CumulativeLost - state.LastCumulativeLost)
	}

	state.LastCumulativeLost = rr.CumulativeLost

	// Fallback estimate from fraction-lost when cumulative counter doesn't move.
	if delta == 0 && rr.FractionLost > 0 {
		fk := lossMonitorFlowKeyString(key)
		if bm := m.flowBucket[fk]; bm != nil {
			if b := bm[sec]; b != nil && b.TxInputPackets > 0 {
				estimated := uint64(math.Round(float64(b.TxInputPackets) * float64(rr.FractionLost) / 256.0))
				if estimated > 0 {
					delta = estimated
				}
			}
		}
	}

	state.EstimatedLostInWindow = delta

	if delta > 0 {
		m.updateBucketsLocked(key, sec, func(b *LossBucket) {
			b.TxRealLostPackets += delta
		})
	}
	m.mu.Unlock()
}

func lossRate(numerator uint64, denominator uint64) float64 {
	den := float64(denominator)
	if den <= 0 {
		den = 1
	}
	v := float64(numerator) / den
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func seriesLastN(lastN int, maxSeconds int) int {
	if maxSeconds <= 0 {
		maxSeconds = lossMonitorDefaultMaxSeconds
	}
	if lastN <= 0 {
		return maxSeconds
	}
	if lastN > maxSeconds {
		return maxSeconds
	}
	return lastN
}

func (m *LossMonitor) getSeriesAt(now time.Time, lastN int, key *WeakNetFlowKey) []LossPoint {
	if m == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	n := seriesLastN(lastN, m.maxSeconds)
	nowSec := now.Unix()
	startSec := nowSec - int64(n) + 1

	var source map[int64]*LossBucket
	if key != nil {
		nk := normalizeLossMonitorFlowKey(*key, key.Direction)
		source = m.flowBucket[lossMonitorFlowKeyString(nk)]
	} else {
		source = m.buckets
	}

	points := make([]LossPoint, 0, n)
	for sec := startSec; sec <= nowSec; sec++ {
		b := &LossBucket{TsSec: sec}
		if source != nil {
			if sb := source[sec]; sb != nil {
				*b = *sb
			}
		}

		txEmuRate := lossRate(b.TxEmuDroppedPackets, b.TxInputPackets)
		rxEmuRate := lossRate(b.RxEmuDroppedPackets, b.RxInputPackets)
		txActualRate := lossRate(b.TxEmuDroppedPackets+b.TxRealLostPackets, b.TxInputPackets)
		rxActualRate := lossRate(b.RxEmuDroppedPackets+b.RxRealLostPackets, b.RxInputPackets)

		points = append(points, LossPoint{
			TsSec: sec,

			TxEmulationLossRate: txEmuRate,
			RxEmulationLossRate: rxEmuRate,
			TxActualLossRate:    txActualRate,
			RxActualLossRate:    rxActualRate,

			TxInputPackets:      b.TxInputPackets,
			RxInputPackets:      b.RxInputPackets,
			TxEmuDroppedPackets: b.TxEmuDroppedPackets,
			RxEmuDroppedPackets: b.RxEmuDroppedPackets,
			TxRealLostPackets:   b.TxRealLostPackets,
			RxRealLostPackets:   b.RxRealLostPackets,
		})
	}

	return points
}

func (m *LossMonitor) GetSeries(lastN int) []LossPoint {
	return m.getSeriesAt(time.Now(), lastN, nil)
}

func (m *LossMonitor) GetSeriesByFlow(lastN int, key WeakNetFlowKey) []LossPoint {
	return m.getSeriesAt(time.Now(), lastN, &key)
}

func mergeLossBucket(dst *LossBucket, src *LossBucket) {
	if dst == nil || src == nil {
		return
	}
	dst.TxInputPackets += src.TxInputPackets
	dst.TxEmuDroppedPackets += src.TxEmuDroppedPackets
	dst.TxRealLostPackets += src.TxRealLostPackets
	dst.RxInputPackets += src.RxInputPackets
	dst.RxEmuDroppedPackets += src.RxEmuDroppedPackets
	dst.RxRealLostPackets += src.RxRealLostPackets
}

func (m *LossMonitor) getSeriesByPeerIPAt(now time.Time, lastN int, peerIP string) []LossPoint {
	if m == nil {
		return nil
	}
	peerIP = strings.TrimSpace(peerIP)
	if peerIP == "" {
		return m.getSeriesAt(now, lastN, nil)
	}
	if now.IsZero() {
		now = time.Now()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	n := seriesLastN(lastN, m.maxSeconds)
	nowSec := now.Unix()
	startSec := nowSec - int64(n) + 1

	agg := make(map[int64]*LossBucket, n)
	for fk, bm := range m.flowBucket {
		key, ok := parseLossMonitorFlowKeyString(fk)
		if !ok {
			continue
		}
		if key.PeerIP != peerIP {
			continue
		}
		for sec, b := range bm {
			if sec < startSec || sec > nowSec || b == nil {
				continue
			}
			dst := agg[sec]
			if dst == nil {
				dst = &LossBucket{TsSec: sec}
				agg[sec] = dst
			}
			mergeLossBucket(dst, b)
		}
	}

	points := make([]LossPoint, 0, n)
	for sec := startSec; sec <= nowSec; sec++ {
		b := &LossBucket{TsSec: sec}
		if sb := agg[sec]; sb != nil {
			*b = *sb
		}
		txEmuRate := lossRate(b.TxEmuDroppedPackets, b.TxInputPackets)
		rxEmuRate := lossRate(b.RxEmuDroppedPackets, b.RxInputPackets)
		txActualRate := lossRate(b.TxEmuDroppedPackets+b.TxRealLostPackets, b.TxInputPackets)
		rxActualRate := lossRate(b.RxEmuDroppedPackets+b.RxRealLostPackets, b.RxInputPackets)

		points = append(points, LossPoint{
			TsSec: sec,

			TxEmulationLossRate: txEmuRate,
			RxEmulationLossRate: rxEmuRate,
			TxActualLossRate:    txActualRate,
			RxActualLossRate:    rxActualRate,

			TxInputPackets:      b.TxInputPackets,
			RxInputPackets:      b.RxInputPackets,
			TxEmuDroppedPackets: b.TxEmuDroppedPackets,
			RxEmuDroppedPackets: b.RxEmuDroppedPackets,
			TxRealLostPackets:   b.TxRealLostPackets,
			RxRealLostPackets:   b.RxRealLostPackets,
		})
	}
	return points
}

func (m *LossMonitor) GetSeriesByPeerIP(lastN int, peerIP string) []LossPoint {
	return m.getSeriesByPeerIPAt(time.Now(), lastN, peerIP)
}

func (m *LossMonitor) ListPeerIPs() []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	set := make(map[string]struct{}, len(m.flowBucket))
	for fk := range m.flowBucket {
		key, ok := parseLossMonitorFlowKeyString(fk)
		if !ok {
			continue
		}
		if key.PeerIP == "" {
			continue
		}
		set[key.PeerIP] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for ip := range set {
		out = append(out, ip)
	}
	sort.Strings(out)
	return out
}

func (m *LossMonitor) CleanupOld(now time.Time) {
	if m == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	sec := now.Unix()
	m.mu.Lock()
	m.cleanupOldLocked(sec)
	m.mu.Unlock()
}

func parseLossFlowFromQuery(req *http.Request) (*WeakNetFlowKey, error) {
	if req == nil {
		return nil, nil
	}
	q := req.URL.Query()
	direction := strings.TrimSpace(q.Get("direction"))
	peerIP := strings.TrimSpace(q.Get("peer_ip"))
	peerPortText := strings.TrimSpace(q.Get("peer_port"))
	streamID := strings.TrimSpace(q.Get("stream_id"))

	// Global mode by default.
	if direction == "" && peerIP == "" && peerPortText == "" && streamID == "" {
		return nil, nil
	}
	// Peer-IP mode (aggregated over all ports/streams and tx/rx) is handled by handler query.
	if direction == "" && peerPortText == "" && streamID == "" {
		return nil, nil
	}

	if direction == "" || peerIP == "" || peerPortText == "" {
		return nil, fmt.Errorf("need direction/peer_ip/peer_port")
	}
	peerPort, err := strconv.Atoi(peerPortText)
	if err != nil {
		return nil, err
	}
	key := normalizeLossMonitorFlowKey(WeakNetFlowKey{
		Direction: direction,
		PeerIP:    peerIP,
		PeerPort:  peerPort,
		StreamID:  streamID,
	}, direction)
	if key.Direction != "tx" && key.Direction != "rx" {
		return nil, fmt.Errorf("direction must be tx or rx")
	}
	return &key, nil
}

func (r *Relay) handleWeakNetLossAPI(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r == nil || r.lossMonitor == nil {
		http.Error(w, "loss monitor unavailable", http.StatusInternalServerError)
		return
	}

	last := lossMonitorDefaultMaxSeconds
	if raw := strings.TrimSpace(req.URL.Query().Get("last")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			last = n
		}
	}

	flowKey, err := parseLossFlowFromQuery(req)
	if err != nil {
		http.Error(w, "invalid flow query params: need direction/peer_ip/peer_port", http.StatusBadRequest)
		return
	}
	peerIP := strings.TrimSpace(req.URL.Query().Get("peer_ip"))

	points := []LossPoint(nil)
	scope := "global"
	var flow any
	selectedPeerIP := ""
	if flowKey != nil {
		scope = "flow"
		flow = flowKey
		points = r.lossMonitor.GetSeriesByFlow(last, *flowKey)
		selectedPeerIP = flowKey.PeerIP
	} else if peerIP != "" && !strings.EqualFold(peerIP, "all") {
		scope = "peer_ip"
		selectedPeerIP = peerIP
		points = r.lossMonitor.GetSeriesByPeerIP(last, peerIP)
	} else {
		points = r.lossMonitor.GetSeries(last)
	}

	resp := map[string]any{
		"ok":                 true,
		"scope":              scope,
		"last":               seriesLastN(last, r.lossMonitor.maxSeconds),
		"points":             points,
		"available_peer_ips": r.lossMonitor.ListPeerIPs(),
		"selected_peer_ip":   selectedPeerIP,
		"note":               "tx_actual_loss_rate includes tx emulation drop + tx real loss; tx real loss depends on RTCP RR",
	}
	if flow != nil {
		resp["flow_key"] = flow
	}
	_ = json.NewEncoder(w).Encode(resp)
}
