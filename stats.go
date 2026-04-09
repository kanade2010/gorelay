package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/pion/rtp"
)

type LossDetector struct {
	lastSeq     uint16
	initialized bool
	missing     map[uint16]int
}

type WeakNetPeriodStat struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`

	LastArrival time.Time

	LastSeq    uint16
	Received   uint64
	Lost       uint64
	RetransReq uint64
	JitterSum  float64
	JitterCnt  uint64
	RTTSum     uint64
	RTTCnt     uint64
	EgressSent uint64
	EgressNack uint64
	PLIReq     uint64
	FIRReq     uint64
	KeyReq     uint64
}

type AddrWeakSeries struct {
	Periods []*WeakNetPeriodStat
}

type KeyframeTotals struct {
	PLI uint64
	FIR uint64
	All uint64
}

type WeakNetStats struct {
	mutex sync.RWMutex

	Period time.Duration
	MaxWin int

	Stats        map[string]*AddrWeakSeries
	KeyReqTotals map[string]*KeyframeTotals
}

func (ws *WeakNetStats) updatePeriod(period time.Duration) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	ws.Period = period

	for _, s := range ws.Stats {
		s.Periods = nil
	}
	return
}

func (ws *WeakNetStats) currentPeriod(now time.Time) (start, end time.Time) {
	start = now.Truncate(ws.Period)
	end = start.Add(ws.Period)
	return
}

func (ws *WeakNetStats) getOrCreatePeriod(addr string, now time.Time) *WeakNetPeriodStat {
	start, end := ws.currentPeriod(now)

	series, ok := ws.Stats[addr]
	if !ok {
		series = &AddrWeakSeries{}
		ws.Stats[addr] = series
	}

	n := len(series.Periods)
	if n > 0 {
		last := series.Periods[n-1]
		if last.Start.Equal(start) {
			return last
		}
	}

	p := &WeakNetPeriodStat{
		Start: start,
		End:   end,
	}
	series.Periods = append(series.Periods, p)

	if len(series.Periods) > ws.MaxWin {
		series.Periods = series.Periods[len(series.Periods)-ws.MaxWin:]
	}
	return p
}

func (ws *WeakNetStats) UpdateRTP(addr string, pkt *rtp.Packet) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	now := time.Now()
	p := ws.getOrCreatePeriod(addr, now)

	seq := pkt.SequenceNumber
	p.Received++

	if p.LastSeq != 0 {
		var diff int
		if seq >= p.LastSeq {
			diff = int(seq - p.LastSeq)
		} else {
			diff = int(seq) + 65536 - int(p.LastSeq)
		}

		if diff > 1 && diff < 32767 {
			p.Lost += uint64(diff - 1)
		}

	}
	p.LastSeq = seq

	if !p.LastArrival.IsZero() {
		d := now.Sub(p.LastArrival).Seconds() * 1000
		if d < 0 {
			d = -d
		}
		p.JitterSum += d
		p.JitterCnt++
	}
	p.LastArrival = now
}

func (ws *WeakNetStats) UpdateRTT(addr string, rtt time.Duration) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	p := ws.getOrCreatePeriod(addr, time.Now())
	p.RTTSum += uint64(rtt.Milliseconds())
	p.RTTCnt++
}

func (ws *WeakNetStats) UpdateEgressSent(addr string, n uint64) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	p := ws.getOrCreatePeriod(addr, time.Now())
	p.EgressSent += n
}

func (ws *WeakNetStats) UpdateEgressNack(addr string, n uint64) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	p := ws.getOrCreatePeriod(addr, time.Now())
	p.EgressNack += n
}

func (ws *WeakNetStats) UpdateKeyframeReq(addr string, isPLI bool, isFIR bool) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	t := ws.KeyReqTotals[addr]
	if t == nil {
		t = &KeyframeTotals{}
		ws.KeyReqTotals[addr] = t
	}
	if isPLI {
		t.PLI++
	}
	if isFIR {
		t.FIR++
	}
	if isPLI || isFIR {
		t.All++
	}
}

func (ws *WeakNetStats) Export(lastN int) map[string]any {
	ws.mutex.RLock()
	defer ws.mutex.RUnlock()

	out := map[string]any{}
	now := time.Now()
	expireBefore := now.Add(-time.Duration(lastN) * ws.Period)

	for addr, series := range ws.Stats {
		rows := make([]map[string]any, 0)
		t := ws.KeyReqTotals[addr]
		var pliReq uint64
		var firReq uint64
		var keyReq uint64
		if t != nil {
			pliReq = t.PLI
			firReq = t.FIR
			keyReq = t.All
		}

		for _, p := range series.Periods {
			if p.End.Before(expireBefore) {
				continue
			}

			lossRate := "0%"
			if p.Received > 0 {
				lr := float64(p.Lost) / (float64(p.Received) + float64(p.Lost))
				if lr > 1 {
					lr = 1
				}
				lossRate = fmt.Sprintf("%.2f%%", lr*100)
			}

			jitter := "0 ms"
			if p.JitterCnt > 0 {
				jitter = fmt.Sprintf("%.2f ms", p.JitterSum/float64(p.JitterCnt))
			}

			rtt := "0 ms"
			if p.RTTCnt > 0 {
				rtt = fmt.Sprintf("%d ms", p.RTTSum/p.RTTCnt)
			}

			egressLossByNack := "0%"
			if p.EgressSent > 0 {
				el := float64(p.EgressNack) / float64(p.EgressSent)
				if el > 1 {
					el = 1
				}
				egressLossByNack = fmt.Sprintf("%.2f%%", el*100)
			}

			rows = append(rows, map[string]any{
				"start":                p.Start.Format(time.RFC3339),
				"end":                  p.End.Format(time.RFC3339),
				"received":             p.Received,
				"lost":                 p.Lost,
				"loss_rate":            lossRate,
				"retrans":              p.RetransReq,
				"jitter":               jitter,
				"rtt":                  rtt,
				"egress_sent":          p.EgressSent,
				"egress_nack":          p.EgressNack,
				"egress_loss_est_nack": egressLossByNack,
				"pli_req":              pliReq,
				"fir_req":              firReq,
				"keyframe_req":         keyReq,
			})
		}

		if len(rows) > 0 {
			out[addr] = rows
		}
	}
	return out
}

type Stats struct {
	mu           sync.RWMutex
	recvPackets  uint64
	sentPackets  uint64
	nackRequests uint64
	retransmits  uint64
	lastUpdated  time.Time
}
