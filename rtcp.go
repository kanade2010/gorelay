package main

import (
	"encoding/binary"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/pion/rtcp"
)

func (r *Relay) handleRTCPPacket(p rtcp.Packet, addr *net.UDPAddr) {
	switch pkt := p.(type) {
	case *rtcp.SenderReport:
		r.recordSenderReport(pkt, addr)
		// log.Printf("SR from %s", addr)
	case *rtcp.ReceiverReport:
		for _, rr := range pkt.Reports {
			log.Printf("[RTCP] RR from %s: ssrc=%d fraction=%d lost=%d",
				addr, rr.SSRC, rr.FractionLost, rr.TotalLost)
		}
		r.handleRR(pkt, addr)
	case *rtcp.TransportLayerNack:
		// log.Printf("NACK %v", pkt.Nacks)
		r.handleNack(pkt, addr)
	case *rtcp.PictureLossIndication:
		// log.Printf("PLI for ssrc=%d", pkt.MediaSSRC)
		r.weakStats.UpdateKeyframeReq(addr.String(), true, false)
	case *rtcp.FullIntraRequest:
		// log.Printf("FIR for ssrc=%d", pkt.SenderSSRC)
		r.weakStats.UpdateKeyframeReq(addr.String(), false, true)
	default:
		// log.Printf("RTCP: %T", p)
	}
}

func rtcpSRSeenKey(reporter string, ssrc uint32, lsr uint32) string {
	return reporter + "|" + strconv.FormatUint(uint64(ssrc), 10) + "|" + strconv.FormatUint(uint64(lsr), 10)
}

func ntpMiddle32From64(ntp uint64) uint32 {
	return uint32((ntp >> 16) & 0xffffffff)
}

func dlsrToDuration(dlsr uint32) time.Duration {
	secs := dlsr >> 16
	fracs := dlsr & 0xffff
	return time.Duration(secs)*time.Second + time.Duration((int64(fracs)*int64(time.Second))>>16)
}

func (r *Relay) putSRSeen(reporter string, ssrc uint32, lsr uint32, t time.Time) {
	r.rtcpSRSeenMux.Lock()
	r.rtcpSRSeen[rtcpSRSeenKey(reporter, ssrc, lsr)] = t
	expireBefore := t.Add(-15 * time.Second)
	for k, ts := range r.rtcpSRSeen {
		if ts.Before(expireBefore) {
			delete(r.rtcpSRSeen, k)
		}
	}
	r.rtcpSRSeenMux.Unlock()
}

func (r *Relay) popSRSeen(reporter string, ssrc uint32, lsr uint32) (time.Time, bool) {
	key := rtcpSRSeenKey(reporter, ssrc, lsr)
	r.rtcpSRSeenMux.Lock()
	t, ok := r.rtcpSRSeen[key]
	if ok {
		delete(r.rtcpSRSeen, key)
	}
	r.rtcpSRSeenMux.Unlock()
	return t, ok
}

func (r *Relay) recordSenderReport(sr *rtcp.SenderReport, addr *net.UDPAddr) {
	lsr := ntpMiddle32From64(sr.NTPTime)
	if lsr == 0 {
		return
	}
	now := time.Now()
	key := addr.String()
	r.addrMutex.RLock()
	targets := r.addrMapping[key]
	r.addrMutex.RUnlock()
	for _, t := range targets {
		reporter := t.String()
		if reporter == key {
			continue
		}
		r.putSRSeen(reporter, sr.SSRC, lsr, now)
	}
}

func (r *Relay) rtcpReadLoop() {
	buf := make([]byte, 1500)
	for {
		n, addr, err := r.rtcpConn.ReadFromUDP(buf)
		if err != nil {
			log.Println("RTCP read err:", err)
			continue
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		r.forwardRTCP(addr, data)

		pkts, perr := rtcp.Unmarshal(data)
		if perr != nil {
			_, nerr := r.parseNatKeepAlivePacket(data, addr)
			if nerr != nil {
			}

			continue
		}

		r.addrTSMux.Lock()
		r.addrTS[addr.String()] = TimeNow
		r.addrTSMux.Unlock()

		for _, p := range pkts {
			r.handleRTCPPacket(p, addr)
		}
	}
}

func (r *Relay) forwardRTCP(srcAddr *net.UDPAddr, pkt []byte) {
	key := srcAddr.String()

	r.addrMutex.RLock()
	targets := r.addrMapping[key]
	r.addrMutex.RUnlock()

	if len(targets) == 0 {
		return
	}

	for _, t := range targets {
		if t.String() == key {
			continue
		}
		_, err := r.rtcpConn.WriteToUDP(pkt, t)
		if err != nil {
			log.Printf("[RTCP] forward to %s err: %v", t, err)
		} else {
			// log.Printf("[RTCP] forward to %s", t.String())
		}
	}
}

func ntpToTime(ntp uint32) time.Time {
	secs := ntp >> 16
	fracs := ntp & 0xffff
	nsec := (int64(fracs) * 1e9) >> 16
	return time.Unix(int64(secs), nsec)
}

func ntpMiddle32ToTime(lsr uint32) time.Time {
	seconds := float64(lsr>>16) + float64(lsr&0xFFFF)/65536.0
	ntpEpoch := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

	return ntpEpoch.Add(time.Duration(seconds * float64(time.Second)))
}

func (r *Relay) collectPacersForReporter(addr *net.UDPAddr) []*Pacer {
	if addr == nil {
		return nil
	}
	reporterKey := addr.String()
	reporterIP := ""
	if addr.IP != nil {
		reporterIP = addr.IP.String()
	}

	r.pacerMux.RLock()
	defer r.pacerMux.RUnlock()

	seen := make(map[*Pacer]struct{})
	out := make([]*Pacer, 0, 4)
	appendUnique := func(p *Pacer) {
		if p == nil {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}

	appendUnique(r.pacers[reporterKey])
	if reporterIP != "" {
		for key, p := range r.pacers {
			ip, ok := ipFromTargetKey(key)
			if !ok || ip != reporterIP {
				continue
			}
			appendUnique(p)
		}
	}
	return out
}

func (r *Relay) applyRRLossFeedback(addr *net.UDPAddr, fractionLost uint8) {
	pacers := r.collectPacersForReporter(addr)
	for _, p := range pacers {
		if p == nil {
			continue
		}
		pcfg := p.Config()
		if !pcfg.Enabled {
			continue
		}
		p.ApplyRTCPFractionLoss(fractionLost)
	}
}

func (r *Relay) handleRR(rr *rtcp.ReceiverReport, addr *net.UDPAddr) {
	now := time.Now()
	r.stats.mu.Lock()
	r.stats.lastUpdated = now
	r.stats.mu.Unlock()
	maxFractionLost := uint8(0)
	for _, rep := range rr.Reports {
		if rep.FractionLost > maxFractionLost {
			maxFractionLost = rep.FractionLost
		}
		if rep.LastSenderReport == 0 {
			continue
		}
		srSeenAt, ok := r.popSRSeen(addr.String(), rep.SSRC, rep.LastSenderReport)
		if !ok {
			continue
		}
		rtt := now.Sub(srSeenAt) - dlsrToDuration(rep.Delay)
		if rtt <= 0 || rtt > 10*time.Second {
			continue
		}
		r.weakStats.UpdateRTT(addr.String(), rtt)
	}
	if len(rr.Reports) > 0 {
		r.applyRRLossFeedback(addr, maxFractionLost)
	}
}

func (r *Relay) retransmitToMapping(ssrc uint32, pkt []byte) {
	targets, ok := r.GetMapping(ssrc)

	if ok {
		for _, addr := range targets {
			if len(pkt) > 4 && false {
				log.Println("[nack] [retransmit] retransmit to ", addr, "-", ssrc, ":", binary.BigEndian.Uint16(pkt[2:4]))
			}
			if addr == nil {
				continue
			}
			// RTCP-triggered retransmit bypasses pacer and forwards directly.
			if _, err := r.rtpConn.WriteToUDP(pkt, addr); err != nil {
				log.Printf("[ERROR][rtcp-retransmit] write_failed target=%s ssrc=%d len=%d err=%v",
					addr.String(), ssrc, len(pkt), err)
				continue
			}
			r.debugForwardPacket(addr.String(), "", ssrc, pkt)
		}
	}
}
