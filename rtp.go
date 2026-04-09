package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/pion/rtp"
)

type ForwardJob struct {
	ssrc    uint32
	srcAddr string
	target  *net.UDPAddr
	pkt     []byte
	prio    PacerPriority
	counted bool
}

func isDebugForwardTarget(target string, debugIP string) bool {
	if debugIP == "" {
		return false
	}
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	return host == debugIP
}

func isSTUN(b []byte) bool {
	if len(b) < 20 {
		return false
	}

	return b[4] == 0x21 &&
		b[5] == 0x12 &&
		b[6] == 0xA4 &&
		b[7] == 0x42
}

func looksLikeRTP(b []byte) bool {
	if len(b) < 12 {
		return false
	}
	return (b[0] >> 6) == 2
}

func (r *Relay) shouldUseJitterForSource(addr *net.UDPAddr) bool {
	if !r.cfg.EnableJitter {
		return false
	}
	if !r.cfg.JitterSourceIPsEnabled {
		return true
	}
	allow := r.cfg.JitterSourceIPs
	if len(allow) == 0 {
		return false
	}
	if addr == nil || addr.IP == nil {
		return false
	}
	srcIP := addr.IP.String()
	for _, ip := range allow {
		if srcIP == ip {
			return true
		}
	}
	return false
}

func (r *Relay) debugForwardPacket(target string, srcAddr string, routeSSRC uint32, pkt []byte) {
	r.forwardDebugMux.Lock()
	debugIP := r.debugForwardIP
	if !isDebugForwardTarget(target, debugIP) {
		r.forwardDebugMux.Unlock()
		return
	}
	if len(pkt) < 12 {
		log.Printf("[ERROR][forward-debug] target=%s invalid_rtp len=%d src=%s route_ssrc=%d", target, len(pkt), srcAddr, routeSSRC)
		r.forwardDebugMux.Unlock()
		return
	}
	seq := binary.BigEndian.Uint16(pkt[2:4])
	pktSSRC := binary.BigEndian.Uint32(pkt[8:12])
	st := r.forwardDebug
	log.Printf("[forward-debug] target=%s src=%s seq=%d pkt_ssrc=%d route_ssrc=%d", target, srcAddr, seq, pktSSRC, routeSSRC)
	if st.initialized {
		if pktSSRC != st.lastSSRC {
			log.Printf("[ERROR][forward-debug] target=%s ssrc_changed last=%d now=%d seq=%d src=%s last_src=%s", target, st.lastSSRC, pktSSRC, seq, srcAddr, st.lastSrcAddr)
		} else {
			expected := st.lastSeq + 1
			if seq != expected {
				checked := 0
				hit := 0
				miss := 0
				s := expected
				for s != seq && checked < 64 {
					if r.hasPacketInHistory(pktSSRC, s) || r.hasPacketInHistory(routeSSRC, s) {
						hit++
					} else {
						miss++
					}
					s++
					checked++
				}
				log.Printf("[ERROR][forward-debug] target=%s seq_discontinuous expected=%d got=%d ssrc=%d src=%s last_src=%s history_checked=%d history_hit=%d history_miss=%d", target, expected, seq, pktSSRC, srcAddr, st.lastSrcAddr, checked, hit, miss)
			}
		}
		if st.lastSrcAddr != "" && srcAddr != "" && srcAddr != st.lastSrcAddr {
			log.Printf("[ERROR][forward-debug] target=%s source_addr_changed last=%s now=%s ssrc=%d seq=%d", target, st.lastSrcAddr, srcAddr, pktSSRC, seq)
		}
	}
	r.forwardDebug.initialized = true
	r.forwardDebug.lastSeq = seq
	r.forwardDebug.lastSSRC = pktSSRC
	if srcAddr != "" {
		r.forwardDebug.lastSrcAddr = srcAddr
	}
	r.forwardDebugMux.Unlock()
}

func (r *Relay) hasPacketInHistory(ssrc uint32, seq uint16) bool {
	r.historyMutex.Lock()
	ph := r.history[ssrc]
	r.historyMutex.Unlock()
	if ph == nil {
		return false
	}
	_, ok := ph.Get(seq)
	return ok
}

func (r *Relay) handleSTUN(pkt []byte, addr string) {
	addrTargets := r.GetAddrTargets(addr)

	if len(addrTargets) == 0 {
		return
	}

	for _, a := range addrTargets {
		if a == nil {
			continue
		}
		if _, err := r.rtpConn.WriteToUDP(pkt, a); err != nil {
			log.Println("forward write err:", err)
		}
	}
}

func (r *Relay) forwardSenderLoop() {
	for job := range r.forwardQueue {
		if ok, reason := r.enqueueToPacer(job); !ok {
			st := PacerStats{}
			if job.target != nil {
				if pst, _, exists := r.getPacerStatsForTarget(job.target); exists {
					st = pst
				}
			}
			if job.target != nil {
				log.Printf("[ERROR][pacer] enqueue_failed reason=%s target=%s src=%s ssrc=%d len=%d prio=%d queue_packets=%d queue_bytes=%d queue_flows=%d",
					reason, job.target.String(), job.srcAddr, job.ssrc, len(job.pkt), job.prio,
					st.QueuePackets, st.QueueBytes, st.QueueFlows)
			} else {
				log.Printf("[ERROR][pacer] enqueue_failed reason=%s src=%s ssrc=%d len=%d prio=%d queue_packets=%d queue_bytes=%d queue_flows=%d",
					reason, job.srcAddr, job.ssrc, len(job.pkt), job.prio,
					st.QueuePackets, st.QueueBytes, st.QueueFlows)
			}
			continue
		}
	}
}

func (r *Relay) enqueueToPacer(job ForwardJob) (bool, string) {
	if job.target == nil || len(job.pkt) == 0 {
		return false, "invalid_job"
	}

	pacer, _, err := r.getOrCreatePacerForTarget(job.target)
	if err != nil {
		return false, "pacer_create_error"
	}
	if pacer != nil {
		return pacer.EnqueueWithReason(job, job.prio)
	}

	if _, err := r.rtpConn.WriteToUDP(job.pkt, job.target); err != nil {
		log.Println("forward write err:", err)
		return false, "udp_write_error"
	}
	r.onPacerSent(job, len(job.pkt))
	return true, ""
}

func (r *Relay) onPacerSent(job ForwardJob, _ int) {
	if job.target == nil {
		return
	}

	r.debugForwardPacket(job.target.String(), job.srcAddr, job.ssrc, job.pkt)

	if job.counted {
		r.stats.mu.Lock()
		r.stats.sentPackets++
		r.stats.mu.Unlock()
		r.weakStats.UpdateEgressSent(job.target.String(), 1)
	}
}

func (r *Relay) rtpReadLoop() {
	buf := make([]byte, 1500)
	cnt := 0
	for {
		n, addr, err := r.rtpConn.ReadFromUDP(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println("RTP read err:", err)
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])

		if isSTUN(data) {
			r.handleSTUN(data, addr.String())
			continue
		}

		var pkt rtp.Packet
		if perr := pkt.Unmarshal(data); perr != nil {
			_, nerr := r.parseNatKeepAlivePacket(data, addr)

			if nerr != nil {
				log.Println("rtp unmarshal err:", perr)
			}
			continue
		}
		srcAddr := addr.String()
		srcSSRC := pkt.SSRC
		r.ssrcTSMux.Lock()
		r.ssrcTS[srcSSRC] = TimeNow
		r.ssrcTSMux.Unlock()
		r.addrTSMux.Lock()
		r.addrTS[srcAddr] = TimeNow
		r.addrTSMux.Unlock()
		r.stats.mu.Lock()
		r.stats.recvPackets++
		r.stats.mu.Unlock()

		r.weakStats.UpdateRTP(addr.String(), &pkt)
		r.handleRtpInput(srcSSRC, pkt.SequenceNumber, addr)

		cnt = cnt + 1
		if cnt%50 == 0 {
		}

		// r.stats.mu.Lock()
		// r.stats.recvPackets++
		// r.stats.mu.Unlock()

		r.addrSsrcMutex.RLock()
		lst, exists := r.addrSsrc[srcAddr]
		r.addrSsrcMutex.RUnlock()
		if !exists {
			r.addrSsrcMutex.Lock()
			r.addrSsrc[srcAddr] = []uint32{srcSSRC}
			r.addrSsrcMutex.Unlock()
		} else {
			add := true
			for _, a := range lst {
				if a == srcSSRC {
					add = false
				}
			}
			if add {
				r.addrSsrcMutex.Lock()
				r.addrSsrc[srcAddr] = append(lst, srcSSRC)
				r.addrSsrcMutex.Unlock()
			}
		}

		r.historyMutex.Lock()
		ph, ok := r.history[srcSSRC]
		var jb *JitterBuffer
		startJitterForwarder := false
		useJitterForPacket := r.shouldUseJitterForSource(addr)
		if ok {
			jb = r.jitter[srcSSRC]
		} else {
			ph = NewPacketHistory(r.cfg.PacketHistoryMS)
			r.history[srcSSRC] = ph
			jb = NewJitterBuffer(
				r.cfg.EnableJitter,
				r.cfg.JitterMinDelayMS,
				r.cfg.JitterMaxDelayMS,
				r.cfg.JitterTickMS,
				r.cfg.JitterUpStepMS,
				r.cfg.JitterDownStepMS,
				r.cfg.JitterLogEnabled,
				r.cfg.JitterLogPeriodMS,
				fmt.Sprintf("ssrc=%d", srcSSRC),
			)
			r.jitter[srcSSRC] = jb
			log.Printf("new ssrc seen: %d from %s\n", srcSSRC, addr)
		}
		if useJitterForPacket && jb != nil && !r.jitterForwarders[srcSSRC] {
			r.jitterForwarders[srcSSRC] = true
			startJitterForwarder = true
		}
		r.historyMutex.Unlock()
		ph.Add(&pkt, data)

		if startJitterForwarder {
			log.Printf("[jitter][ssrc=%d] start forward worker", srcSSRC)
			go r.forwardFromJitter(srcSSRC, jb)
		}

		if jb != nil && useJitterForPacket {
			r.ensureMappingForSSRC(srcSSRC, srcAddr)
			jb.SafeAdd(pkt.SequenceNumber, data)
		} else {
			r.forwardToTargets(srcSSRC, srcAddr, data)
		}
	}
}

func (r *Relay) forwardFromJitter(ssrc uint32, jb *JitterBuffer) {
	for pkt := range jb.Out() {
		if !r.cfg.EnableJitter {
			continue
		}
		r.forwardToTargets(ssrc, "", pkt)
	}
}

func (r *Relay) ensureMappingForSSRC(ssrc uint32, srcAddr string) {
	if srcAddr == "" {
		return
	}

	r.SyncSsrcAddrMapping(ssrc, srcAddr)

	targets, ok := r.GetMapping(ssrc)
	if ok && len(targets) > 0 {
		return
	}

	if addrTargets := r.GetAddrTargets(srcAddr); len(addrTargets) > 0 {
		r.SetMapping(ssrc, addrTargets)
		log.Printf("Synced addrMapping[%s] -> mapping[%d] (%d targets)\n", srcAddr, ssrc, len(addrTargets))
	}
}

func (r *Relay) forwardToTargets(ssrc uint32, srcAddr string, pkt []byte) {
	r.ensureMappingForSSRC(ssrc, srcAddr)
	targets, ok := r.GetMapping(ssrc)

	if !ok || len(targets) == 0 {
		return
	}

	// fix me.
	// if err := r.sendLimiter.WaitN(context.Background(), len(pkt)); err != nil {
	// 	return
	// }

	for _, addr := range targets {
		if addr == nil {
			continue
		}
		job := ForwardJob{
			ssrc:    ssrc,
			srcAddr: srcAddr,
			target:  addr,
			pkt:     append([]byte(nil), pkt...),
			prio:    PacerPriorityNormal,
			counted: true,
		}
		select {
		case r.forwardQueue <- job:
		default:
			log.Printf("[ERROR][forward-queue] queue_full target=%s src=%s ssrc=%d len=%d", addr.String(), srcAddr, ssrc, len(pkt))
		}
	}
}
