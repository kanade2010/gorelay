package main

import (
	"log"
	"math/bits"
	"net"
	"time"

	"github.com/pion/rtcp"
)

func nackPairCount(n rtcp.NackPair) uint64 {
	return 1 + uint64(bits.OnesCount16(uint16(n.LostPackets)))
}

func rtcpToRtpAddr(addr *net.UDPAddr) string {
	if addr == nil {
		return ""
	}
	return (&net.UDPAddr{
		IP:   addr.IP,
		Port: addr.Port - 1,
		Zone: addr.Zone,
	}).String()
}

func makeNackPairs(missing []uint16) []rtcp.NackPair {
	if len(missing) == 0 {
		return nil
	}
	uniqMap := map[uint16]struct{}{}
	for _, s := range missing {
		uniqMap[s] = struct{}{}
	}
	uniq := make([]uint16, 0, len(uniqMap))
	for s := range uniqMap {
		uniq = append(uniq, s)
	}
	for i := 0; i < len(uniq); i++ {
		for j := i + 1; j < len(uniq); j++ {
			if (uniq[i]-uniq[j])&0x8000 != 0 {
				uniq[i], uniq[j] = uniq[j], uniq[i]
			} else if uniq[i] > uniq[j] {
				uniq[i], uniq[j] = uniq[j], uniq[i]
			}
		}
	}

	var pairs []rtcp.NackPair
	i := 0
	for i < len(uniq) {
		base := uniq[i]
		var blp rtcp.PacketBitmap = 0
		j := i + 1
		for j < len(uniq) {
			diff := uniq[j] - base
			if diff == 0 {
				j++
				continue
			}
			if diff >= 1 && diff <= 16 {
				blp |= (1 << (diff - 1))
				j++
				continue
			}
			break
		}
		pairs = append(pairs, rtcp.NackPair{
			PacketID:    base,
			LostPackets: blp,
		})
		i = j
	}
	return pairs
}

func (r *Relay) sendNack(senderSSRC uint32, mediaSSRC uint32, missingSeqs []uint16, srcAddr *net.UDPAddr) {
	remoteSsrc, ok := r.resolveSenderSSRC(senderSSRC, nil, 0)
	if !ok {
		log.Printf("[sendNack] cannot resolve remote SSRC for sender=%d\n", senderSSRC)
		return
	}

	if len(missingSeqs) == 0 || srcAddr == nil {
		return
	}

	pairs := makeNackPairs(missingSeqs)
	if len(pairs) == 0 {
		return
	}

	nack := &rtcp.TransportLayerNack{
		SenderSSRC: remoteSsrc,
		MediaSSRC:  0,
		Nacks:      pairs,
	}

	b, err := nack.Marshal()
	if err != nil {
		log.Printf("[sendNack] marshal nack err: %v", err)
		return
	}

	rtcpAddr := &net.UDPAddr{
		IP:   srcAddr.IP,
		Port: srcAddr.Port + 1,
		Zone: srcAddr.Zone,
	}
	_, err = r.rtcpConn.WriteToUDP(b, rtcpAddr)
	if err != nil {
		log.Printf("failed to send RTCP NACK to %s: %v", srcAddr.String(), err)
	} else {
		log.Printf("→→→ [sendNack](lost=%v) send to %s [ssrc=%d, rssrc=%d]", missingSeqs, rtcpAddr, senderSSRC, remoteSsrc)
	}
}

func (r *Relay) handleRtpInput(ssrc uint32, seq uint16, srcAddr *net.UDPAddr) {
	r.lossMutex.Lock()
	ld, ok := r.loss[ssrc]
	if !ok {
		ld = &LossDetector{
			missing: make(map[uint16]int),
		}
		r.loss[ssrc] = ld
	}
	r.lossMutex.Unlock()

	if !ld.initialized {
		ld.initialized = true
		ld.lastSeq = seq
		return
	}

	if seq == ld.lastSeq {
		return
	}

	if !seqAhead(seq, ld.lastSeq) {
		// Late/out-of-order packet arrived, clear pending-missing mark if any.
		delete(ld.missing, seq)
		return
	}

	expected := ld.lastSeq + 1
	ahead, ok := seqAheadDistance(seq, ld.lastSeq)
	if !ok {
		ld.lastSeq = seq
		delete(ld.missing, seq)
		return
	}
	if ahead == 1 {
		ld.lastSeq = seq
		delete(ld.missing, seq)
		return
	}

	now := time.Now()

	losts := make([]uint16, 0, ahead-1)
	for i := 0; i < ahead-1; i++ {
		lost := expected + uint16(i)
		if ld.missing[lost] < 10 {
			ld.missing[lost]++
			losts = append(losts, lost)

			p := r.weakStats.getOrCreatePeriod(srcAddr.String(), now)
			p.RetransReq++
		}
	}
	if len(losts) > 0 {
		go r.sendNack(ssrc, 0, losts, srcAddr)
	}

	ld.lastSeq = seq
	delete(ld.missing, seq)
}

func (r *Relay) resolveSenderSSRC(sender uint32, rtcpSrcAddr *net.UDPAddr, mediaSSRC uint32) (uint32, bool) {
	r.ssrcDirectMux.RLock()
	if ts, ok := r.ssrcDirectMapping[sender]; ok && ts != 0 {
		if _, exist := r.history[ts]; exist {
			r.ssrcDirectMux.RUnlock()
			// log.Printf("[nack] cached ssrcDirectMapping: sender=%d -> receiver=%d\n", sender, ts)
			return ts, true
		}
	}
	r.ssrcDirectMux.RUnlock()

	srcAddrStr, found := r.findSrcAddrBySender(sender)
	if !found {
		if rtcpSrcAddr != nil {
			rtpAddr := &net.UDPAddr{
				IP:   rtcpSrcAddr.IP,
				Port: rtcpSrcAddr.Port - 1,
				Zone: rtcpSrcAddr.Zone,
			}
			srcAddrStr = rtpAddr.String()
			log.Printf("[nack] no addrSsrc entry for sender=%d, try rtcp src addr %s\n", sender, srcAddrStr)
		} else {
			log.Printf("[nack] cannot find srcAddr for sender=%d\n", sender)
		}
	} else {
		log.Printf("[nack] findSrcAddrBySender: sender=%d -> %s\n", sender, srcAddrStr)
	}

	if srcAddrStr != "" {
		targets := r.GetAddrTargets(srcAddrStr)
		if len(targets) > 0 {
			for _, targ := range targets {
				if targ == nil {
					continue
				}
				if ssrc, ok := r.findSsrcByTargetAddr(targ); ok {
					r.ssrcDirectMux.Lock()
					r.ssrcDirectMapping[sender] = ssrc
					r.ssrcDirectMux.Unlock()
					log.Printf("[nack] resolved: sender=%d -> srcAddr=%s -> target=%s -> receiverSSRC=%d (cached)\n",
						sender, srcAddrStr, targ.String(), ssrc)
					return ssrc, true
				}
			}
		} else {
			log.Printf("[nack] addrMapping[%s] empty\n", srcAddrStr)
		}
	}

	if mediaSSRC != 0 {
		r.ssrcDirectMux.Lock()
		r.ssrcDirectMapping[sender] = mediaSSRC
		r.ssrcDirectMux.Unlock()
		log.Printf("[nack] fallback mediaSSRC used: sender=%d -> media=%d (cached)\n", sender, mediaSSRC)
		return mediaSSRC, true
	}

	log.Printf("[nack] failed to resolve true SSRC for sender=%d\n", sender)
	return 0, false
}

func (r *Relay) handleNack(nack *rtcp.TransportLayerNack, rtcpSrc *net.UDPAddr) {
	r.stats.mu.Lock()
	r.stats.nackRequests++
	r.stats.mu.Unlock()

	rtpAddr := rtcpToRtpAddr(rtcpSrc)

	sender := nack.SenderSSRC
	media := nack.MediaSSRC

	for _, n := range nack.Nacks {
		if rtpAddr != "" {
			r.weakStats.UpdateEgressNack(rtpAddr, nackPairCount(n))
		}
		pid := n.PacketID
		blp := n.LostPackets

		nackSSRC, ok := r.resolveSenderSSRC(sender, rtcpSrc, media)
		if !ok {
			log.Printf("[nack] cannot resolve true SSRC for sender=%d pid=%d blp=%d\n", sender, pid, blp)
			continue
		}

		r.historyMutex.Lock()
		ph := r.history[nackSSRC]
		r.historyMutex.Unlock()

		if ph == nil {
			log.Printf("[nack] no history for nackSSRC=%d\n", nackSSRC)
			continue
		}

		if pkt, ok := ph.Get(pid); ok {
			r.retransmitToMapping(nackSSRC, pkt)
			r.stats.mu.Lock()
			r.stats.retransmits++
			r.stats.mu.Unlock()
		} else {
			log.Printf("[nack] history miss nackSSRC=%d seq=%d\n", nackSSRC, pid)
		}

		for i := uint16(0); i < 16; i++ {
			if (blp>>i)&1 == 1 {
				seq := pid + i + 1
				if pkt2, ok2 := ph.Get(seq); ok2 {
					r.retransmitToMapping(nackSSRC, pkt2)
					r.stats.mu.Lock()
					r.stats.retransmits++
					r.stats.mu.Unlock()
				} else {
					log.Printf("[nack] history miss nackSSRC=%d seq=%d\n", nackSSRC, seq)
				}
			}
		}
	}
}
