package main

import (
	"fmt"
	"log"
	"math/bits"
	"net"
	"time"

	"github.com/pion/rtcp"
)

const nackLateProbeDelay = 100 * time.Millisecond
const nackLateProbeMaxSeqBehind = 64
const nackLateProbeChecks = 3
const nackLateProbeBatchWindow = 10 * time.Millisecond

type nackProbeBatch struct {
	ph   *PacketHistory
	seqs map[uint16]struct{}
}

func nackPairCount(n rtcp.NackPair) uint64 {
	return 1 + uint64(bits.OnesCount16(uint16(n.LostPackets)))
}

func nackProbeKey(ssrc uint32, seq uint16) string {
	return fmt.Sprintf("%d:%d", ssrc, seq)
}

func (r *Relay) tryBeginNackProbe(ssrc uint32, seq uint16) bool {
	key := nackProbeKey(ssrc, seq)
	r.nackProbeMu.Lock()
	defer r.nackProbeMu.Unlock()
	if _, ok := r.nackProbePending[key]; ok {
		return false
	}
	r.nackProbePending[key] = struct{}{}
	return true
}

func (r *Relay) endNackProbe(ssrc uint32, seq uint16) {
	key := nackProbeKey(ssrc, seq)
	r.nackProbeMu.Lock()
	delete(r.nackProbePending, key)
	r.nackProbeMu.Unlock()
}

func shouldScheduleLateProbe(lastSeq uint16, missedSeq uint16) bool {
	d, ok := seqAheadDistance(lastSeq, missedSeq)
	if !ok {
		return false
	}
	return d > 0 && d <= nackLateProbeMaxSeqBehind
}

func (r *Relay) scheduleLateHistoryProbe(nackSSRC uint32, seq uint16, ph *PacketHistory) {
	if ph == nil {
		return
	}
	if !r.tryBeginNackProbe(nackSSRC, seq) {
		return
	}

	startBatchWorker := false
	r.nackProbeBatchMu.Lock()
	batch, ok := r.nackProbeBatches[nackSSRC]
	if !ok {
		batch = &nackProbeBatch{
			ph:   ph,
			seqs: make(map[uint16]struct{}),
		}
		r.nackProbeBatches[nackSSRC] = batch
		startBatchWorker = true
	}
	batch.seqs[seq] = struct{}{}
	r.nackProbeBatchMu.Unlock()

	if startBatchWorker {
		go r.runLateHistoryProbeBatch(nackSSRC, batch)
	}
}

func (r *Relay) runLateHistoryProbeBatch(nackSSRC uint32, batch *nackProbeBatch) {
	time.Sleep(nackLateProbeBatchWindow)

	r.nackProbeBatchMu.Lock()
	current, ok := r.nackProbeBatches[nackSSRC]
	if !ok || current != batch {
		r.nackProbeBatchMu.Unlock()
		return
	}
	delete(r.nackProbeBatches, nackSSRC)
	ph := current.ph
	seqs := make([]uint16, 0, len(current.seqs))
	for seq := range current.seqs {
		seqs = append(seqs, seq)
	}
	r.nackProbeBatchMu.Unlock()

	if ph == nil || len(seqs) == 0 {
		for _, seq := range seqs {
			r.endNackProbe(nackSSRC, seq)
		}
		return
	}

	remaining := make(map[uint16]struct{}, len(seqs))
	for _, seq := range seqs {
		remaining[seq] = struct{}{}
	}

	for probe := 1; probe <= nackLateProbeChecks && len(remaining) > 0; probe++ {
		time.Sleep(nackLateProbeDelay)
		delayMS := int64(probe) * nackLateProbeDelay.Milliseconds()
		for seq := range remaining {
			pkt, hit := ph.Get(seq)
			if !hit {
				continue
			}
			hSize, hMaxMs, hLastAgeMs, hLastSeq := ph.Snapshot(time.Now())
			r.retransmitToMapping(nackSSRC, pkt)
			r.stats.mu.Lock()
			r.stats.retransmits++
			r.stats.nackLateRecovered++
			r.stats.mu.Unlock()
			log.Printf("[nack] late-hit retransmit nackSSRC=%d seq=%d delay_ms=%d history_size=%d history_max_ms=%d last_age_ms=%d last_seq=%d",
				nackSSRC, seq, delayMS, hSize, hMaxMs, hLastAgeMs, hLastSeq)
			delete(remaining, seq)
			r.endNackProbe(nackSSRC, seq)
		}
	}

	if len(remaining) == 0 {
		return
	}

	r.stats.mu.Lock()
	r.stats.nackLateStillMiss += uint64(len(remaining))
	r.stats.mu.Unlock()
	for seq := range remaining {
		r.endNackProbe(nackSSRC, seq)
	}
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
	if len(missingSeqs) == 0 || srcAddr == nil {
		return
	}

	remoteSsrc, ok := r.resolveSenderSSRC(senderSSRC, nil, mediaSSRC)
	if !ok {
		log.Printf("[sendNack] cannot resolve remote SSRC for sender=%d media=%d\n", senderSSRC, mediaSSRC)
		return
	}

	pairs := makeNackPairs(missingSeqs)
	if len(pairs) == 0 {
		return
	}

	nack := &rtcp.TransportLayerNack{
		SenderSSRC: remoteSsrc,
		MediaSSRC:  mediaSSRC,
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
			r.stats.mu.Lock()
			r.stats.nackResolveFail++
			r.stats.mu.Unlock()
			log.Printf("[nack] cannot resolve true SSRC for sender=%d pid=%d blp=%d\n", sender, pid, blp)
			continue
		}

		r.historyMutex.Lock()
		ph := r.history[nackSSRC]
		r.historyMutex.Unlock()

		if ph == nil {
			r.stats.mu.Lock()
			r.stats.nackNoHistory++
			r.stats.mu.Unlock()

			r.historyMutex.Lock()
			historyStreams := len(r.history)
			r.historyMutex.Unlock()

			log.Printf("[nack] no history for nackSSRC=%d history_streams=%d\n", nackSSRC, historyStreams)
			continue
		}

		if pkt, ok := ph.Get(pid); ok {
			r.retransmitToMapping(nackSSRC, pkt)
			r.stats.mu.Lock()
			r.stats.retransmits++
			r.stats.mu.Unlock()
		} else {
			r.stats.mu.Lock()
			r.stats.nackHistoryMiss++
			r.stats.mu.Unlock()
			hSize, hMaxMs, hLastAgeMs, hLastSeq := ph.Snapshot(time.Now())
			log.Printf("[nack] history miss nackSSRC=%d seq=%d history_size=%d history_max_ms=%d last_age_ms=%d last_seq=%d\n",
				nackSSRC, pid, hSize, hMaxMs, hLastAgeMs, hLastSeq)
			if shouldScheduleLateProbe(hLastSeq, pid) {
				r.scheduleLateHistoryProbe(nackSSRC, pid, ph)
			}
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
					r.stats.mu.Lock()
					r.stats.nackHistoryMiss++
					r.stats.mu.Unlock()
					hSize, hMaxMs, hLastAgeMs, hLastSeq := ph.Snapshot(time.Now())
					log.Printf("[nack] history miss nackSSRC=%d seq=%d history_size=%d history_max_ms=%d last_age_ms=%d last_seq=%d\n",
						nackSSRC, seq, hSize, hMaxMs, hLastAgeMs, hLastSeq)
					if shouldScheduleLateProbe(hLastSeq, seq) {
						r.scheduleLateHistoryProbe(nackSSRC, seq, ph)
					}
				}
			}
		}
	}
}
