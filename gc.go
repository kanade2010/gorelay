package main

import (
	"log"
	"net"
	"time"
)

func (r *Relay) StartSSRCUpdater() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			r.updateSSRC()
		}
	}()
}

func (r *Relay) removeSSRCFromAddr(addr string, ssrc uint32) {
	ssrcList, ok := r.addrSsrc[addr]
	if !ok {
		return
	}

	newList := ssrcList[:0]
	for _, v := range ssrcList {
		if v != ssrc {
			newList = append(newList, v)
		}
	}

	if len(newList) == 0 {
		delete(r.addrSsrc, addr)
	} else {
		r.addrSsrc[addr] = newList
	}
}

func (r *Relay) cleanupSSRC(ssrc uint32) {
	log.Printf("[GC] remove SSRC %d", ssrc)

	r.mapMutex.Lock()
	delete(r.mapping, ssrc)
	r.mapMutex.Unlock()

	r.historyMutex.Lock()
	delete(r.history, ssrc)
	r.historyMutex.Unlock()

	r.historyMutex.Lock()
	if jb, ok := r.jitter[ssrc]; ok {
		jb.Close()
		delete(r.jitter, ssrc)
	}
	delete(r.jitterForwarders, ssrc)
	r.historyMutex.Unlock()

	r.lossMutex.Lock()
	delete(r.loss, ssrc)
	r.lossMutex.Unlock()

	r.ssrcDirectMux.Lock()
	delete(r.ssrcDirectMapping, ssrc)
	for k, v := range r.ssrcDirectMapping {
		if v == ssrc {
			delete(r.ssrcDirectMapping, k)
		}
	}
	r.ssrcDirectMux.Unlock()

	r.addrSsrcMutex.Lock()
	for addr, lst := range r.addrSsrc {
		n := make([]uint32, 0, len(lst))
		for _, s := range lst {
			if s != ssrc {
				n = append(n, s)
			}
		}
		if len(n) == 0 {
			delete(r.addrSsrc, addr)
		} else {
			r.addrSsrc[addr] = n
		}
	}
	r.addrSsrcMutex.Unlock()

	r.ssrcTSMux.Lock()
	delete(r.ssrcTS, ssrc)
	r.ssrcTSMux.Unlock()
}

func (r *Relay) cleanupAddr(addr string) {
	log.Printf("[GC] remove ADDR %s", addr)

	r.addrMutex.Lock()
	delete(r.addrMapping, addr)

	for key, addrList := range r.addrMapping {
		newList := make([]*net.UDPAddr, 0, len(addrList))

		for _, udpAddr := range addrList {
			if udpAddr.String() != addr {
				newList = append(newList, udpAddr)
			}
		}

		if len(newList) == 0 {
			delete(r.addrMapping, key)
		} else {
			r.addrMapping[key] = newList
		}
	}
	r.addrMutex.Unlock()

	r.addrSsrcMutex.Lock()
	delete(r.addrSsrc, addr)
	r.addrSsrcMutex.Unlock()

	r.NatMux.Lock()
	delete(r.natMapping, addr)
	r.NatMux.Unlock()

	r.addrTSMux.Lock()
	delete(r.addrTS, addr)
	r.addrTSMux.Unlock()
}

func (r *Relay) updateSSRC() {
	now := time.Now()
	TimeNow = now
	const ssrcTTL = 36 * time.Second
	const addrTTL = 36 * time.Second

	var deadSSRC []uint32

	r.ssrcTSMux.RLock()
	for ssrc, ts := range r.ssrcTS {
		if now.Sub(ts) > ssrcTTL {
			deadSSRC = append(deadSSRC, ssrc)
		}
	}
	r.ssrcTSMux.RUnlock()

	for _, ssrc := range deadSSRC {
		r.cleanupSSRC(ssrc)
	}

	var deadAddr []string
	var activeAddr []string

	r.addrTSMux.RLock()
	for addr, ts := range r.addrTS {
		if now.Sub(ts) > addrTTL {
			deadAddr = append(deadAddr, addr)
		} else {
			activeAddr = append(activeAddr, addr)
		}
	}
	r.addrTSMux.RUnlock()

	for _, addr := range deadAddr {
		r.cleanupAddr(addr)
	}

	if len(deadSSRC) > 0 || len(deadAddr) > 0 {
		log.Printf("[GC] cleaned ssrc=%d addr=%d",
			len(deadSSRC), len(deadAddr))
	}

	var reCheckAddr []string
	r.addrMutex.RLock()
	for addr := range r.addrMapping {
		reCheckAddr = append(reCheckAddr, addr)
	}
	r.addrMutex.RUnlock()

	if TimeNow.Second()%60 == 0 {
		r.addrTSMux.Lock()
		for _, reAddr := range reCheckAddr {

			isOmission := true
			for addr := range r.addrTS {
				if addr == reAddr {
					isOmission = false
				}
			}

			if isOmission {
				log.Printf("[GC] recheck addr=%s\n", reAddr)
				r.addrTS[reAddr] = time.Now().Add(-18 * time.Second)
			}

		}
		r.addrTSMux.Unlock()
	}
}
