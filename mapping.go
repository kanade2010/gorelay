package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

func (r *Relay) SetMapping(ssrc uint32, targets []*net.UDPAddr) {
	r.mapMutex.Lock()
	defer r.mapMutex.Unlock()

	uniq := make([]*net.UDPAddr, 0, len(targets))
	exists := map[string]bool{}

	for _, t := range targets {
		if t == nil {
			continue
		}
		key := t.String()
		if !exists[key] {
			exists[key] = true
			uniq = append(uniq, t)
		}
	}

	r.mapping[ssrc] = uniq
	log.Printf("[mapping] SetMapping ssrc=%d targets=%d\n", ssrc, len(uniq))
}

func (r *Relay) GetMapping(ssrc uint32) ([]*net.UDPAddr, bool) {
	r.mapMutex.RLock()
	m, ok := r.mapping[ssrc]
	r.mapMutex.RUnlock()
	return m, ok
}

func (r *Relay) AddTarget(ssrc uint32, addr *net.UDPAddr) {
	r.mapMutex.Lock()
	r.mapping[ssrc] = append(r.mapping[ssrc], addr)
	r.mapMutex.Unlock()
}

func (r *Relay) AddAddrTarget(key string, addr *net.UDPAddr) {
	r.addrMutex.Lock()
	defer r.addrMutex.Unlock()

	lst := r.addrMapping[key]

	for _, a := range lst {
		if a.IP.Equal(addr.IP) && a.Port == addr.Port {
			return
		}
	}

	r.addrMapping[key] = append(lst, addr)
}

func (r *Relay) RemoveAddrTarget(key string) {
	r.addrMutex.Lock()
	defer r.addrMutex.Unlock()
	delete(r.addrMapping, key)
}

func (r *Relay) GetAddrTargets(key string) []*net.UDPAddr {
	r.addrMutex.RLock()
	defer r.addrMutex.RUnlock()
	list := r.addrMapping[key]
	if list == nil {
		return nil
	}
	out := make([]*net.UDPAddr, 0, len(list))
	for _, a := range list {
		out = append(out, a)
	}
	return out
}

func (r *Relay) mappingPrintln() {
	r.mapMutex.RLock()
	fmt.Println("mapping:")
	for ssrc, targets := range r.mapping {
		fmt.Printf("  %d => ", ssrc)
		for _, t := range targets {
			fmt.Printf("%s ", t.String())
		}
		fmt.Println()
	}
	r.mapMutex.RUnlock()

	r.addrMutex.RLock()
	fmt.Println("addrMapping:")
	for key, targets := range r.addrMapping {
		fmt.Printf("  %s => ", key)
		for _, t := range targets {
			fmt.Printf("%s ", t.String())
		}
		fmt.Println()
	}
	r.addrMutex.RUnlock()

	r.addrSsrcMutex.RLock()
	fmt.Println("addrToSSRC:")
	for addr, ssrc := range r.addrSsrc {
		fmt.Printf("  %s => %v\n", addr, ssrc)
	}
	r.addrSsrcMutex.RUnlock()

	r.mappingsMux.RLock()
	fmt.Println("ControlMappings:")
	for ssrc, m := range r.mappings {
		fmt.Printf("  %d => %+v\n", ssrc, *m)
	}
	r.mappingsMux.RUnlock()
}

func mergeTargets(a, b []*net.UDPAddr) []*net.UDPAddr {
	out := make([]*net.UDPAddr, 0, len(a)+len(b))
	seen := make(map[string]struct{})

	add := func(list []*net.UDPAddr) {
		for _, t := range list {
			key := t.String()
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, t)
			}
		}
	}

	add(a)
	add(b)
	return out
}

func (r *Relay) SyncSsrcAddrMapping(ssrc uint32, srcAddr string) {
	if TimeNow.Sub(TimeRecord) >= 5*time.Second {
		r.mapMutex.RLock()
		ssrcTargets := r.mapping[ssrc]
		r.mapMutex.RUnlock()

		r.addrMutex.RLock()
		addrTargets := r.addrMapping[srcAddr]
		r.addrMutex.RUnlock()

		if len(ssrcTargets) != len(addrTargets) {
			merged := mergeTargets(ssrcTargets, addrTargets)

			r.mapMutex.Lock()
			r.mapping[ssrc] = merged
			r.mapMutex.Unlock()

			r.addrMutex.Lock()
			r.addrMapping[srcAddr] = merged
			r.addrMutex.Unlock()

			log.Printf("[SYNC] merged targets: ssrc=%d addr=%s size=%d",
				ssrc, srcAddr, len(merged))
		}

		TimeRecord = TimeNow
	}
}

func (r *Relay) findSrcAddrBySender(sender uint32) (string, bool) {
	r.addrSsrcMutex.RLock()
	defer r.addrSsrcMutex.RUnlock()
	for addr, ssrcs := range r.addrSsrc {
		for _, s := range ssrcs {
			if s == sender {
				return addr, true
			}
		}
	}
	return "", false
}

func (r *Relay) findSsrcByTargetAddr(target *net.UDPAddr) (uint32, bool) {
	r.addrSsrcMutex.RLock()
	defer r.addrSsrcMutex.RUnlock()
	for addr, ssrcs := range r.addrSsrc {
		for _, s := range ssrcs {
			if addr == target.String() {
				return s, true
			}
		}
	}
	return 0, false
}

func (r *Relay) findSSRCByTargetAddr(target *net.UDPAddr) (uint32, bool) {
	r.mapMutex.RLock()
	defer r.mapMutex.RUnlock()
	for ssrc, targets := range r.mapping {
		for _, t := range targets {
			if t == nil {
				continue
			}
			if t.IP.Equal(target.IP) && t.Port == target.Port {
				return ssrc, true
			}
		}
	}
	return 0, false
}
