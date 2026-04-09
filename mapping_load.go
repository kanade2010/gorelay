package main

import (
	"fmt"
	"net"
)

func (r *Relay) loadMapping() error {
	for _, m := range r.cfg.Mappings {
		addrs := make([]*net.UDPAddr, 0, len(m.Targets))
		for _, t := range m.Targets {
			addr, err := net.ResolveUDPAddr("udp", t)
			if err != nil {
				return err
			}
			addrs = append(addrs, addr)
		}
		r.mapping[m.SrcSSRC] = addrs
		r.history[m.SrcSSRC] = NewPacketHistory(r.cfg.PacketHistoryMS)
		r.jitter[m.SrcSSRC] = NewJitterBuffer(
			r.cfg.EnableJitter,
			r.cfg.JitterMinDelayMS,
			r.cfg.JitterMaxDelayMS,
			r.cfg.JitterTickMS,
			r.cfg.JitterUpStepMS,
			r.cfg.JitterDownStepMS,
			r.cfg.JitterLogEnabled,
			r.cfg.JitterLogPeriodMS,
			fmt.Sprintf("ssrc=%d", m.SrcSSRC),
		)
	}
	return nil
}
