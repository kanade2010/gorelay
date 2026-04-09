package main

import (
	"log"
	"net"
	"time"
)

func (r *Relay) Run() error {
	laddrRTP, err := net.ResolveUDPAddr("udp", r.cfg.ListenRTP)
	if err != nil {
		return err
	}
	laddrRTCP, err := net.ResolveUDPAddr("udp", r.cfg.ListenRTCP)
	if err != nil {
		return err
	}
	rtpConn, err := net.ListenUDP("udp", laddrRTP)
	if err != nil {
		return err
	}
	rtcpConn, err := net.ListenUDP("udp", laddrRTCP)
	if err != nil {
		return err
	}
	r.rtpConn = rtpConn
	r.rtcpConn = rtcpConn
	r.setupPacer()

	if err := r.loadMapping(); err != nil {
		return err
	}

	if r.cfg.EnableJitter {
		for ssrc, jb := range r.jitter {
			r.historyMutex.Lock()
			r.jitterForwarders[ssrc] = true
			r.historyMutex.Unlock()
			go r.forwardFromJitter(ssrc, jb)
		}
	}

	go r.rtpReadLoop()
	go r.rtcpReadLoop()
	go r.forwardSenderLoop()
	go r.controlLoop()
	go r.StartSSRCUpdater()

	go func() {
		for {
			time.Sleep(10 * time.Second)
			r.stats.mu.RLock()
			log.Printf("[stats] recv=%d sent=%d nacks=%d retrans=%d\n", r.stats.recvPackets, r.stats.sentPackets, r.stats.nackRequests, r.stats.retransmits)
			r.stats.mu.RUnlock()
		}
	}()

	select {}
}

func (r *Relay) setupPacer() {
	enable, rate := r.getGlobalPacerDefaults()
	params := defaultPacerResolvedParams(r.cfg)
	log.Printf("[pacer] setup mode=per-target default_enable=%t default_rate=%dbps default_tick=%dms log=%t/%dms",
		enable, rate, params.TickMS, params.LogEnabled, params.LogPeriodMS)
}
