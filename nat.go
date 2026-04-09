package main

import (
	"fmt"
	"log"
	"net"
)

const (
	MAGIC1 = 0xAA
	MAGIC2 = 0x88
)

type KeepAlivePacket struct {
	Version   uint8
	LocalAddr string
	Flags     uint8
	FromAddr  *net.UDPAddr
}

func (r *Relay) parseNatKeepAlivePacket(data []byte, from *net.UDPAddr) (*KeepAlivePacket, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("packet too short")
	}

	pos := 0

	if data[pos] != MAGIC1 || data[pos+1] != MAGIC2 {
		return nil, fmt.Errorf("magic mismatch")
	}
	pos += 2

	version := data[pos]
	pos++

	ipLen := int(data[pos])
	pos++

	if pos+ipLen+1 > len(data) {
		return nil, fmt.Errorf("invalid ip length")
	}

	localAddr := string(data[pos : pos+ipLen])
	pos += ipLen

	flags := data[pos]

	r.NatMux.Lock()
	r.natMapping[localAddr] = from.String()
	r.NatMux.Unlock()

	r.addrMutex.Lock()
	target, exists := r.addrMapping[localAddr]
	if exists {
		r.addrMapping[from.String()] = target
	}
	r.addrMutex.Unlock()

	log.Printf("[Nat] ParseNatKeepAlivePacket: private ip <-> public ip : %s <-> %s", localAddr, from.String())

	if exists {
		for _, a := range target {
			r.AddAddrTarget(a.String(), from)
			log.Printf("[Nat] ParseNatKeepAlivePacket: Reflect stream : %s -> %s", a.String(), from.String())
		}
	}

	return &KeepAlivePacket{
		Version:   version,
		LocalAddr: localAddr,
		Flags:     flags,
		FromAddr:  from,
	}, nil
}
