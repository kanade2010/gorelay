package main

import (
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type ForwardDebugState struct {
	initialized bool
	lastSeq     uint16
	lastSSRC    uint32
	lastSrcAddr string
}

type PcapCaptureState struct {
	running    bool
	ports      []int
	backend    string
	rawPath    string
	etlPath    string
	pcapngPath string
	startedAt  time.Time
	deadlineAt time.Time
}

type Relay struct {
	cfg               *Config
	rtpConn           *net.UDPConn
	rtcpConn          *net.UDPConn
	mapping           map[uint32][]*net.UDPAddr
	addrMapping       map[string][]*net.UDPAddr
	addrSsrc          map[string][]uint32
	mappings          map[uint32]*ControlMapping
	ssrcDirectMapping map[uint32]uint32
	natMapping        map[string]string
	ssrcTS            map[uint32]time.Time
	loss              map[uint32]*LossDetector
	lossMutex         sync.Mutex
	mapMutex          sync.RWMutex
	addrMutex         sync.RWMutex
	addrSsrcMutex     sync.RWMutex
	mappingsMux       sync.RWMutex
	ssrcDirectMux     sync.RWMutex
	NatMux            sync.RWMutex
	ssrcTSMux         sync.RWMutex
	history           map[uint32]*PacketHistory
	historyMutex      sync.Mutex
	jitter            map[uint32]*JitterBuffer
	jitterForwarders  map[uint32]bool
	sendLimiter       *rate.Limiter
	stats             *Stats
	weakStats         WeakNetStats
	addrTS            map[string]time.Time
	addrTSMux         sync.RWMutex
	rtcpSRSeen        map[string]time.Time
	rtcpSRSeenMux     sync.Mutex
	forwardDebug      ForwardDebugState
	forwardDebugMux   sync.Mutex
	debugForwardIP    string
	forwardQueue      chan ForwardJob
	pacers            map[string]*Pacer
	pacerMux          sync.RWMutex
	pacerCfgMux       sync.RWMutex
	pacerOverrides    map[string]PacerTargetConfig
	pcapCapture       PcapCaptureState
	pcapCaptureMux    sync.Mutex
	pcapCaptureCmd    any
	pcapCaptureTimer  *time.Timer
}

func NewRelay(cfg *Config) *Relay {
	return &Relay{
		cfg:               cfg,
		mapping:           make(map[uint32][]*net.UDPAddr),
		addrMapping:       make(map[string][]*net.UDPAddr),
		addrSsrc:          make(map[string][]uint32),
		mappings:          make(map[uint32]*ControlMapping),
		ssrcDirectMapping: make(map[uint32]uint32),
		natMapping:        make(map[string]string),
		ssrcTS:            make(map[uint32]time.Time),
		loss:              make(map[uint32]*LossDetector),
		history:           make(map[uint32]*PacketHistory),
		jitter:            make(map[uint32]*JitterBuffer),
		jitterForwarders:  make(map[uint32]bool),
		sendLimiter:       rate.NewLimiter(rate.Limit(cfg.SendRateBps), cfg.SendRateBps),
		stats:             &Stats{lastUpdated: time.Now()},
		weakStats: WeakNetStats{
			Stats:        make(map[string]*AddrWeakSeries),
			KeyReqTotals: make(map[string]*KeyframeTotals),
			Period:       1 * time.Second,
			MaxWin:       60,
		},
		addrTS:         make(map[string]time.Time),
		rtcpSRSeen:     make(map[string]time.Time),
		forwardQueue:   make(chan ForwardJob, 8192),
		pacers:         make(map[string]*Pacer),
		pacerOverrides: make(map[string]PacerTargetConfig),
	}
}
