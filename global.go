package main

import "time"

var LastSeen = make(map[uint32]time.Time)
var addrLastSeen = make(map[string]time.Time)
var TimeNow = time.Now()
var TimeRecord = time.Now()
var version = "1.1.5"
