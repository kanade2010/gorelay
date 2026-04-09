package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	cfgFile := flag.String("c", "mapping.json", "config file")
	flag.Parse()

	cfg, err := loadConfig(*cfgFile)
	if err != nil {
		log.Fatal(err)
	}

	logHub := NewLogHub(500)

	log.SetOutput(io.MultiWriter(os.Stdout, logHub))

	http.HandleFunc("/debug/logs", logHub.handleWS)
	http.HandleFunc("/debug/logpage", logPage)

	relay := NewRelay(cfg)
	log.Println("Starting relay RTP:", cfg.ListenRTP, "RTCP:", cfg.ListenRTCP)
	if err := relay.Run(); err != nil {
		log.Fatal(err)
	}
}
