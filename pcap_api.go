package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const maxPcapCaptureDuration = 10 * time.Minute

func parseListenPort(addr string) (int, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return 0, err
	}
	return p, nil
}

func (r *Relay) capturePorts() []int {
	ports := make([]int, 0, 3)
	if p, err := parseListenPort(r.cfg.ListenRTP); err == nil {
		ports = append(ports, p)
	}
	if p, err := parseListenPort(r.cfg.ListenRTCP); err == nil {
		ports = append(ports, p)
	}
	if p, err := parseListenPort(r.cfg.ListenControl); err == nil {
		ports = append(ports, p)
	}
	sort.Ints(ports)
	out := make([]int, 0, len(ports))
	prev := -1
	for _, p := range ports {
		if p != prev {
			out = append(out, p)
			prev = p
		}
	}
	return out
}

func runCmd(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

func ensureCaptureDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(wd, "captures")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

func convertEtlToPcapng(etlPath, pcapngPath string) error {
	if _, err := runCmd("pktmon", "pcapng", etlPath, "-o", pcapngPath); err == nil {
		return nil
	}
	if _, err := runCmd("etl2pcapng", etlPath, pcapngPath); err == nil {
		return nil
	}
	return errors.New("convert etl to pcapng failed")
}

func convertPcapToPcapng(pcapPath, pcapngPath string) error {
	if _, err := runCmd("editcap", "-F", "pcapng", pcapPath, pcapngPath); err == nil {
		return nil
	}
	if _, err := runCmd("tshark", "-F", "pcapng", "-r", pcapPath, "-w", pcapngPath); err == nil {
		return nil
	}
	return errors.New("convert pcap to pcapng failed; install editcap or tshark")
}

func buildPortFilterExpr(ports []int) string {
	if len(ports) == 0 {
		return ""
	}
	parts := make([]string, 0, len(ports))
	for _, p := range ports {
		parts = append(parts, "port "+strconv.Itoa(p))
	}
	return strings.Join(parts, " or ")
}

func (r *Relay) cancelPcapAutoStopLocked() {
	if r.pcapCaptureTimer != nil {
		r.pcapCaptureTimer.Stop()
		r.pcapCaptureTimer = nil
	}
}

func (r *Relay) schedulePcapAutoStopLocked() {
	r.cancelPcapAutoStopLocked()
	r.pcapCaptureTimer = time.AfterFunc(maxPcapCaptureDuration, func() {
		r.pcapCaptureMux.Lock()
		defer r.pcapCaptureMux.Unlock()
		if !r.pcapCapture.running {
			return
		}
		if err := r.stopPcapCaptureLocked(); err != nil {
			log.Printf("[pcap] auto stop failed: %v", err)
			return
		}
		log.Printf("[pcap] auto stopped after %s", maxPcapCaptureDuration)
	})
}

func (r *Relay) startPcapCapture() error {
	ports := r.capturePorts()
	if len(ports) == 0 {
		return errors.New("no listen ports found")
	}
	dir, err := ensureCaptureDir()
	if err != nil {
		return err
	}
	base := "relay_capture"
	etlPath := filepath.Join(dir, base+".etl")
	rawPath := filepath.Join(dir, base+".pcap")
	pcapngPath := filepath.Join(dir, base+".pcapng")
	_ = os.Remove(etlPath)
	_ = os.Remove(rawPath)
	_ = os.Remove(pcapngPath)
	switch runtime.GOOS {
	case "windows":
		_, _ = runCmd("pktmon", "stop")
		_, _ = runCmd("pktmon", "filter", "remove")
		for _, p := range ports {
			if _, err := runCmd("pktmon", "filter", "add", "-p", strconv.Itoa(p)); err != nil {
				return err
			}
		}
		if _, err := runCmd("pktmon", "start", "--capture", "--pkt-size", "0", "--file-name", etlPath); err != nil {
			return err
		}
		r.pcapCapture.backend = "pktmon"
		r.pcapCapture.etlPath = etlPath
	case "linux":
		tcpdumpPath := "/root/tcpdump"
		tcpdumpBin := ""
		if fi, err := os.Stat(tcpdumpPath); err == nil && !fi.IsDir() && (fi.Mode()&0o111) != 0 {
			tcpdumpBin = tcpdumpPath
		} else if p, err := exec.LookPath("tcpdump"); err == nil {
			tcpdumpBin = p
		} else {
			return errors.New("tcpdump not found; set /root/tcpdump or install tcpdump in PATH")
		}
		expr := buildPortFilterExpr(ports)
		cmd := exec.Command(tcpdumpBin, "-i", "any", "-nn", "-U", "-w", rawPath, expr)
		if err := cmd.Start(); err != nil {
			return err
		}
		r.pcapCapture.backend = "tcpdump"
		r.pcapCapture.rawPath = rawPath
		r.pcapCaptureCmd = cmd
	default:
		return errors.New("unsupported os for pcap capture api")
	}

	r.pcapCapture.running = true
	r.pcapCapture.ports = ports
	r.pcapCapture.pcapngPath = pcapngPath
	r.pcapCapture.startedAt = time.Now()
	r.pcapCapture.deadlineAt = r.pcapCapture.startedAt.Add(maxPcapCaptureDuration)
	r.schedulePcapAutoStopLocked()
	return nil
}

func (r *Relay) stopPcapCaptureLocked() error {
	if !r.pcapCapture.running {
		downloadPath := r.captureDownloadPathLocked()
		if downloadPath != "" {
			if _, err := os.Stat(downloadPath); err == nil {
				return nil
			}
		}
		if r.pcapCapture.backend == "pktmon" && r.pcapCapture.etlPath != "" && r.pcapCapture.pcapngPath != "" {
			if _, err := os.Stat(r.pcapCapture.etlPath); err == nil {
				return convertEtlToPcapng(r.pcapCapture.etlPath, r.pcapCapture.pcapngPath)
			}
		}
		return nil
	}
	r.pcapCapture.running = false
	r.cancelPcapAutoStopLocked()
	switch r.pcapCapture.backend {
	case "pktmon":
		if _, err := runCmd("pktmon", "stop"); err != nil {
			return err
		}
		if r.pcapCapture.etlPath == "" || r.pcapCapture.pcapngPath == "" {
			return errors.New("capture output path is empty")
		}
		return convertEtlToPcapng(r.pcapCapture.etlPath, r.pcapCapture.pcapngPath)
	case "tcpdump":
		cmd, _ := r.pcapCaptureCmd.(*exec.Cmd)
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGINT)
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-time.After(3 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			case <-done:
			}
		}
		r.pcapCaptureCmd = nil
		if r.pcapCapture.rawPath == "" {
			return errors.New("capture output path is empty")
		}
		return nil
	default:
		return errors.New("capture backend not set")
	}
}

func (r *Relay) captureDownloadPathLocked() string {
	if r.pcapCapture.backend == "tcpdump" {
		return r.pcapCapture.rawPath
	}
	return r.pcapCapture.pcapngPath
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (r *Relay) handlePcapStart(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.pcapCaptureMux.Lock()
	defer r.pcapCaptureMux.Unlock()
	if r.pcapCapture.running {
		writeJSON(w, http.StatusConflict, map[string]any{
			"ok":      false,
			"message": "capture is already running",
		})
		return
	}
	if err := r.startPcapCapture(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"ok":      false,
			"message": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"running":     r.pcapCapture.running,
		"backend":     r.pcapCapture.backend,
		"ports":       r.pcapCapture.ports,
		"raw_path":    r.pcapCapture.rawPath,
		"etl_path":    r.pcapCapture.etlPath,
		"pcapng_path": r.pcapCapture.pcapngPath,
		"started_at":  r.pcapCapture.startedAt.Format(time.RFC3339),
		"deadline_at": r.pcapCapture.deadlineAt.Format(time.RFC3339),
	})
}

func (r *Relay) handlePcapStop(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.pcapCaptureMux.Lock()
	defer r.pcapCaptureMux.Unlock()
	if err := r.stopPcapCaptureLocked(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"ok":      false,
			"message": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"running":     r.pcapCapture.running,
		"backend":     r.pcapCapture.backend,
		"pcapng_path": r.pcapCapture.pcapngPath,
		"deadline_at": r.pcapCapture.deadlineAt.Format(time.RFC3339),
	})
}

func (r *Relay) handlePcapStatus(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.pcapCaptureMux.Lock()
	defer r.pcapCaptureMux.Unlock()
	size := int64(0)
	downloadPath := r.captureDownloadPathLocked()
	if downloadPath != "" {
		if fi, err := os.Stat(downloadPath); err == nil {
			size = fi.Size()
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"running":       r.pcapCapture.running,
		"backend":       r.pcapCapture.backend,
		"ports":         r.pcapCapture.ports,
		"raw_path":      r.pcapCapture.rawPath,
		"etl_path":      r.pcapCapture.etlPath,
		"pcapng_path":   r.pcapCapture.pcapngPath,
		"download_path": downloadPath,
		"pcapng_size":   size,
		"started_at":    r.pcapCapture.startedAt.Format(time.RFC3339),
		"deadline_at":   r.pcapCapture.deadlineAt.Format(time.RFC3339),
		"max_seconds":   int(maxPcapCaptureDuration.Seconds()),
		"download_api":  "/debug/pcap/download",
	})
}

func (r *Relay) handlePcapDownload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.pcapCaptureMux.Lock()
	if err := r.stopPcapCaptureLocked(); err != nil {
		r.pcapCaptureMux.Unlock()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	p := r.captureDownloadPathLocked()
	filename := "relay_capture.pcapng"
	if r.pcapCapture.backend == "tcpdump" {
		filename = "relay_capture.pcap"
	}
	r.pcapCaptureMux.Unlock()
	if p == "" {
		http.Error(w, "capture file not ready", http.StatusNotFound)
		return
	}
	if _, err := os.Stat(p); err != nil {
		http.Error(w, fmt.Sprintf("capture file not found: %v", err), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	http.ServeFile(w, req, p)
}
