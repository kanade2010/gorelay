package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

type UpdateRequest struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

func verifySHA256(path string, expectedHex string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}

	actual := hex.EncodeToString(h.Sum(nil))
	if actual != expectedHex {
		return fmt.Errorf("expected %s but got %s", expectedHex, actual)
	}
	return nil
}

func handleInstallUpdate(w http.ResponseWriter, req *http.Request) {
	var body UpdateRequest

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil || body.URL == "" {
		http.Error(w, "invalid json or missing url", 400)
		return
	}

	exePath, err := os.Executable()
	if err != nil {
		http.Error(w, fmt.Sprintf("get exe path failed: %v", err), 500)
		return
	}

	dir := filepath.Dir(exePath)

	tmpFile, err := os.CreateTemp(dir, "update-*")
	if err != nil {
		http.Error(w, fmt.Sprintf("create temp file failed: %v", err), 500)
		return
	}
	defer tmpFile.Close()

	resp, err := http.Get(body.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("download failed: %v", err), 500)
		return
	}
	defer resp.Body.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		http.Error(w, fmt.Sprintf("write temp file failed: %v", err), 500)
		return
	}

	if body.SHA256 != "" {
		if err := verifySHA256(tmpFile.Name(), body.SHA256); err != nil {
			http.Error(w, fmt.Sprintf("sha256 verification failed: %v", err), 400)
			return
		}
		log.Println("sha256 verification passed")
	}

	if err := os.Rename(tmpFile.Name(), exePath); err != nil {
		http.Error(w, fmt.Sprintf("replace exe failed: %v", err), 500)
		return
	}

	err = os.Chmod(exePath, 0755)
	if err != nil {
		http.Error(w, "chmod setting failed : "+err.Error(), 500)
		return
	}

	w.Write([]byte("update downloaded and replaced, please restart manually : " + exePath))
}
