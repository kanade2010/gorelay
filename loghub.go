package main

import (
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type LogHub struct {
	mu     sync.Mutex
	subs   map[*websocket.Conn]bool
	buffer []string
	maxBuf int
}

func NewLogHub(max int) *LogHub {
	return &LogHub{
		subs:   make(map[*websocket.Conn]bool),
		buffer: make([]string, 0, max),
		maxBuf: max,
	}
}

func (h *LogHub) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")

	h.mu.Lock()
	if len(h.buffer) >= h.maxBuf {
		h.buffer = h.buffer[1:]
	}
	h.buffer = append(h.buffer, msg)

	for ws := range h.subs {
		ws.WriteMessage(websocket.TextMessage, []byte(msg))
	}
	h.mu.Unlock()

	return len(p), nil
}

func (h *LogHub) handleWS(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	h.mu.Lock()
	h.subs[ws] = true
	for _, line := range h.buffer {
		ws.WriteMessage(websocket.TextMessage, []byte(line))
	}
	h.mu.Unlock()

	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			break
		}
	}

	h.mu.Lock()
	delete(h.subs, ws)
	h.mu.Unlock()
	ws.Close()
}

func logPage(w http.ResponseWriter, r *http.Request) {
	html := `
<!DOCTYPE html>
<html>
<head>
<title>Relay Logs</title>
<style>
body { background:#111; color:#0f0; font-family: monospace; }
#log { white-space: pre-wrap; }
#filterBox { position:sticky; top:0; z-index:10; margin-bottom:10px; padding:8px 0; background:#111; border-bottom:1px solid #1f1f1f; }
#filterInput { width:320px; background:#000; color:#0f0; border:1px solid #0f0; padding:4px 6px; }
#pauseBtn { margin-left:8px; background:#000; color:#0f0; border:1px solid #0f0; padding:4px 10px; cursor:pointer; }
#clearBtn { margin-left:8px; background:#000; color:#0f0; border:1px solid #0f0; padding:4px 10px; cursor:pointer; }
.line-err { color:#ff4d4f; }
</style>
</head>
<body>
<h2>Relay Live Logs</h2>
<div id="filterBox">
  <label for="filterInput">过滤：</label>
  <input id="filterInput" type="text" placeholder="输入关键词过滤日志">
  <button id="pauseBtn" type="button">暂停</button>
  <button id="clearBtn" type="button">清空缓存</button>
</div>
<div id="log"></div>

<script>
const logDiv = document.getElementById("log");
const filterInput = document.getElementById("filterInput");
const pauseBtn = document.getElementById("pauseBtn");
const clearBtn = document.getElementById("clearBtn");
const allLines = [];
const maxCacheLines = 3000;
const maxCacheChars = 300000;
const maxFilteredCacheLines = 500;
const filteredLines = [];
let cacheChars = 0;
let paused = false;
function esc(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function stripAnsi(s) {
  // Real ANSI sequences, e.g. "\x1b[31m...\x1b[0m"
  let out = s.replace(/\u001b\[[0-9;]*m/g, "");
  // Fallback for already-broken textual markers seen in some browsers/log chains.
  out = out.replace(/\[31m/g, "").replace(/\[0m/g, "");
  return out;
}
function refillFilteredCache(keyword) {
  filteredLines.length = 0;
  if (!keyword) {
    return;
  }
  for (let i = allLines.length - 1; i >= 0 && filteredLines.length < maxFilteredCacheLines; i--) {
    const line = allLines[i];
    if (line.toLowerCase().includes(keyword)) {
      filteredLines.push(line);
    }
  }
  filteredLines.reverse();
}
function render() {
  const keyword = filterInput.value.trim().toLowerCase();
  if (keyword) {
    refillFilteredCache(keyword);
  }
  const rows = keyword ? filteredLines : allLines;
  logDiv.innerHTML = rows.map(line => {
    const isAnsiRed = /\u001b\[31m/.test(line) || line.includes("[31m");
    const klass = (line.includes("[ERROR]") || isAnsiRed) ? "line-err" : "";
    return "<div class=\"" + klass + "\">" + esc(stripAnsi(line)) + "</div>";
  }).join("");
}
filterInput.addEventListener("input", render);
pauseBtn.addEventListener("click", () => {
  paused = !paused;
  pauseBtn.textContent = paused ? "继续" : "暂停";
  if (!paused) {
    render();
    window.scrollTo(0, document.body.scrollHeight);
  }
});
clearBtn.addEventListener("click", () => {
  allLines.length = 0;
  filteredLines.length = 0;
  cacheChars = 0;
  render();
});
let ws = new WebSocket("ws://" + location.host + "/debug/logs");
ws.onmessage = e => {
  const atBottom = (window.innerHeight + window.scrollY) >= (document.body.scrollHeight - 20);
  const line = e.data || "";
  const keyword = filterInput.value.trim().toLowerCase();
  allLines.push(line);
  cacheChars += line.length;
  while (allLines.length > maxCacheLines || cacheChars > maxCacheChars) {
    const dropped = allLines.shift();
    if (dropped) {
      cacheChars -= dropped.length;
    }
  }
  if (keyword && line.toLowerCase().includes(keyword)) {
    filteredLines.push(line);
    while (filteredLines.length > maxFilteredCacheLines) {
      filteredLines.shift();
    }
  }
  if (paused) {
    return;
  }
  render();
  if (atBottom) {
    window.scrollTo(0, document.body.scrollHeight);
  }
};
</script>
</body>
</html>
`
	w.Write([]byte(html))
}
