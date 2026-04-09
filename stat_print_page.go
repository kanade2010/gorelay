package main

import (
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func latestLoss(rowsAny any, preferEgress bool) string {
	rows, ok := rowsAny.([]map[string]any)
	if !ok || len(rows) == 0 {
		return "0 %"
	}
	last := rows[len(rows)-1]
	loss := "0 %"
	if preferEgress {
		if v, ok := last["egress_loss_est_nack"].(string); ok && v != "" {
			loss = v
		}
	}
	if loss == "0 %" {
		if v, ok := last["loss_rate"].(string); ok && v != "" {
			loss = v
		}
	}
	return loss
}

func latestKeyframeReqCount(rowsAny any) (uint64, uint64) {
	rows, ok := rowsAny.([]map[string]any)
	if !ok || len(rows) == 0 {
		return 0, 0
	}
	last := rows[len(rows)-1]
	pli, _ := last["pli_req"].(uint64)
	fir, _ := last["fir_req"].(uint64)
	return pli, fir
}

func mergedKeyframeReqLikeNetworkPage(exported map[string]any, addr string) string {
	rtpAddr, rtcpAddr := addrPairForRtt(addr)
	rPli, rFir := latestKeyframeReqCount(exported[rtpAddr])
	cPli, cFir := latestKeyframeReqCount(exported[rtcpAddr])
	pli := rPli
	fir := rFir
	if cPli > pli {
		pli = cPli
	}
	if cFir > fir {
		fir = cFir
	}
	return fmt.Sprintf("P:%d F:%d", pli, fir)
}

func parseMetric(v string) float64 {
	fs := strings.Fields(strings.TrimSpace(v))
	if len(fs) == 0 {
		return 0
	}
	f, err := strconv.ParseFloat(fs[0], 64)
	if err != nil {
		return 0
	}
	return f
}

func addrPairForRtt(addr string) (string, string) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return addr, ""
	}
	base := port
	if port%2 == 1 {
		base = port - 1
	}
	return net.JoinHostPort(host, strconv.Itoa(base)), net.JoinHostPort(host, strconv.Itoa(base+1))
}

func latestRttFromRows(rowsAny any) (float64, string) {
	rows, ok := rowsAny.([]map[string]any)
	if !ok || len(rows) == 0 {
		return 0, "0 ms"
	}
	last := rows[len(rows)-1]
	v, ok := last["rtt"].(string)
	if !ok || v == "" {
		return 0, "0 ms"
	}
	n := parseMetric(v)
	if n <= 0 {
		return 0, "0 ms"
	}
	return n, v
}

func mergedRttLikeNetworkPage(exported map[string]any, addr string) (float64, string) {
	rtpAddr, rtcpAddr := addrPairForRtt(addr)
	rtpN, rtpText := latestRttFromRows(exported[rtpAddr])
	if rtpN > 0 {
		return rtpN, rtpText
	}
	if rtcpAddr != "" {
		rtcpN, rtcpText := latestRttFromRows(exported[rtcpAddr])
		if rtcpN > 0 {
			return rtcpN, rtcpText
		}
	}
	return 0, "0 ms"
}

func segmentRttBidirectional(exported map[string]any, a string, b string) string {
	aN, aText := mergedRttLikeNetworkPage(exported, a)
	if aN > 0 {
		return aText
	}
	bN, bText := mergedRttLikeNetworkPage(exported, b)
	if bN > 0 {
		return bText
	}
	return "0 ms"
}

func (r *Relay) relayDisplayAddr(req *http.Request) string {
	listen := r.cfg.ListenRTP
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		reqHost := req.Host
		if h, _, e := net.SplitHostPort(reqHost); e == nil {
			host = h
		} else {
			host = reqHost
		}
	}
	return net.JoinHostPort(host, port)
}

func (r *Relay) handleStatPrintData(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	n := 10
	if v := req.URL.Query().Get("count"); v != "" {
		if iv, err := strconv.Atoi(v); err == nil && iv > 0 {
			n = iv
		}
	}

	exported := r.weakStats.Export(n)
	relayAddr := r.relayDisplayAddr(req)

	r.addrSsrcMutex.RLock()
	addrSsrcCopy := make(map[string][]uint32, len(r.addrSsrc))
	for k, v := range r.addrSsrc {
		addrSsrcCopy[k] = append([]uint32(nil), v...)
	}
	r.addrSsrcMutex.RUnlock()

	r.mapMutex.RLock()
	mappingCopy := make(map[uint32][]string, len(r.mapping))
	for ssrc, targets := range r.mapping {
		for _, t := range targets {
			if t != nil {
				mappingCopy[ssrc] = append(mappingCopy[ssrc], t.String())
			}
		}
	}
	r.mapMutex.RUnlock()

	r.ssrcDirectMux.RLock()
	directCopy := make(map[uint32]uint32, len(r.ssrcDirectMapping))
	for k, v := range r.ssrcDirectMapping {
		directCopy[k] = v
	}
	r.ssrcDirectMux.RUnlock()

	srcAddrs := make([]string, 0, len(addrSsrcCopy))
	for src := range addrSsrcCopy {
		srcAddrs = append(srcAddrs, src)
	}
	sort.Strings(srcAddrs)

	lines := make([]string, 0, 64)
	for _, src := range srcAddrs {
		ssrcs := addrSsrcCopy[src]
		sort.Slice(ssrcs, func(i, j int) bool { return ssrcs[i] < ssrcs[j] })

		srcLoss := latestLoss(exported[src], false)
		srcKeyReq := mergedKeyframeReqLikeNetworkPage(exported, src)
		for _, ssrc := range ssrcs {
			targets := append([]string(nil), mappingCopy[ssrc]...)
			sort.Strings(targets)
			direct := directCopy[ssrc]
			srcRelayRtt := segmentRttBidirectional(exported, src, relayAddr)
			if len(targets) == 0 {
				lines = append(lines, fmt.Sprintf("Target: - <-- {0 ms} [0 %%] [KF P:0 F:0] -- %s <-- {%s} [%s] [KF %s] -- Source: %s (SSRC: %d Direct From: %d)", relayAddr, srcRelayRtt, srcLoss, srcKeyReq, src, ssrc, direct))
				continue
			}
			for _, target := range targets {
				tLoss := latestLoss(exported[target], true)
				tKeyReq := mergedKeyframeReqLikeNetworkPage(exported, target)
				tRelayRtt := segmentRttBidirectional(exported, target, relayAddr)
				lines = append(lines, fmt.Sprintf("Target: %s <-- {%s} [%s] [KF %s] -- %s <-- {%s} [%s] [KF %s] -- Source: %s (SSRC: %d Direct From: %d)", target, tRelayRtt, tLoss, tKeyReq, relayAddr, srcRelayRtt, srcLoss, srcKeyReq, src, ssrc, direct))
			}
		}
	}

	if len(lines) == 0 {
		w.Write([]byte("暂无数据"))
		return
	}
	w.Write([]byte(strings.Join(lines, "\n")))
}

func (r *Relay) handleStatPrintPage(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(`<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stat Print</title>
<style>
body{font-family:Consolas,monospace;background:#020617;color:#e2e8f0;margin:0}
.wrap{padding:14px}
.head{display:flex;gap:10px;align-items:center;position:sticky;top:0;background:#020617;padding-bottom:10px}
button,input{background:#0b1220;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:6px 10px}
pre{white-space:pre-wrap;word-break:break-word;background:#0b1220;border:1px solid #1e293b;border-radius:10px;padding:12px;line-height:1.65}
.val-alert{color:#ef4444;font-weight:700}
</style>
</head>
<body>
<div class="wrap">
  <div class="head">
    <strong>实时 Stat 打印</strong>
    <label>周期(ms) <input id="interval" type="number" value="1000" min="200" step="100"></label>
    <button id="toggle">暂停</button>
    <span id="time"></span>
  </div>
  <pre id="out">加载中...</pre>
</div>
<script>
const out=document.getElementById('out')
const intervalInput=document.getElementById('interval')
const toggle=document.getElementById('toggle')
const t=document.getElementById('time')
let timer=null
let paused=false
function esc(s){
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
}
function highlightLine(line){
  let html=esc(line)
  html=html.replace(/\[(\d+(?:\.\d+)?)\s*%\]/g,(_,num)=>{
    const n=Number(num)||0
    if(n>0){
      return '[<span class="val-alert">'+num+'%</span>]'
    }
    return '['+num+'%]'
  })
  html=html.replace(/\[KF P:(\d+)\s+F:(\d+)\]/g,(_,p,f)=>{
    const pv=Number(p)||0
    const fv=Number(f)||0
    const ps=pv>0?'<span class="val-alert">'+p+'</span>':p
    const fs=fv>0?'<span class="val-alert">'+f+'</span>':f
    return '[KF P:'+ps+' F:'+fs+']'
  })
  return html
}
function renderText(txt){
  if(!txt){
    out.textContent='暂无数据'
    return
  }
  const lines=txt.split('\n')
  out.innerHTML=lines.map(line=>highlightLine(line)).join('\n')
}
async function refresh(){
  try{
    const r=await fetch('/debug/stat/print/data?count=5',{cache:'no-store'})
    const txt=await r.text()
    renderText(txt)
    t.textContent='更新时间 '+new Date().toLocaleTimeString()
  }catch(e){
    out.textContent='拉取失败: '+String(e)
  }
}
function restart(){
  if(timer) clearInterval(timer)
  if(paused) return
  const ms=Math.max(200,Number(intervalInput.value)||1000)
  timer=setInterval(refresh,ms)
}
toggle.onclick=()=>{
  paused=!paused
  toggle.textContent=paused?'继续':'暂停'
  if(paused){
    if(timer) clearInterval(timer)
    timer=null
  }else{
    refresh()
    restart()
  }
}
intervalInput.onchange=()=>restart()
refresh()
restart()
</script>
</body>
</html>`))
}
