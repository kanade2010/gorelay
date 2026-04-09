package main

import "net/http"

func (r *Relay) handleNetworkPage(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(`<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Network Stat</title>
<style>
body{font-family:Arial,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;padding:0}
.wrap{max-width:1400px;margin:0 auto;padding:16px}
.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;background:#111827;padding:10px 12px;border:1px solid #1f2937;border-radius:10px;position:sticky;top:0;z-index:9}
.toolbar input,.toolbar select,.toolbar button{background:#0b1220;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:6px 10px}
.toolbar button{cursor:pointer}
.status{font-size:12px;padding:2px 8px;border-radius:999px;border:1px solid #334155}
.status.ok{color:#22c55e;border-color:#166534}
.status.err{color:#ef4444;border-color:#7f1d1d}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:12px;margin-top:12px}
.card{background:#111827;border:1px solid #1f2937;border-radius:10px;padding:12px}
.head{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px}
.head-right{display:flex;align-items:center;gap:8px}
.addr{font-size:13px;color:#93c5fd;word-break:break-all}
.level{font-size:12px;padding:2px 8px;border-radius:999px}
.level.good{background:#052e16;color:#22c55e}
.level.warn{background:#3f2a00;color:#f59e0b}
.level.bad{background:#450a0a;color:#ef4444}
.zoom-btn{font-size:12px;line-height:1;padding:5px 8px;border-radius:8px;background:#0b1220;color:#93c5fd;border:1px solid #334155;cursor:pointer}
.kv{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:8px;margin-bottom:10px}
.kv div{background:#0b1220;border:1px solid #1f2937;border-radius:8px;padding:6px 8px}
.k{font-size:11px;color:#94a3b8}
.v{font-size:14px;font-weight:600}
.canvas-box{height:170px;background:#0b1220;border:1px solid #1f2937;border-radius:8px;padding:4px}
.legend{display:flex;gap:10px;font-size:12px;color:#94a3b8;margin-top:8px}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block}
.dot-rtt{background:#22d3ee}.dot-jit{background:#a78bfa}.dot-loss{background:#f43f5e}
.dot-txloss{background:#f59e0b}
.empty{margin-top:12px;padding:24px;text-align:center;color:#94a3b8;background:#111827;border:1px dashed #334155;border-radius:10px}
.modal{position:fixed;inset:0;background:rgba(2,6,23,.7);display:none;align-items:center;justify-content:center;padding:20px;z-index:99}
.modal.open{display:flex}
.modal-card{width:min(1100px,95vw);max-height:92vh;overflow:auto;background:#111827;border:1px solid #334155;border-radius:12px;padding:14px}
.modal-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.modal-close{background:#0b1220;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:6px 10px;cursor:pointer}
.modal-charts{display:grid;grid-template-columns:1fr;gap:10px}
.metric-row{display:grid;grid-template-columns:120px 1fr 70px;gap:8px;align-items:stretch}
.metric-side,.metric-axis{background:#0b1220;border:1px solid #1f2937;border-radius:8px;padding:8px}
.metric-name{font-size:12px;color:#94a3b8}
.metric-now{font-size:18px;font-weight:700;margin-top:6px}
.metric-unit{font-size:12px;color:#94a3b8}
.metric-plot{height:170px;background:#0b1220;border:1px solid #1f2937;border-radius:8px;padding:4px;overflow:hidden}
.metric-plot canvas{display:block}
.axis-col{height:100%;display:flex;flex-direction:column;justify-content:space-between;font-size:12px;color:#93c5fd;text-align:right}
</style>
</head>
<body>
<div class="wrap">
  <div class="toolbar">
    <strong>Network Stat</strong>
    <label>刷新(ms)<input id="interval" type="number" min="200" step="100" value="1000"></label>
    <label>窗口<select id="count"><option value="30">30</option><option value="60" selected>60</option><option value="120">120</option></select></label>
    <button id="toggle">暂停</button>
    <button id="refresh">立即刷新</button>
    <span id="fetchState" class="status ok">运行中</span>
    <span id="updatedAt" style="font-size:12px;color:#94a3b8"></span>
  </div>
  <div id="cards" class="grid"></div>
  <div id="emptyState" class="empty" style="display:none">当前没有 RTT 大于 0 的端点</div>
</div>
<div id="zoomModal" class="modal">
  <div class="modal-card">
    <div class="modal-head">
      <strong id="modalTitle">端点详情</strong>
      <button id="modalClose" class="modal-close">关闭</button>
    </div>
    <div id="modalKv" class="kv"></div>
    <div id="modalCharts" class="modal-charts"></div>
  </div>
</div>
<script>
const qs=new URLSearchParams(location.search)
const intervalInput=document.getElementById('interval')
const countSelect=document.getElementById('count')
const toggleBtn=document.getElementById('toggle')
const refreshBtn=document.getElementById('refresh')
const fetchState=document.getElementById('fetchState')
const updatedAt=document.getElementById('updatedAt')
const cards=document.getElementById('cards')
const emptyState=document.getElementById('emptyState')
const zoomModal=document.getElementById('zoomModal')
const modalTitle=document.getElementById('modalTitle')
const modalKv=document.getElementById('modalKv')
const modalCharts=document.getElementById('modalCharts')
const modalClose=document.getElementById('modalClose')
let timer=null
let paused=false
let lastData={}
let mergedCache={}
let expandedEndpoint=''
intervalInput.value=qs.get('interval')||'1000'
if(qs.get('count')) countSelect.value=qs.get('count')
function parseMetric(v){
  if(typeof v!=='string') return 0
  const m=v.match(/-?\d+(\.\d+)?/)
  if(!m) return 0
  return Number(m[0])||0
}
function splitAddr(addr){
  if(typeof addr!=='string') return null
  const i=addr.lastIndexOf(':')
  if(i<0) return null
  const host=addr.slice(0,i)
  const port=Number(addr.slice(i+1))
  if(!Number.isFinite(port)) return null
  return {host,port}
}
function groupByRtpEndpoint(data){
  const grouped={}
  for(const addr of Object.keys(data||{})){
    const p=splitAddr(addr)
    if(!p) continue
    const base=p.port%2===1?p.port-1:p.port
    const rtpKey=p.host+':'+base
    if(!grouped[rtpKey]) grouped[rtpKey]={rtp:[],rtcp:[]}
    if(p.port===base) grouped[rtpKey].rtp=data[addr]||[]
    else if(p.port===base+1) grouped[rtpKey].rtcp=data[addr]||[]
  }
  return grouped
}
function indexByEnd(rows){
  const m={}
  ;(rows||[]).forEach(x=>{ if(x&&x.end) m[x.end]=x })
  return m
}
function mergeSeries(rtpRows,rtcpRows){
  const rtpMap=indexByEnd(rtpRows)
  const rtcpMap=indexByEnd(rtcpRows)
  const keys=[...new Set([...Object.keys(rtpMap),...Object.keys(rtcpMap)])].sort()
  const rows=[]
  let keyReqCarry=0
  let pliReqCarry=0
  let firReqCarry=0
  for(const k of keys){
    const rtp=rtpMap[k]||{}
    const rtcp=rtcpMap[k]||{}
    const rtpRtt=parseMetric(rtp.rtt)
    const rtcpRtt=parseMetric(rtcp.rtt)
    const rtt=rtpRtt>0?rtpRtt:rtcpRtt
    const keyReqNow=Number(rtcp.keyframe_req)
    const pliReqNow=Number(rtcp.pli_req)
    const firReqNow=Number(rtcp.fir_req)
    if(Number.isFinite(keyReqNow) && keyReqNow>=0) keyReqCarry=keyReqNow
    if(Number.isFinite(pliReqNow) && pliReqNow>=0) pliReqCarry=pliReqNow
    if(Number.isFinite(firReqNow) && firReqNow>=0) firReqCarry=firReqNow
    rows.push({
      end:k,
      rtt:rtt,
      jitter:parseMetric(rtp.jitter),
      loss:parseMetric(rtp.loss_rate),
      txLoss:parseMetric(rtp.egress_loss_est_nack),
      keyReq:keyReqCarry,
      pliReq:pliReqCarry,
      firReq:firReqCarry,
      recv:Number(rtp.received||0),
      lost:Number(rtp.lost||0),
      retrans:Number(rtp.retrans||0),
      bitrate:parseMetric(rtp.bitrate),
      rttText:rtt>0?(Math.round(rtt*100)/100)+' ms':'0 ms',
      jitterText:rtp.jitter||'0 ms',
      lossText:rtp.loss_rate||'0%',
      txLossText:rtp.egress_loss_est_nack||'0%',
      keyReqText:String(keyReqCarry),
      pliReqText:String(pliReqCarry),
      firReqText:String(firReqCarry),
      bitrateText:rtp.bitrate||'0 kbps'
    })
  }
  return rows
}
function hasPositiveRTT(rows){
  return (rows||[]).some(x=>Number(x.rtt)>0)
}
function formatNum(v){
  if(!Number.isFinite(v)) return '0'
  return String(Math.round(v*100)/100)
}
function levelOf(rtt,jitter,loss){
  const bad=rtt>150||jitter>40||loss>5
  if(bad) return {key:'bad',text:'差'}
  const warn=(rtt>=80&&rtt<=150)||(jitter>=15&&jitter<=40)||(loss>=1&&loss<=5)
  if(warn) return {key:'warn',text:'中'}
  return {key:'good',text:'优'}
}
function calcRange(){
  const values=[...arguments].flat().filter(v=>Number.isFinite(v))
  if(values.length===0) return {min:0,max:1}
  let min=Math.min(0,...values)
  let max=Math.max(1,...values)
  if(max-min<1) max=min+1
  const pad=(max-min)*0.15
  return {min:min-pad,max:max+pad}
}
function drawLine(ctx,w,h,arr,color,minY,maxY,pad){
  const innerH=h-pad*2
  ctx.strokeStyle=color
  ctx.lineWidth=2
  ctx.beginPath()
  const n=arr.length
  if(n===0){ctx.stroke();return}
  for(let i=0;i<n;i++){
    const x=n===1?0:(i*(w-1)/(n-1))
    const v=arr[i]
    const y=pad+((maxY-v)/(maxY-minY))*innerH
    if(i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y)
  }
  ctx.stroke()
}
function drawLabel(ctx,text,x,y,color){
  ctx.font='12px Arial'
  const tw=ctx.measureText(text).width
  const px=Math.max(4,Math.min(x,wCache-tw-8))
  const py=Math.max(14,Math.min(y,hCache-4))
  ctx.fillStyle='rgba(2,6,23,0.85)'
  ctx.fillRect(px-4,py-12,tw+8,16)
  ctx.fillStyle=color
  ctx.fillText(text,px,py)
}
let wCache=0
let hCache=0
function pointY(v,minY,maxY,pad,h){
  const innerH=h-pad*2
  return pad+((maxY-v)/(maxY-minY))*innerH
}
function drawChart(canvas,rtt,jitter,loss,txLoss,last){
  const p=canvas.parentElement
  const pw=p?Math.max(0,p.clientWidth-8):800
  const ph=p?Math.max(0,p.clientHeight-8):360
  const box={width:Math.min(1400,Math.max(300,pw)),height:Math.min(520,Math.max(150,ph))}
  const dpr=window.devicePixelRatio||1
  canvas.style.width=box.width+'px'
  canvas.style.height=box.height+'px'
  canvas.width=Math.floor(box.width*dpr)
  canvas.height=Math.floor(box.height*dpr)
  const ctx=canvas.getContext('2d')
  ctx.setTransform(dpr,0,0,dpr,0,0)
  const w=box.width
  const h=box.height
  wCache=w
  hCache=h
  const pad=10
  ctx.clearRect(0,0,w,h)
  ctx.strokeStyle='#1f2937'
  ctx.lineWidth=1
  for(let i=1;i<4;i++){
    const y=i*h/4
    ctx.beginPath()
    ctx.moveTo(0,y)
    ctx.lineTo(w,y)
    ctx.stroke()
  }
  const range=calcRange(rtt,jitter,loss,txLoss)
  drawLine(ctx,w,h,rtt,'#22d3ee',range.min,range.max,pad)
  drawLine(ctx,w,h,jitter,'#a78bfa',range.min,range.max,pad)
  drawLine(ctx,w,h,loss,'#f43f5e',range.min,range.max,pad)
  drawLine(ctx,w,h,txLoss,'#f59e0b',range.min,range.max,pad)
  ctx.fillStyle='#22d3ee'
  if(rtt.length===1){
    const y=pointY(rtt[0],range.min,range.max,pad,h)
    ctx.beginPath(); ctx.arc(w-2,y,3,0,Math.PI*2); ctx.fill()
  }
  ctx.fillStyle='#a78bfa'
  if(jitter.length===1){
    const y=pointY(jitter[0],range.min,range.max,pad,h)
    ctx.beginPath(); ctx.arc(w-10,y,3,0,Math.PI*2); ctx.fill()
  }
  ctx.fillStyle='#f43f5e'
  if(loss.length===1){
    const y=pointY(loss[0],range.min,range.max,pad,h)
    ctx.beginPath(); ctx.arc(w-18,y,3,0,Math.PI*2); ctx.fill()
  }
  if(rtt.length>0) drawLabel(ctx,'RTT '+formatNum(rtt[rtt.length-1])+'ms',w-120,pointY(rtt[rtt.length-1],range.min,range.max,pad,h)-6,'#22d3ee')
  if(jitter.length>0) drawLabel(ctx,'JIT '+formatNum(jitter[jitter.length-1])+'ms',w-120,pointY(jitter[jitter.length-1],range.min,range.max,pad,h)+12,'#a78bfa')
  if(loss.length>0) drawLabel(ctx,'LOSS '+formatNum(loss[loss.length-1])+'%',w-120,pointY(loss[loss.length-1],range.min,range.max,pad,h)+30,'#f43f5e')
  if(txLoss.length>0) drawLabel(ctx,'TX '+formatNum(txLoss[txLoss.length-1])+'%',w-120,pointY(txLoss[txLoss.length-1],range.min,range.max,pad,h)+48,'#f59e0b')
  if(last&&last.end){
    ctx.font='11px Arial'
    ctx.fillStyle='#94a3b8'
    ctx.fillText(last.end.slice(11,19),8,h-6)
  }
  return range
}
function drawSingleChart(canvas,arr,color,last,unit,endText){
  const p=canvas.parentElement
  const pw=p?Math.max(0,p.clientWidth-8):800
  const ph=p?Math.max(0,p.clientHeight-8):160
  const box={width:Math.min(1400,Math.max(300,pw)),height:Math.min(260,Math.max(120,ph))}
  const dpr=window.devicePixelRatio||1
  canvas.style.width=box.width+'px'
  canvas.style.height=box.height+'px'
  canvas.width=Math.floor(box.width*dpr)
  canvas.height=Math.floor(box.height*dpr)
  const ctx=canvas.getContext('2d')
  ctx.setTransform(dpr,0,0,dpr,0,0)
  const w=box.width
  const h=box.height
  const pad=10
  ctx.clearRect(0,0,w,h)
  ctx.strokeStyle='#1f2937'
  ctx.lineWidth=1
  for(let i=1;i<4;i++){
    const y=i*h/4
    ctx.beginPath()
    ctx.moveTo(0,y)
    ctx.lineTo(w,y)
    ctx.stroke()
  }
  const range=calcRange(arr,[],[])
  drawLine(ctx,w,h,arr,color,range.min,range.max,pad)
  if(arr.length===1){
    const y=pointY(arr[0],range.min,range.max,pad,h)
    ctx.fillStyle=color
    ctx.beginPath(); ctx.arc(w-2,y,3,0,Math.PI*2); ctx.fill()
  }
  if(arr.length>0){
    const now=arr[arr.length-1]
    drawLabel(ctx,(unit==='%'?formatNum(now)+unit:formatNum(now)+' '+unit),w-120,pointY(now,range.min,range.max,pad,h)-6,color)
  }
  if(endText){
    ctx.font='11px Arial'
    ctx.fillStyle='#94a3b8'
    ctx.fillText(endText.slice(11,19),8,h-6)
  }
  return range
}
function renderZoom(addr){
  const merged=mergedCache[addr]||[]
  if(merged.length===0) return
  const last=merged[merged.length-1]
  modalTitle.textContent='端点 '+addr
  modalKv.innerHTML=
    '<div><div class="k">RTT</div><div class="v">'+last.rttText+'</div></div>'+
    '<div><div class="k">Jitter</div><div class="v">'+last.jitterText+'</div></div>'+
    '<div><div class="k">Loss</div><div class="v">'+last.lossText+'</div></div>'+
    '<div><div class="k">发送丢包</div><div class="v">'+last.txLossText+'</div></div>'+
    '<div><div class="k">关键帧请求</div><div class="v">'+last.keyReqText+' (P'+last.pliReqText+'/F'+last.firReqText+')</div></div>'+
    '<div><div class="k">Recv/Lost</div><div class="v">'+last.recv+'/'+last.lost+'</div></div>'+
    '<div><div class="k">Bitrate</div><div class="v">'+last.bitrateText+'</div></div>'
  const rtt=merged.map(x=>x.rtt)
  const jitter=merged.map(x=>x.jitter)
  const loss=merged.map(x=>x.loss)
  const txLoss=merged.map(x=>x.txLoss)
  modalCharts.innerHTML=
    '<div class="metric-row">'+
      '<div class="metric-side"><div class="metric-name">RTT</div><div class="metric-now">'+last.rttText+'</div><div class="metric-unit">ms</div></div>'+
      '<div class="metric-plot"><canvas id="metricRtt"></canvas></div>'+
      '<div class="metric-axis"><div class="axis-col"><span id="axisRttMax">-</span><span id="axisRttMid">-</span><span id="axisRttMin">-</span></div></div>'+
    '</div>'+
    '<div class="metric-row">'+
      '<div class="metric-side"><div class="metric-name">Jitter</div><div class="metric-now">'+last.jitterText+'</div><div class="metric-unit">ms</div></div>'+
      '<div class="metric-plot"><canvas id="metricJit"></canvas></div>'+
      '<div class="metric-axis"><div class="axis-col"><span id="axisJitMax">-</span><span id="axisJitMid">-</span><span id="axisJitMin">-</span></div></div>'+
    '</div>'+
    '<div class="metric-row">'+
      '<div class="metric-side"><div class="metric-name">Loss</div><div class="metric-now">'+last.lossText+'</div><div class="metric-unit">%</div></div>'+
      '<div class="metric-plot"><canvas id="metricLoss"></canvas></div>'+
      '<div class="metric-axis"><div class="axis-col"><span id="axisLossMax">-</span><span id="axisLossMid">-</span><span id="axisLossMin">-</span></div></div>'+
    '</div>'+
    '<div class="metric-row">'+
      '<div class="metric-side"><div class="metric-name">发送丢包</div><div class="metric-now">'+last.txLossText+'</div><div class="metric-unit">%</div></div>'+
      '<div class="metric-plot"><canvas id="metricTxLoss"></canvas></div>'+
      '<div class="metric-axis"><div class="axis-col"><span id="axisTxLossMax">-</span><span id="axisTxLossMid">-</span><span id="axisTxLossMin">-</span></div></div>'+
    '</div>'
  requestAnimationFrame(()=>{
    const rr=drawSingleChart(document.getElementById('metricRtt'),rtt,'#22d3ee',last.rtt,'ms',last.end)
    const jr=drawSingleChart(document.getElementById('metricJit'),jitter,'#a78bfa',last.jitter,'ms',last.end)
    const lr=drawSingleChart(document.getElementById('metricLoss'),loss,'#f43f5e',last.loss,'%',last.end)
    const tr=drawSingleChart(document.getElementById('metricTxLoss'),txLoss,'#f59e0b',last.txLoss,'%',last.end)
    document.getElementById('axisRttMax').textContent=formatNum(rr.max)+' ms'
    document.getElementById('axisRttMid').textContent=formatNum((rr.max+rr.min)/2)+' ms'
    document.getElementById('axisRttMin').textContent=formatNum(rr.min)+' ms'
    document.getElementById('axisJitMax').textContent=formatNum(jr.max)+' ms'
    document.getElementById('axisJitMid').textContent=formatNum((jr.max+jr.min)/2)+' ms'
    document.getElementById('axisJitMin').textContent=formatNum(jr.min)+' ms'
    document.getElementById('axisLossMax').textContent=formatNum(lr.max)+' %'
    document.getElementById('axisLossMid').textContent=formatNum((lr.max+lr.min)/2)+' %'
    document.getElementById('axisLossMin').textContent=formatNum(lr.min)+' %'
    document.getElementById('axisTxLossMax').textContent=formatNum(tr.max)+' %'
    document.getElementById('axisTxLossMid').textContent=formatNum((tr.max+tr.min)/2)+' %'
    document.getElementById('axisTxLossMin').textContent=formatNum(tr.min)+' %'
  })
}
function openZoom(addr){
  const merged=mergedCache[addr]||[]
  if(merged.length===0) return
  expandedEndpoint=addr
  zoomModal.classList.add('open')
  renderZoom(addr)
}
function render(raw){
  const grouped=groupByRtpEndpoint(raw)
  const keys=Object.keys(grouped).sort()
  cards.innerHTML=''
  mergedCache={}
  let shown=0
  keys.forEach(addr=>{
    const mergedAll=mergeSeries(grouped[addr].rtp,grouped[addr].rtcp)
    if(!hasPositiveRTT(mergedAll)) return
    const merged=mergedAll.filter(x=>x.rtt>0)
    if(merged.length===0) return
    mergedCache[addr]=merged
    shown++
    const rtt=merged.map(x=>x.rtt)
    const jitter=merged.map(x=>x.jitter)
    const loss=merged.map(x=>x.loss)
    const txLoss=merged.map(x=>x.txLoss)
    const last=merged[merged.length-1]||{rttText:'0 ms',jitterText:'0 ms',lossText:'0%',txLossText:'0%',keyReqText:'0',pliReqText:'0',firReqText:'0',recv:0,lost:0,bitrateText:'0 kbps'}
    const lv=levelOf(parseMetric(last.rttText),parseMetric(last.jitterText),parseMetric(last.lossText))
    const div=document.createElement('div')
    div.className='card'
    div.innerHTML=
      '<div class="head"><div class="addr">'+addr+'</div><div class="head-right"><button class="zoom-btn" data-addr="'+addr+'">放大</button><span class="level '+lv.key+'">'+lv.text+'</span></div></div>'+
      '<div class="kv">'+
      '<div><div class="k">RTT</div><div class="v">'+last.rttText+'</div></div>'+
      '<div><div class="k">Jitter</div><div class="v">'+last.jitterText+'</div></div>'+
      '<div><div class="k">Loss</div><div class="v">'+last.lossText+'</div></div>'+
      '<div><div class="k">发送丢包</div><div class="v">'+last.txLossText+'</div></div>'+
      '<div><div class="k">关键帧请求</div><div class="v">'+last.keyReqText+' (P'+last.pliReqText+'/F'+last.firReqText+')</div></div>'+
      '<div><div class="k">Recv/Lost</div><div class="v">'+last.recv+'/'+last.lost+'</div></div>'+
      '<div><div class="k">Bitrate</div><div class="v">'+last.bitrateText+'</div></div>'+
      '</div>'+
      '<div class="canvas-box"><canvas></canvas></div>'+
      '<div class="legend"><span><i class="dot dot-rtt"></i> RTT</span><span><i class="dot dot-jit"></i> Jitter</span><span><i class="dot dot-loss"></i> Loss</span><span><i class="dot dot-txloss"></i> 发送丢包</span></div>'
    cards.appendChild(div)
    drawChart(div.querySelector('canvas'),rtt,jitter,loss,txLoss,last)
    div.querySelector('.zoom-btn').onclick=()=>openZoom(addr)
  })
  emptyState.style.display=shown===0?'block':'none'
  if(expandedEndpoint){
    if(mergedCache[expandedEndpoint]){
      renderZoom(expandedEndpoint)
    }else{
      zoomModal.classList.remove('open')
      expandedEndpoint=''
    }
  }
}
async function poll(){
  const count=countSelect.value||'60'
  try{
    const resp=await fetch('/debug/network?count='+encodeURIComponent(count),{cache:'no-store'})
    if(!resp.ok) throw new Error('http '+resp.status)
    const data=await resp.json()
    lastData=data||{}
    render(lastData)
    fetchState.className='status ok'
    fetchState.textContent='运行中'
    updatedAt.textContent='更新时间 '+new Date().toLocaleTimeString()
  }catch(e){
    render(lastData)
    fetchState.className='status err'
    fetchState.textContent='拉取失败'
    updatedAt.textContent='最近失败 '+new Date().toLocaleTimeString()
  }
}
function restart(){
  if(timer) clearInterval(timer)
  if(paused) return
  const t=Math.max(200,Number(intervalInput.value)||1000)
  timer=setInterval(poll,t)
}
toggleBtn.onclick=()=>{
  paused=!paused
  toggleBtn.textContent=paused?'继续':'暂停'
  if(paused){
    if(timer) clearInterval(timer)
    timer=null
    fetchState.className='status err'
    fetchState.textContent='已暂停'
  }else{
    fetchState.className='status ok'
    fetchState.textContent='运行中'
    poll()
    restart()
  }
}
refreshBtn.onclick=()=>poll()
intervalInput.onchange=()=>restart()
countSelect.onchange=()=>poll()
modalClose.onclick=()=>{ zoomModal.classList.remove('open'); expandedEndpoint='' }
zoomModal.onclick=(e)=>{ if(e.target===zoomModal){ zoomModal.classList.remove('open'); expandedEndpoint='' } }
window.onresize=()=>{ render(lastData); if(expandedEndpoint&&mergedCache[expandedEndpoint]) openZoom(expandedEndpoint) }
poll()
restart()
</script>
</body>
</html>`))
}
