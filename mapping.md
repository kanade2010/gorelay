

# mapping

```
┌────────────────────────┐
│      RTCP NACK 包      │
│   (senderSSRC, PID…)   │
└──────────┬──────────────┘
           │
           ▼
┌────────────────────────┐
│ ① 用 senderSSRC → 从 addrToSSRC 反查 │
│    找到 RTCP 发送者的 RTP 地址        │
│    mediaSenderAddr                   │
└──────────┬──────────────┘
           │
           ▼
┌────────────────────────┐
│ ② 从 addrMapping 查    │
│    mediaSenderAddr → mediaReceiverAddr │
└──────────┬──────────────┘
           │
           ▼
┌────────────────────────┐
│ ③ 从 addrToSSRC 查     │
│    mediaReceiverAddr → trueSSRC │
│ （真正需要重传的源 SSRC）      │
└──────────┬──────────────┘
           │
           ▼
┌────────────────────────┐
│ 根据 trueSSRC 在 history 里查找包  │
│ 发送重传 RTP 包                    │
└────────────────────────┘

```



rtcp senderSSRC → srcAddr

srcAddr → targetAddr

targetAddr → trueSSRC
```
                     ┌──────────────────────────┐
                     │          Relay            │
                     └──────────────────────────┘
                               ▲     ▲
                               │     │
            (3) targetAddr─────┘     │ (1) rtpSenderAddr
                               │     │
                         addrToSSRC   │
                               │     │
                               ▼     ▼
                     ┌──────────────────────────┐
                     │ RTP 源地址 → SSRC 映射     │
                     └──────────────────────────┘

                               ▲
                               │(2)
                    addrMapping[srcAddr] = targetAddr

                               ▲
                               │
                     ┌──────────────────────────┐
                     │   控制接口动态更新映射      │
                     └──────────────────────────┘

```


| 映射名称                      | Key                 | Value             | 作用                                         |
| ------------------------- | ------------------- | ----------------- | ------------------------------------------ |
| `mapping`                 | `SSRC` (uint32)     | `[]*UDPAddr`      | RTP 源 SSRC 对应的目标地址列表，用于直接转发 RTP            |
| `addrMapping`             | `srcAddr string`    | `[]*UDPAddr`      | 某个 UDP 源地址（IP:Port）对应的目标地址列表，动态学习或控制接口更新   |
| `addrToSSRC`              | `srcAddr string`    | `SSRC uint32`     | 反查 SSRC，用于 RTCP 处理 NACK 时反向找到源 SSRC        |
| `mappings`                | `SSRC uint32`       | `ControlMapping`  | 保存控制接口下发的映射配置，包含 DstIP/DstPort/DstRtcpPort |
| `ssrcDirectMapping` (建议加) | `senderSSRC uint32` | `trueSSRC uint32` | 缓存 RTCP NACK 反查结果，避免多层查找                   |


```
        +----------------+
        |   RTP Packet   |
        |  srcAddr:10.0.0.2 |
        +--------+-------+
                 |
                 v
        +----------------+
        |   addrToSSRC   |
        |  10.0.0.2 => 1111 |
        +--------+-------+
                 |
                 v
        +----------------+
        |    mapping     |
        | 1111 => [192.168.1.10:5004, 192.168.1.11:5004] |
        +----------------+
                 |
                 v
       +------------------+
       | Forward to targets|
       +------------------+



 RTCP SenderSSRC
        │
        ▼
  addrToSSRC反查得到 srcAddr
        │
        ▼
  addrMapping[srcAddr] -> targetAddr(s)
        │
        ▼
  addrToSSRC[targetAddr] -> trueSSRC (NACK真正源SSRC)
        │
        ▼
  从 history 找包 → retransmit

```