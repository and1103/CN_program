import socket
import struct
import random
import sys
import threading
import time
import queue
import pandas as pd
from datetime import datetime

TYPE_SYN, TYPE_SYN_ACK, TYPE_ACK_CONN, TYPE_DATA, TYPE_ACK = 1, 2, 3, 4, 5

INIT_TIMEOUT  = 0.3
WINDOW_SIZE   = 400
CHUNK_MIN     = 40
CHUNK_MAX     = 80
TOTAL_PACKETS = 30

STUDENT_ID_LAST4 = 2607
EXPECTED_ID = 0x5A3C
STUDENT_ID_VAL = STUDENT_ID_LAST4 ^ EXPECTED_ID

lock = threading.Lock()
base = next_seq = window_bytes = 0
ack_updated  = threading.Event()
syn_ack_rcvd = threading.Event()
all_done     = False

send_times  = {}
rtt_list    = []
total_sends = 0
chunks      = []

# ── 日志 ──
log_queue = queue.Queue()
log_fp = None

def logger_worker():
    while True:
        item = log_queue.get()
        if item is None:
            break
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        line = f"[{ts}] {item}"
        print(line)
        if log_fp:
            log_fp.write(line + "\n")
            log_fp.flush()

def log(msg):
    log_queue.put(msg)

def ms_now():
    return int(time.time() * 1000)

# ── 分块 ──
def generate_chunks(content):
    random.seed(42)
    offset = 0
    result = []
    for i in range(TOTAL_PACKETS):
        remaining = len(content) - offset
        if remaining <= 0:
            break
        size = remaining if i == TOTAL_PACKETS - 1 else random.randint(CHUNK_MIN, min(CHUNK_MAX, remaining))
        data = content[offset:offset+size].encode("ascii")
        result.append((offset, offset + size - 1, data))
        offset += size
    return result

# ── 接收线程 ──
class AckReceiver(threading.Thread):
    def __init__(self, sock):
        super().__init__(daemon=True)
        self.sock = sock

    def run(self):
        global base, window_bytes, all_done, rtt_list
        while not all_done:
            try:
                data, _ = self.sock.recvfrom(4096)
                ptype = struct.unpack("!H", data[:2])[0]
                t = ms_now()

                if ptype == TYPE_SYN_ACK:
                    syn_ack_rcvd.set()
                    log("[*] 收到 SYN-ACK 报文")

                elif ptype == TYPE_ACK:
                    ack_seq = struct.unpack("!H", data[2:4])[0]
                    with lock:
                        if ack_seq >= base:
                            for s in range(base, ack_seq + 1):
                                if s < len(chunks) and s in send_times:
                                    rtt = t - send_times[s]
                                    rtt_list.append(rtt)
                                    a, b, _ = chunks[s]
                                    log(f"第 {s+1} 个（第 {a}~{b} 字节）server 端已经收到，RTT 是 {rtt} ms")
                                    del send_times[s]
                                if s < len(chunks):
                                    window_bytes -= len(chunks[s][2]) + 6
                            base = ack_seq + 1
                            ack_updated.set()
                            if base >= len(chunks):
                                all_done = True
            except socket.timeout:
                continue
            except:
                if not all_done:
                    break

# ── 发包 ──
def send_one(sock, addr, seq, retrans=False):
    global total_sends, window_bytes
    a, b, data = chunks[seq]
    sock.sendto(struct.pack("!HHH", TYPE_DATA, seq, len(data)) + data, addr)
    send_times[seq] = ms_now()
    total_sends += 1
    window_bytes += len(data) + 6
    if retrans:
        log(f"重传第 {seq+1} 个（第 {a}~{b} 字节）数据包")
    else:
        log(f"第 {seq+1} 个（第 {a}~{b} 字节）client 端已经发送")

def fill_window(sock, addr):
    global next_seq
    while True:
        with lock:
            if next_seq >= len(chunks): break
            if window_bytes + len(chunks[next_seq][2]) + 6 > WINDOW_SIZE: break
            seq = next_seq
            next_seq += 1
        if seq >= len(chunks): break
        send_one(sock, addr, seq)

def retransmit(sock, addr):
    with lock:
        if base >= len(chunks): return
        send_one(sock, addr, base, retrans=True)

def calc_timeout():
    if len(rtt_list) >= 3:
        avg = sum(rtt_list[-5:]) / min(5, len(rtt_list))
        return max(0.1, avg * 3 / 1000.0)
    return INIT_TIMEOUT

# ── 汇总 ──
def print_summary():
    print()
    print("=" * 55)
    print("                    汇  总  统  计")
    print("=" * 55)
    rate = len(chunks) / total_sends * 100 if total_sends else 0
    print(f"  丢包率        : {rate:.2f}%  ({len(chunks)}/{total_sends})")
    
    if rtt_list:
        df = pd.DataFrame(rtt_list, columns=["RTT"])
        print(f"  最大 RTT      : {df['RTT'].max():.0f} ms")
        print(f"  最小 RTT      : {df['RTT'].min():.0f} ms")
        print(f"  平均 RTT      : {df['RTT'].mean():.1f} ms")
        print(f"  RTT 标准差    : {df['RTT'].std(ddof=1):.1f} ms")
    print("=" * 55)

# ════════════════════════════════════════════════
def main(server_ip, server_port):
    global log_fp, chunks, all_done
    addr = (server_ip, server_port)

    try:
        with open("test_data.txt", "r", encoding="ascii") as f:
            content = f.read()
    except FileNotFoundError:
        print("Error: test_data.txt not found"); return

    chunks = generate_chunks(content)
    print(f"[*] 分块信息: N={len(chunks)}")
    for i, (a, b, d) in enumerate(chunks):
        print(f"    块{i+1}: {len(d)} bytes")

    log_fp = open("run_log.txt", "w", encoding="utf-8")
    log_worker = threading.Thread(target=logger_worker, daemon=True)
    log_worker.start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 0))
    sock.settimeout(0.05)
    log(f"[*] CLIENT started (server={server_ip}:{server_port})")

    receiver = AckReceiver(sock)
    receiver.start()

    # ── 握手 ──
    log("[*] 发送 SYN 报文")
    sock.sendto(struct.pack("!HH", TYPE_SYN, STUDENT_ID_VAL), addr)
    if not syn_ack_rcvd.wait(timeout=3.0):
        log("[!] 握手超时")
        log_queue.put(None); sock.close(); log_fp.close(); return

    log("[*] 发送 ACK 报文，连接建立")
    sock.sendto(struct.pack("!H", TYPE_ACK_CONN), addr)

    # ── GBN 循环 ──
    fill_window(sock, addr)
    timeout = INIT_TIMEOUT

    while not all_done:
        ok = ack_updated.wait(timeout=timeout)
        ack_updated.clear()
        with lock:
            if base >= len(chunks):
                all_done = True; break
        if ok:
            timeout = calc_timeout()
            fill_window(sock, addr)
        else:
            retransmit(sock, addr)
            timeout = calc_timeout()

    log_queue.put(None)
    log_worker.join(timeout=2)
    time.sleep(0.3)
    print_summary()
    log_fp.close()
    sock.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python client.py <serverIP> <serverPort>"); sys.exit(1)
    main(sys.argv[1], int(sys.argv[2]))
