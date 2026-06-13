import socket
import struct
import random
import sys

TYPE_SYN, TYPE_SYN_ACK, TYPE_ACK_CONN, TYPE_DATA, TYPE_ACK = 1, 2, 3, 4, 5
DROP_RATE   = 0.2
EXPECTED_ID = 0x5A3C

def main(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", port))
    sock.settimeout(1.0)  # 每秒超时一次, 让 Ctrl+C 可中断

    print(f"[S] 服务端已启动, 端口={port}")
    print(f"[S] 按 Ctrl+C 关闭")

    while True:
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            continue
        except KeyboardInterrupt:
            break

        ptype = struct.unpack("!H", data[:2])[0]

        if ptype == TYPE_SYN:
            sid = struct.unpack("!H", data[2:4])[0]
            v = sid ^ EXPECTED_ID
            if 0 <= v <= 9999:
                print(f"[S] 验证通过(学号={v}), 来自 {addr}")
                sock.sendto(struct.pack("!H", TYPE_SYN_ACK), addr)
            else:
                print(f"[S] 验证失败, 拒绝 {addr}")

        elif ptype == TYPE_ACK_CONN:
            print(f"[S] 连接建立, 来自 {addr}")
            transfer(sock, addr)

def transfer(sock, addr):
    expected = 0
    count = 0
    while True:
        try:
            data, _ = sock.recvfrom(4096)
        except socket.timeout:
            continue
        except KeyboardInterrupt:
            return
        except:
            break

        ptype = struct.unpack("!H", data[:2])[0]
        if ptype != TYPE_DATA:
            continue

        seq     = struct.unpack("!H", data[2:4])[0]
        datalen = struct.unpack("!H", data[4:6])[0]

        if random.random() < DROP_RATE:
            print(f"[S] 丢弃 seq={seq}")
            continue

        if seq == expected:
            count += 1
            expected += 1
            ack = struct.pack("!HH", TYPE_ACK, expected - 1)
            sock.sendto(ack, addr)
            print(f"[S] seq={seq} ok (已收{count})")
        elif seq < expected:
            ack = struct.pack("!HH", TYPE_ACK, expected - 1)
            sock.sendto(ack, addr)
        else:
            pass  # 乱序丢弃

    print(f"[S] 传输结束, 共收到 {count} 包, 来自 {addr}")

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9999
    try:
        main(port)
    except KeyboardInterrupt:
        print("\n[S] 服务端已关闭")
