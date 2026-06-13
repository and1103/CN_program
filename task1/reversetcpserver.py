#!/usr/bin/env python3
import socket
import struct
import threading
from datetime import datetime

TYPE_INIT = 1
TYPE_AGREE = 2
TYPE_REQUEST = 3
TYPE_ANSWER = 4

def reverse_text(text):
    return text[::-1]

def handle_client(client_socket, client_address):
    print(f"[+] 新连接: {client_address}")
    try:
        while True:
            type_data = client_socket.recv(2)
            if not type_data:
                break
            
            msg_type = struct.unpack('!H', type_data)[0]
            
            if msg_type == TYPE_INIT:
                n_data = client_socket.recv(4)
                n = struct.unpack('!I', n_data)[0]
                print(f"[ ] 收到初始化报文，块数N={n}")
                
                agree_packet = struct.pack('!H', TYPE_AGREE)
                client_socket.sendall(agree_packet)
                print(f"[ ] 发送agree报文")
            
            elif msg_type == TYPE_REQUEST:
                length_data = client_socket.recv(4)
                length = struct.unpack('!I', length_data)[0]
                data = client_socket.recv(length)
                
                if len(data) < length:
                    remaining = length - len(data)
                    while remaining > 0:
                        chunk = client_socket.recv(remaining)
                        if not chunk:
                            break
                        data += chunk
                        remaining -= len(chunk)
                
                text = data.decode('ascii')
                reversed_text = reverse_text(text)
                reversed_data = reversed_text.encode('ascii')
                
                answer_packet = struct.pack('!HI', TYPE_ANSWER, len(reversed_data)) + reversed_data
                client_socket.sendall(answer_packet)
                print(f"[ ] 处理请求，长度={length}")
            
            else:
                print(f"[!] 未知报文类型: {msg_type}")
                break
                
    except Exception as e:
        print(f"[!] 错误: {e}")
    finally:
        client_socket.close()
        print(f"[-] 连接关闭: {client_address}")

def main(port=8888):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)
    
    server_socket.settimeout(1.0)  # 1秒超时，让Ctrl+C可中断
    
    print(f"[*] Server listening on :{port}")
    
    try:
        while True:
            try:
                client_sock, client_addr = server_socket.accept()
                client_thread = threading.Thread(target=handle_client, args=(client_sock, client_addr))
                client_thread.daemon = True
                client_thread.start()
            except socket.timeout:
                continue  # 超时后回到while循环，检查KeyboardInterrupt
            
    except KeyboardInterrupt:
        print("\n[*] Server shutting down")
    finally:
        server_socket.close()

if __name__ == "__main__":
    import sys
    port = 8888
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    main(port)
