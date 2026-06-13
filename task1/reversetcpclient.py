import socket
import struct
import random
from datetime import datetime

TYPE_INIT = 1
TYPE_AGREE = 2
TYPE_REQUEST = 3
TYPE_ANSWER = 4

def init_logger(filename):
    return open(filename, 'w')

def log_message(log_file, direction, msg_type, length):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    log_file.write(f"[{timestamp}] {direction} {msg_type}, length={length}\n")
    log_file.flush()

def generate_chunks(file_size, lmin, lmax, seed):
    random.seed(seed)
    chunks = []
    remaining = file_size
    
    while remaining > 0:
        if remaining <= lmax:
            chunks.append(remaining)
            remaining = 0
        else:
            chunk_size = random.randint(lmin, lmax)
            if chunk_size > remaining:
                chunk_size = remaining
            chunks.append(chunk_size)
            remaining -= chunk_size
    
    return chunks

def main(server_ip, server_port, input_file, output_file, lmin, lmax, seed):
    with open(input_file, 'r', encoding='ascii') as f:
        content = f.read()
    file_size = len(content)
    
    chunks = generate_chunks(file_size, lmin, lmax, seed)
    n = len(chunks)
    
    print(f"[*] 分块信息: N={n}")
    for i, size in enumerate(chunks):
        print(f"    块{i+1}: {size} bytes")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, server_port))
    
    log_file = init_logger('run_log.txt')
    
    try:
        init_packet = struct.pack('!HI', TYPE_INIT, n)
        sock.sendall(init_packet)
        log_message(log_file, "CLIENT→SERVER", "Initialization", 6)
        print("[*] 发送Initialization报文")
        
        agree_type = struct.unpack('!H', sock.recv(2))[0]
        if agree_type != TYPE_AGREE:
            print("[!] 未收到agree报文")
            return
        log_message(log_file, "SERVER→CLIENT", "agree", 2)
        print("[*] 收到agree报文")
        
        reversed_content = []
        offset = 0
        
        for i in range(n):
            chunk_size = chunks[i]
            chunk = content[offset:offset + chunk_size]
            offset += chunk_size
            
            request_packet = struct.pack('!HI', TYPE_REQUEST, chunk_size) + chunk.encode('ascii')
            sock.sendall(request_packet)
            log_message(log_file, "CLIENT→SERVER", "reverseRequest", 6 + chunk_size)
            
            answer_type = struct.unpack('!H', sock.recv(2))[0]
            answer_length = struct.unpack('!I', sock.recv(4))[0]
            answer_data = sock.recv(answer_length)
            
            if len(answer_data) < answer_length:
                remaining = answer_length - len(answer_data)
                while remaining > 0:
                    data = sock.recv(remaining)
                    answer_data += data
                    remaining -= len(data)
            
            log_message(log_file, "SERVER→CLIENT", "reverseAnswer", 6 + answer_length)
            reversed_text = answer_data.decode('ascii')
            print(f"{i+1}:{reversed_text}")
            reversed_content.append(reversed_text)
        
        full_reversed = ''.join(reversed(reversed_content))
        with open(output_file, 'w', encoding='ascii') as f:
            f.write(full_reversed)
        print(f"[*] 反转结果已保存到 {output_file}")
        
    finally:
        sock.close()
        log_file.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 5:
        print("用法: python client.py <serverIP> <serverPort> <Lmin> <Lmax>")
        sys.exit(1)
    
    main(sys.argv[1], int(sys.argv[2]), "test.txt", "reversed.txt", int(sys.argv[3]), int(sys.argv[4]), 42)
