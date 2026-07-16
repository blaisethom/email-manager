#!/usr/bin/env python3
"""Minimal SOCKS5 relay server.

Listens on 172.21.1.38:8090. Each connection gets an outbound TCP connection
made from THIS process (in the dev container's network namespace, so the dev
container's Prokura transparent proxy intercepts it and injects credentials).
"""
import socket, threading, struct

BIND_ADDR = '172.21.1.38'
BIND_PORT = 8090

def forward(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except:
        pass
    finally:
        try: src.close()
        except: pass
        try: dst.close()
        except: pass

def handle(client):
    try:
        # SOCKS5 greeting
        header = client.recv(2)
        if len(header) < 2 or header[0] != 5:
            return
        n_methods = header[1]
        methods = client.recv(n_methods)
        client.sendall(b'\x05\x00')  # No auth required

        # Request
        req = client.recv(4)
        if len(req) < 4 or req[1] != 1:  # Only CONNECT
            client.sendall(b'\x05\x07\x00\x01' + b'\x00'*6)
            return

        atyp = req[3]
        if atyp == 1:  # IPv4
            addr_bytes = client.recv(4)
            host = '.'.join(str(b) for b in addr_bytes)
        elif atyp == 3:  # Domain
            length = client.recv(1)[0]
            host = client.recv(length).decode()
        elif atyp == 4:  # IPv6
            addr_bytes = client.recv(16)
            import ipaddress
            host = str(ipaddress.IPv6Address(addr_bytes))
        else:
            client.sendall(b'\x05\x08\x00\x01' + b'\x00'*6)
            return

        port_bytes = client.recv(2)
        port = struct.unpack('!H', port_bytes)[0]

        # Connect to target (goes through dev container's transparent proxy!)
        try:
            server = socket.create_connection((host, port), timeout=30)
        except Exception as e:
            client.sendall(b'\x05\x04\x00\x01' + b'\x00'*6)
            return

        client.sendall(b'\x05\x00\x00\x01' + socket.inet_aton('0.0.0.0') + b'\x00\x00')

        t1 = threading.Thread(target=forward, args=(client, server), daemon=True)
        t2 = threading.Thread(target=forward, args=(server, client), daemon=True)
        t1.start(); t2.start()
        t1.join(); t2.join()
    except:
        pass
    finally:
        try: client.close()
        except: pass

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((BIND_ADDR, BIND_PORT))
server.listen(100)
print(f'SOCKS5 relay: {BIND_ADDR}:{BIND_PORT}', flush=True)
while True:
    client, addr = server.accept()
    threading.Thread(target=handle, args=(client,), daemon=True).start()
