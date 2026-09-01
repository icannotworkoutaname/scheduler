#!/usr/bin/env python3
"""极简用户态 TCP 转发：listen 127.0.0.1:<listen-port> -> 127.0.0.1:<target-port>。

场景 5(网络分区，8/22)用它。这套系统里节点之间没有直连通信，一个节点唯一
依赖的外部协调渠道就是 Postgres——所以"网络分区"在这套架构下精确地等价于
"这个节点连不上 Postgres 了"。让 node A 只通过这个转发进程访问 Postgres，
node B 直连，就能精确隔离单个节点，不需要 root、不碰 iptables / /etc/hosts。

模拟分区：kill -STOP <pid>。进程冻结 -> 两个方向的字节都停止流动，但 socket
保持打开(内核不发 FIN/RST)。这正是真实网络分区的样子：包被静默丢弃，而不是
被拒绝。已建立的 JDBC 连接会发出查询然后一直等，直到 socketTimeout 触发——
正是 8/22 要验证 socketTimeout 参数的那条路径。
恢复分区：kill -CONT <pid>。
"""
import argparse
import socket
import sys
import threading


def pump(src: socket.socket, dst: socket.socket) -> None:
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        for s in (src, dst):
            try:
                s.close()
            except OSError:
                pass


def handle(client: socket.socket, target_port: int) -> None:
    try:
        upstream = socket.create_connection(("127.0.0.1", target_port))
    except OSError as e:
        print(f"upstream connect failed: {e}", flush=True)
        client.close()
        return
    threading.Thread(target=pump, args=(client, upstream), daemon=True).start()
    threading.Thread(target=pump, args=(upstream, client), daemon=True).start()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen-port", type=int, required=True)
    ap.add_argument("--target-port", type=int, default=5432)
    args = ap.parse_args()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", args.listen_port))
    srv.listen(128)
    print(
        f"db_proxy: 127.0.0.1:{args.listen_port} -> 127.0.0.1:{args.target_port}",
        flush=True,
    )
    while True:
        try:
            client, _ = srv.accept()
        except OSError as e:
            print(f"accept failed: {e}", flush=True)
            sys.exit(1)
        handle(client, args.target_port)


if __name__ == "__main__":
    main()
