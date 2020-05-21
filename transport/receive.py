# The MIT License (MIT)
#
# Copyright (c) 2020 James K Bowler, Data Centauri Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from threading import Thread
from random import randint

import json
import time
import socket
import struct


class Receive:
    def __init__(
            self,
            queue,
            port=None,
            host=None,
            identifier="rx"):
        self.id = identifier
        self.queue = queue
        self.stopped = True
        self.stream = None
        self.thread = None
        self.host = host
        self.port = port

    def __del__(self):
        self.stop()

    def start(self):
        def _run():
            self.stream_to_queue()

        self.stopped = False
        self._pre()
        self._bind()
        self._post()
        self.thread = Thread(target=_run)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.stopped = True
        if self.stream is None:
            return
        try:
            self.stream.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        time.sleep(2)
        self.stream.close()

    def _pre(self):
        # Pre connection settings
        self.stream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.stream.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def _bind(self):
        # Setup host and port information
        if self.host is None:
            self.host = "127.0.0.1"
        if self.port is not None:
            self.stream.bind((self.host, self.port))
        else:
            while self.port is None:
                try:
                    self.port = randint(55000, 60000)
                    self.stream.bind((self.host, self.port))
                except OSError:
                    self.port = None

    def _post(self):
        # Post connection settings
        self.stream.listen(5)
        print("-- Started Receive, listening @%s:%s" % (self.host, self.port))

    def stream_to_queue(self):
        def handle(_id, _sock, _que):
            def recv_msg(rx):
                raw_msglen = recvall(rx, 4)
                if not raw_msglen:
                    return None
                msglen = struct.unpack('>I', raw_msglen)[0]
                return recvall(rx, msglen)

            def recvall(rx, n):
                data = b''
                while len(data) < n:
                    packet = rx.recv(n - len(data))
                    if not packet:
                        return None
                    data += packet
                return data

            message = recv_msg(_sock)
            _sock.send(b'ACK!')
            _sock.close()
            if message:
                dmsg = json.loads(message.decode())
                if not isinstance(dmsg, dict):
                    raise AttributeError("Loaded json must be a list")
                _que.put(dmsg)
            else:
                raise EOFError(
                    "Unexpected end of Receive %s" % _id
                )
        while not self.stopped:
            try:
                _sock = self.stream.accept()  # Blocks
                # (<socket.socket fd=4,
                # family=AddressFamily.AF_INET,
                # type=SocketKind.SOCK_STREAM,
                # proto=0, laddr=('127.0.0.1', 61001),
                # raddr=('127.0.0.1', 48812)>,
                # ('127.0.0.1', 48812))
            except OSError:
                break
            handle(self.id, _sock[0], self.queue)
        print("-- Shutting down Receive")


if __name__ == "__main__":
    import queue as qu

    q = qu.Queue()
    rx = Receive(q, port=61001)
    rx.start()
    try:
        while True:
            try:
                msg = q.get(False)
            except qu.Empty:
                time.sleep(1)
            else:
                if msg is not None:
                    print(msg)

    except KeyboardInterrupt:
        rx.stop()
