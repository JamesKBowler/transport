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

import socket
import json
import struct


class Transmit:
    def __init__(
            self,
            identifier='tx'):
        self.id = identifier
        self.host = None
        self.port = None

    def set(self, host="127.0.0.1", port=61001):
        self.host = host
        self.port = port

    def _reset(self):
        self.host = None
        self.port = None

    def _transmit(self, message):
        encoded = json.dumps(message).encode()
        packed = struct.pack('>I', len(encoded)) + encoded
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client.connect((str(self.host), int(self.port)))
        client.sendall(packed)
        client.recv(4096)

    def send(self, payload):
        try:
            self._transmit(payload)
        except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError) as e:
            print(
                "Notification: Transmit() with id: {} has "
                "an error {}".format(self.id, e)
            )
        self._reset()


if __name__ == "__main__":
    streamer = Transmit()
    streamer.set(port=61001)
    streamer.send({"foo": "bar"})
