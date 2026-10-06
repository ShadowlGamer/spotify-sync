const net = require('net');
const crypto = require('crypto');

// Suga will tell us which port to use via an environment variable
const PORT = process.env.PORT || 80;

const rooms = new Map(); // room -> Set of sockets
const sendMap = new Map(); // socket -> send function

function makeSend(socket) {
    return function (payload, opcode) {
        opcode = opcode || 0x01;
        if (socket.destroyed) return;
        const data = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), 'utf-8');
        const len = data.length;
        let header;
        if (len < 126) {
            header = Buffer.from([0x80 | opcode, len]);
        } else if (len < 65536) {
            header = Buffer.alloc(4);
            header[0] = 0x80 | opcode;
            header[1] = 126;
            header.writeUInt16BE(len, 2);
        } else {
            header = Buffer.alloc(10);
            header[0] = 0x80 | opcode;
            header[1] = 127;
            header.writeBigUInt64BE(BigInt(len), 2);
        }
        try { socket.write(Buffer.concat([header, data])); } catch (e) {}
    };
}

const server = net.createServer(function (socket) {
    let buffer = Buffer.alloc(0);
    let room = null;
    let closed = false;
    let send = null;
    let isWebSocket = false;

    function cleanup() {
        if (closed) return;
        closed = true;
        if (room && rooms.has(room)) {
            rooms.get(room).delete(socket);
            if (rooms.get(room).size === 0) rooms.delete(room);
            console.log('[WS] Left room ' + room);
        }
        if (send) sendMap.delete(socket);
        try { socket.end(); } catch (e) {}
    }

    socket.on('data', function (chunk) {
        buffer = Buffer.concat([buffer, chunk]);

        // Check for WebSocket handshake
        if (!isWebSocket) {
            const headerEnd = buffer.indexOf('\r\n\r\n');
            if (headerEnd === -1) return; // Wait for more data

            const headerStr = buffer.slice(0, headerEnd).toString('utf-8');
            const lines = headerStr.split('\r\n');
            const headers = {};
            for (let i = 1; i < lines.length; i++) {
                const idx = lines[i].indexOf(':');
                if (idx > 0) headers[lines[i].slice(0, idx).trim().toLowerCase()] = lines[i].slice(idx + 1).trim();
            }

            if ((headers['upgrade'] || '').toLowerCase() === 'websocket' && headers['sec-websocket-key']) {
                isWebSocket = true;
                const key = headers['sec-websocket-key'];
                const accept = crypto.createHash('sha1')
                    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');

                socket.write(
                    'HTTP/1.1 101 Switching Protocols\r\n' +
                    'Upgrade: websocket\r\n' +
                    'Connection: Upgrade\r\n' +
                    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
                );

                send = makeSend(socket);
                sendMap.set(socket, send);
                buffer = buffer.slice(headerEnd + 4); // Remove handshake from buffer
            } else {
                // Plain HTTP health check
                socket.write('HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 27\r\nConnection: close\r\n\r\nSpotify Sync server is live.');
                socket.end();
                return;
            }
        }

        // Process WebSocket frames
        while (buffer.length >= 2) {
            const opcode = buffer[0] & 0x0f;
            const masked = (buffer[1] & 0x80) !== 0;
            let payloadLen = buffer[1] & 0x7f;
            let offset = 2;

            if (payloadLen === 126) {
                if (buffer.length < 4) return;
                payloadLen = buffer.readUInt16BE(2); offset = 4;
            } else if (payloadLen === 127) {
                if (buffer.length < 10) return;
                payloadLen = Number(buffer.readBigUInt64BE(2)); offset = 10;
            }

            let maskKey = null;
            if (masked) {
                if (buffer.length < offset + 4) return;
                maskKey = buffer.slice(offset, offset + 4); offset += 4;
            }

            if (buffer.length < offset + payloadLen) return;

            let payload = buffer.slice(offset, offset + payloadLen);
            buffer = buffer.slice(offset + payloadLen);

            if (masked) {
                const u = Buffer.alloc(payload.length);
                for (let i = 0; i < payload.length; i++) u[i] = payload[i] ^ maskKey[i % 4];
                payload = u;
            }

            if (opcode === 0x08) { cleanup(); return; }
            if (opcode === 0x09) { if (send) send(payload, 0x0a); continue; }
            if (opcode === 0x0a) continue;
            if (opcode !== 0x01) continue;

            let data;
            try { data = JSON.parse(payload.toString('utf-8')); } catch (e) { continue; }

            if (data.type === 'join' && data.room) {
                if (room && rooms.has(room)) rooms.get(room).delete(socket);
                room = String(data.room);
                if (!rooms.has(room)) rooms.set(room, new Set());
                rooms.get(room).add(socket);
                if (send) send(JSON.stringify({ type: 'joined', room: room }));
                console.log('[WS] Joined room ' + room + ' (total: ' + rooms.get(room).size + ')');
            } else if (room) {
                const msgStr = payload.toString('utf-8');
                const peers = rooms.get(room);
                if (peers) {
                    for (const peer of peers) {
                        if (peer !== socket && !peer.destroyed) {
                            const ps = sendMap.get(peer);
                            if (ps) ps(msgStr);
                        }
                    }
                }
            }
        }
    });

    socket.on('error', cleanup);
    socket.on('close', cleanup);
});

server.listen(PORT, '0.0.0.0', function () {
    console.log('[WS] WebSocket server listening on port ' + PORT);
    console.log('Ready. Connect your app to wss://<your-suga-url>');
});
