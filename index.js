/**
 * ============================================================
 * ANON MESSENGER — ANONYMOUS TAG-BASED ROUTING SERVER
 * ============================================================
 */

const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
});

const tagQueues = {};
const activeRooms = {};
const socketMeta = {};

// ─── Helpers ─────────────────────────────────────────────────────────────────

function broadcastTagCount(tag) {
  if (!tag) return;
  let count = tagQueues[tag] ? 1 : 0;
  for (const room of Object.values(activeRooms)) {
    if (room.tag === tag) count += room.users.size;
  }
  io.to(`tag:${tag}`).emit('tag-user-count', { tag, count });
}

function removeFromQueue(socketId) {
  const meta = socketMeta[socketId];
  if (!meta) return false;

  const tag = meta.tag;
  meta.tag = null;

  if (tag && tagQueues[tag] === socketId) {
    delete tagQueues[tag];
    broadcastTagCount(tag);
    return true;
  }
  return false;
}

function destroyRoom(socketId) {
  const meta = socketMeta[socketId];
  if (!meta || !meta.roomId) return null;

  const { roomId } = meta;
  meta.roomId = null;

  const room = activeRooms[roomId];
  if (!room) return null;

  room.users.forEach((uid) => {
    if (uid !== socketId) {
      io.to(uid).emit('peer-disconnected');
      if (socketMeta[uid]) {
        socketMeta[uid].roomId = null;
      }
    }
    const s = io.sockets.sockets.get(uid);
    if (s) {
      s.leave(roomId);
    }
  });

  const { tag } = room;
  delete activeRooms[roomId];
  broadcastTagCount(tag);
  return tag;
}

// ─── Connection Handler ───────────────────────────────────────────────────────

io.on('connection', (socket) => {
  console.log(`🟢 Connected: ${socket.id}`);
  socketMeta[socket.id] = { tag: null, roomId: null };

  // ── 1. JOIN TAG QUEUE ───────────────────────────────────────────────────────
  socket.on('join-tag-queue', (data) => {
    const rawTag = data && typeof data === 'object' && typeof data.tag === 'string' ? data.tag : null;
    if (!rawTag) {
      socket.emit('error-msg', 'Please enter a valid tag.');
      return;
    }

    const trimmed = rawTag.trim().toLowerCase();
    const normalizedTag = trimmed.startsWith('#') ? trimmed : `#${trimmed}`;

    if (!normalizedTag || normalizedTag === '#') {
      socket.emit('error-msg', 'Please enter a valid tag.');
      return;
    }

    if (socketMeta[socket.id]?.roomId) {
      destroyRoom(socket.id);
    }
    if (socketMeta[socket.id]?.tag) {
      const oldTag = socketMeta[socket.id].tag;
      removeFromQueue(socket.id);
      socket.leave(`tag:${oldTag}`);
    }

    socket.join(`tag:${normalizedTag}`);
    socketMeta[socket.id].tag = normalizedTag;

    let peerSocketId = tagQueues[normalizedTag];

    if (peerSocketId === socket.id) {
      socket.emit('error-msg', 'Already in queue for this tag.');
      return;
    }

    if (peerSocketId) {
      const peerSocket = io.sockets.sockets.get(peerSocketId);
      if (!peerSocket || !peerSocket.connected) {
        delete tagQueues[normalizedTag];
        if (socketMeta[peerSocketId]) {
          delete socketMeta[peerSocketId];
        }
        peerSocketId = null;
      }
    }

    if (!peerSocketId) {
      tagQueues[normalizedTag] = socket.id;
      broadcastTagCount(normalizedTag);

      let currentCount = 1;
      for (const room of Object.values(activeRooms)) {
        if (room.tag === normalizedTag) currentCount += room.users.size;
      }

      socket.emit('waiting', {
        tag: normalizedTag,
        onlineCount: currentCount,
      });

      console.log(`⏳ [${socket.id}] waiting in ${normalizedTag}`);
      return;
    }

    delete tagQueues[normalizedTag];

    const roomId = crypto.randomUUID();
    const encKey = crypto.randomBytes(32).toString('hex');

    activeRooms[roomId] = {
      tag: normalizedTag,
      users: new Set([peerSocketId, socket.id]),
      encKey,
    };

    if (socketMeta[peerSocketId]) socketMeta[peerSocketId].roomId = roomId;
    socketMeta[socket.id].roomId = roomId;

    const peerSocket = io.sockets.sockets.get(peerSocketId);
    if (peerSocket) {
      peerSocket.join(roomId);
      peerSocket.leave(`tag:${normalizedTag}`);
    }
    socket.join(roomId);
    socket.leave(`tag:${normalizedTag}`);

    const matchPayload = { roomId, encKey, tag: normalizedTag };
    io.to(peerSocketId).emit('matched', matchPayload);
    socket.emit('matched', matchPayload);

    broadcastTagCount(normalizedTag);
    console.log(`🔗 Matched [${peerSocketId}] ↔ [${socket.id}] in room [${roomId}]`);
  });

  // ── 2. LEAVE QUEUE ──────────────────────────────────────────────────────────
  socket.on('leave-queue', () => {
    const meta = socketMeta[socket.id];
    const tag = meta?.tag;
    const wasRemoved = removeFromQueue(socket.id);
    if (tag) {
      socket.leave(`tag:${tag}`);
    }
    if (wasRemoved) {
      console.log(`🚪 [${socket.id}] left queue`);
    }
  });

  // ── 3. BLIND MESSAGE RELAY ─────────────────────────────────────────────────
  socket.on('send-message', (data) => {
    if (!data || typeof data !== 'object') return;
    const { roomId, encryptedPayload } = data;
    if (!roomId || typeof roomId !== 'string' || !encryptedPayload || typeof encryptedPayload !== 'string') return;
    if (encryptedPayload.length > 65536) return;

    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;

    socket.to(roomId).emit('receive-message', encryptedPayload);
  });

  // ── 4. TYPING INDICATORS ───────────────────────────────────────────────────
  socket.on('typing-start', (data) => {
    if (!data || typeof data !== 'object') return;
    const { roomId } = data;
    if (!roomId || typeof roomId !== 'string') return;

    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;
    socket.to(roomId).emit('stranger-typing');
  });

  socket.on('typing-stop', (data) => {
    if (!data || typeof data !== 'object') return;
    const { roomId } = data;
    if (!roomId || typeof roomId !== 'string') return;

    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;
    socket.to(roomId).emit('stranger-stopped-typing');
  });

  // ── 5. WEBRTC VOICE CALL SIGNALING (CLEANED) ───────────────────────────────

  socket.on('webrtc-offer', (data) => {
    if (!data || typeof data !== 'object') return;
    const { roomId, offer } = data;
    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;

    console.log(`📞 [${socket.id}] sending OFFER to room [${roomId}]`);
    socket.to(roomId).emit('webrtc-offer', offer);
  });

  socket.on('webrtc-answer', (data) => {
    if (!data || typeof data !== 'object') return;
    const { roomId, answer } = data;
    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;

    console.log(`✅ [${socket.id}] sending ANSWER to room [${roomId}]`);
    socket.to(roomId).emit('webrtc-answer', answer);
  });

  socket.on('webrtc-ice-candidate', (data) => {
    if (!data || typeof data !== 'object') return;
    const { roomId, candidate } = data;
    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;

    socket.to(roomId).emit('webrtc-ice-candidate', candidate);
  });

  socket.on('end-call', (data) => {
    if (!data || typeof data !== 'object') return;
    const { roomId } = data;
    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;

    console.log(`🛑 [${socket.id}] ENDED call in room [${roomId}]`);
    socket.to(roomId).emit('call-ended');
  });

  // ── 6. LEAVE ROOM / DISCONNECT ─────────────────────────────────────────────
  socket.on('leave-room', (data) => {
    const roomId = data && typeof data === 'object' ? data.roomId : null;
    const meta = socketMeta[socket.id];
    if (!meta) return;

    if (meta.roomId && (!roomId || meta.roomId === roomId)) {
      destroyRoom(socket.id);
    }
  });

  socket.on('disconnect', (reason) => {
    console.log(`🔴 Disconnected: ${socket.id} (${reason})`);
    const meta = socketMeta[socket.id];
    if (!meta) return;

    if (meta.roomId) {
      destroyRoom(socket.id);
    }
    if (meta.tag) {
      removeFromQueue(socket.id);
    }
    delete socketMeta[socket.id];
  });
});

// ─── Health Check Endpoint ────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    connected: io.engine.clientsCount,
    queuedTags: Object.keys(tagQueues).length,
    activeRooms: Object.keys(activeRooms).length,
  });
});

// ─── Start Server ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🔒 Anon Messenger Tag-Router running on port ${PORT}`);
});