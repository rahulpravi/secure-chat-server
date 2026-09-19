/**
 * ============================================================
 * ANON MESSENGER — ANONYMOUS TAG-BASED ROUTING SERVER
 * ============================================================
 * Architecture:
 *   - tagQueues     : Holds one waiting socket per tag
 *   - activeRooms   : Stores paired room state (in-memory only)
 *   - socketMeta    : Tracks each socket's current tag/room for cleanup
 *
 * Privacy guarantee: NO message content is ever stored on this server.
 * The server only relays opaque encrypted blobs it cannot decode.
 * ============================================================
 */

const express = require('express');
const http    = require('http');
const crypto  = require('crypto');
const { Server } = require('socket.io');

const app    = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  // Prefer WebSocket, fall back to polling for constrained environments
  transports: ['websocket', 'polling'],
});

// ─── In-Memory Volatile State ────────────────────────────────────────────────
//
// tagQueues: { '#adventure': 'socketId_of_waiting_user' }
//   Only ONE user can wait per tag. When a second arrives they are matched.
//
// activeRooms: {
//   'uuid-room-id': {
//     tag    : '#adventure',
//     users  : Set<socketId>,   // always exactly 2 users
//     encKey : 'hex-string',    // shared AES key, generated fresh per match
//   }
// }
//
// socketMeta: { socketId: { tag: '#adventure', roomId: 'uuid' | null } }
//   Used to clean up correctly on unexpected disconnect.
//
const tagQueues  = {};   // tag  → socketId (one waiter per tag)
const activeRooms = {};  // uuid → { tag, users: Set, encKey }
const socketMeta  = {};  // socketId → { tag, roomId }

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Broadcast the live online count for a given tag to ALL connected sockets
 * that are either waiting in or actively chatting under that tag.
 */
function broadcastTagCount(tag) {
  if (!tag) return;
  // Count: waiting user (if any) + active room users under this tag
  let count = tagQueues[tag] ? 1 : 0;

  for (const room of Object.values(activeRooms)) {
    if (room.tag === tag) count += room.users.size;
  }

  // Broadcast to the tag-specific namespace room all sockets subscribe to
  io.to(`tag:${tag}`).emit('tag-user-count', { tag, count });
}

/**
 * Cleanly remove a socket from the waiting queue for its tag.
 * Returns true if the socket was found and removed.
 */
function removeFromQueue(socketId) {
  const meta = socketMeta[socketId];
  if (!meta) return false;

  const tag = meta.tag;
  meta.tag = null; // Always clear tag reference from socket metadata

  if (tag && tagQueues[tag] === socketId) {
    delete tagQueues[tag];
    broadcastTagCount(tag);
    return true;
  }
  return false;
}

/**
 * Cleanly destroy an active room, notify the peer, and release all memory.
 * Returns the tag that was in use.
 */
function destroyRoom(socketId) {
  const meta = socketMeta[socketId];
  if (!meta || !meta.roomId) return null;

  const { roomId } = meta;
  meta.roomId = null; // Clear calling user's roomId immediately

  const room = activeRooms[roomId];
  if (!room) return null;

  // Notify every OTHER user in the room and clear their roomId
  room.users.forEach((uid) => {
    if (uid !== socketId) {
      io.to(uid).emit('peer-disconnected');
      if (socketMeta[uid]) {
        socketMeta[uid].roomId = null;
      }
    }
    // Have socket leave the room if connected
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

  // Initialise metadata entry for this socket
  socketMeta[socket.id] = { tag: null, roomId: null };

  // ── 1. JOIN TAG QUEUE ───────────────────────────────────────────────────────
  socket.on('join-tag-queue', (data) => {
    const rawTag = data && typeof data === 'object' && typeof data.tag === 'string' ? data.tag : null;
    if (!rawTag) {
      socket.emit('error-msg', 'Please enter a valid tag.');
      return;
    }

    // Normalize: force lowercase, ensure '#' prefix
    const trimmed = rawTag.trim().toLowerCase();
    const normalizedTag = trimmed.startsWith('#') ? trimmed : `#${trimmed}`;

    if (!normalizedTag || normalizedTag === '#') {
      socket.emit('error-msg', 'Please enter a valid tag.');
      return;
    }

    // Clean up any existing state (active room or previous queue)
    if (socketMeta[socket.id]?.roomId) {
      destroyRoom(socket.id);
    }
    if (socketMeta[socket.id]?.tag) {
      const oldTag = socketMeta[socket.id].tag;
      removeFromQueue(socket.id);
      socket.leave(`tag:${oldTag}`);
    }

    // Subscribe this socket to the tag's broadcast channel
    socket.join(`tag:${normalizedTag}`);
    socketMeta[socket.id].tag = normalizedTag;

    // ── Check waiting queue ──
    let peerSocketId = tagQueues[normalizedTag];

    // Edge case: self in queue
    if (peerSocketId === socket.id) {
      socket.emit('error-msg', 'Already in queue for this tag.');
      return;
    }

    // If there is a peer in queue, verify they are still connected and valid
    if (peerSocketId) {
      const peerSocket = io.sockets.sockets.get(peerSocketId);
      if (!peerSocket || !peerSocket.connected) {
        // Peer is disconnected zombie — clean up stale queue entry
        delete tagQueues[normalizedTag];
        if (socketMeta[peerSocketId]) {
          delete socketMeta[peerSocketId];
        }
        peerSocketId = null;
      }
    }

    // ── Case A: No one waiting — add to queue ──
    if (!peerSocketId) {
      tagQueues[normalizedTag] = socket.id;
      broadcastTagCount(normalizedTag);

      // Compute actual current online count under this tag
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

    // ── Case B: Valid peer waiting — match them ──
    delete tagQueues[normalizedTag];

    // Generate a fresh, unguessable room ID and 256-bit AES key
    const roomId = crypto.randomUUID();
    const encKey = crypto.randomBytes(32).toString('hex'); // 64 hex chars = 256 bits

    // Register the active room
    activeRooms[roomId] = {
      tag  : normalizedTag,
      users: new Set([peerSocketId, socket.id]),
      encKey,
    };

    // Update metadata for both sockets
    if (socketMeta[peerSocketId]) socketMeta[peerSocketId].roomId = roomId;
    socketMeta[socket.id].roomId = roomId;

    // Join both sockets into the Socket.io room for message relay
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
    console.log(`🔗 Matched [${peerSocketId}] ↔ [${socket.id}] in room [${roomId}] tag ${normalizedTag}`);
  });

  // ── 2. LEAVE QUEUE (user cancels waiting) ──────────────────────────────────
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

    // Limit payload size (max 64KB)
    if (encryptedPayload.length > 65536) return;

    // Validate that this socket is actually in the claimed room
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

  // ── 5. LEAVE ROOM / DISCONNECT — Comprehensive Cleanup ─────────────────────
  socket.on('leave-room', (data) => {
    const roomId = data && typeof data === 'object' ? data.roomId : null;
    const meta = socketMeta[socket.id];
    if (!meta) return;

    if (meta.roomId && (!roomId || meta.roomId === roomId)) {
      destroyRoom(socket.id);
    }
  });

  // Handles connection drops
  socket.on('disconnect', (reason) => {
    console.log(`🔴 Disconnected: ${socket.id} (${reason})`);

    const meta = socketMeta[socket.id];
    if (!meta) return;

    if (meta.roomId) {
      // Was in an active chat — notify peer and destroy room
      destroyRoom(socket.id);
    }
    if (meta.tag) {
      // Was still waiting in queue
      removeFromQueue(socket.id);
    }

    // Final cleanup of this socket's metadata
    delete socketMeta[socket.id];
  });
});

// ─── Health Check Endpoint ────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    status     : 'ok',
    connected  : io.engine.clientsCount,
    queuedTags : Object.keys(tagQueues).length,
    activeRooms: Object.keys(activeRooms).length,
  });
});

// ─── Start Server ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🔒 Anon Messenger Tag-Router running on port ${PORT}`);
});