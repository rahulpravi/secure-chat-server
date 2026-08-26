/**
 * ============================================================
 * SECURE MESSENGER — ANONYMOUS TAG-BASED ROUTING SERVER
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
  if (!meta || !meta.tag) return false;

  const { tag } = meta;
  if (tagQueues[tag] === socketId) {
    delete tagQueues[tag];
    broadcastTagCount(tag);
    return true;
  }
  return false;
}

/**
 * Cleanly destroy an active room and notify the other user.
 * Returns the tag that was in use.
 */
function destroyRoom(socketId) {
  const meta = socketMeta[socketId];
  if (!meta || !meta.roomId) return null;

  const { roomId } = meta;
  const room = activeRooms[roomId];
  if (!room) return null;

  // Notify every OTHER user in the room
  room.users.forEach((uid) => {
    if (uid !== socketId) {
      io.to(uid).emit('peer-disconnected');
      // Clean up their meta so they don't try to clean room again
      if (socketMeta[uid]) socketMeta[uid].roomId = null;
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
  //
  //  Client emits: join-tag-queue  { tag: '#adventure' }
  //  Server either:
  //    a) Queues the user and emits 'waiting'
  //    b) Matches with waiting user, emits 'matched' to both
  //
  socket.on('join-tag-queue', ({ tag }) => {
    // Normalize: force lowercase, ensure '#' prefix
    const normalizedTag = tag.trim().toLowerCase().startsWith('#')
      ? tag.trim().toLowerCase()
      : `#${tag.trim().toLowerCase()}`;

    if (!normalizedTag || normalizedTag === '#') {
      socket.emit('error-msg', 'Please enter a valid tag.');
      return;
    }

    // Subscribe this socket to the tag's broadcast channel
    socket.join(`tag:${normalizedTag}`);
    socketMeta[socket.id].tag = normalizedTag;

    // ── Case A: No one waiting — add to queue ──────────────────────────────
    if (!tagQueues[normalizedTag]) {
      tagQueues[normalizedTag] = socket.id;
      broadcastTagCount(normalizedTag);

      socket.emit('waiting', {
        tag: normalizedTag,
        onlineCount: 1,
      });

      console.log(`⏳ [${socket.id}] waiting in ${normalizedTag}`);
      return;
    }

    // ── Case B: Someone IS waiting — match them ────────────────────────────
    const peerSocketId = tagQueues[normalizedTag];

    // Edge case: the waiting socket somehow matches itself (shouldn't happen,
    // but guard anyway)
    if (peerSocketId === socket.id) {
      socket.emit('error-msg', 'Already in queue for this tag.');
      return;
    }

    // Remove from queue before doing anything else
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
    socketMeta[peerSocketId].roomId = roomId;
    socketMeta[socket.id].roomId    = roomId;

    // Join both sockets into the Socket.io room for message relay
    const peerSocket = io.sockets.sockets.get(peerSocketId);
    if (peerSocket) peerSocket.join(roomId);
    socket.join(roomId);

    const matchPayload = { roomId, encKey, tag: normalizedTag };
    io.to(peerSocketId).emit('matched', matchPayload);
    socket.emit('matched', matchPayload);

    broadcastTagCount(normalizedTag);
    console.log(`🔗 Matched [${peerSocketId}] ↔ [${socket.id}] in room [${roomId}] tag ${normalizedTag}`);
  });

  // ── 2. LEAVE QUEUE (user cancels waiting) ──────────────────────────────────
  socket.on('leave-queue', () => {
    const wasRemoved = removeFromQueue(socket.id);
    if (wasRemoved) {
      const tag = socketMeta[socket.id]?.tag;
      if (tag) socket.leave(`tag:${tag}`);
      console.log(`🚪 [${socket.id}] left queue`);
    }
  });

  // ── 3. BLIND MESSAGE RELAY ─────────────────────────────────────────────────
  //  The server NEVER decrypts. It blindly forwards the opaque ciphertext.
  socket.on('send-message', ({ roomId, encryptedPayload }) => {
    // Validate that this socket is actually in the claimed room
    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;

    socket.to(roomId).emit('receive-message', encryptedPayload);
  });

  // ── 4. TYPING INDICATORS ───────────────────────────────────────────────────
  socket.on('typing-start', ({ roomId }) => {
    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;
    socket.to(roomId).emit('stranger-typing');
  });

  socket.on('typing-stop', ({ roomId }) => {
    const meta = socketMeta[socket.id];
    if (!meta || meta.roomId !== roomId) return;
    socket.to(roomId).emit('stranger-stopped-typing');
  });

  // ── 5. DISCONNECT — Comprehensive Cleanup ──────────────────────────────────
  //  Handles both voluntary leave and abrupt connection drops.
  socket.on('disconnect', (reason) => {
    console.log(`🔴 Disconnected: ${socket.id} (${reason})`);

    const meta = socketMeta[socket.id];
    if (!meta) return;

    if (meta.roomId) {
      // Was in an active chat — notify peer and destroy room
      destroyRoom(socket.id);
    } else if (meta.tag) {
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
  console.log(`🔒 Secure Messenger Tag-Router running on port ${PORT}`);
});