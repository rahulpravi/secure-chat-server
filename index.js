const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// Allow connections from any origin (your mobile app)
const io = new Server(server, {
  cors: { origin: "*" }
});

// In-memory volatile storage for room states only. 
// Example structure: { 'room123': { limit: 5, currentUsers: 2 } }
// NO MESSAGE DATA IS EVER STORED HERE.
const rooms = {};

io.on('connection', (socket) => {
  console.log(`🟢 New device connected: ${socket.id}`);

  // 1. Create a Secure Room
  socket.on('create-room', ({ roomId, limit }) => {
    if (rooms[roomId]) {
      socket.emit('error-msg', 'Room ID already exists. Choose another.');
      return;
    }
    
    // Store only room limits in RAM
    rooms[roomId] = { limit: limit, currentUsers: 1 };
    socket.join(roomId);
    
    console.log(`🛡️ Room [${roomId}] created (Limit: ${limit})`);
    socket.emit('room-created', roomId);
  });

  // 2. Join an Existing Room
  socket.on('join-room', (roomId) => {
    const room = rooms[roomId];
    
    if (!room) {
      socket.emit('error-msg', 'Room does not exist.');
      return;
    }
    if (room.currentUsers >= room.limit) {
      socket.emit('error-msg', 'Room is currently full.');
      return;
    }
    
    room.currentUsers += 1;
    socket.join(roomId);
    
    console.log(`👤 User joined [${roomId}]. Total users: ${room.currentUsers}`);
    socket.emit('room-joined', roomId);
    
    // Alert others in the room
    socket.to(roomId).emit('peer-joined', 'A new peer has entered the room.');
  });

  // 3. Blind Message Relay (The Core Privacy Feature)
  socket.on('send-message', ({ roomId, encryptedPayload }) => {
    // The server has NO decryption key. It blindly forwards the ciphertext.
    socket.to(roomId).emit('receive-message', encryptedPayload);
  });

  // 4. Memory Cleanup on Disconnect
  socket.on('disconnect', () => {
    console.log(`🔴 Device disconnected: ${socket.id}`);
    // In a full production version, we will add logic here to decrement 
    // the currentUsers count when a socket drops out.
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🔒 Zero-Knowledge Signaling Server running on port ${PORT}`);
});