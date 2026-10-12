const express = require("express");
const http = require("http");
const path = require("path");
const { randomUUID } = require("crypto");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
  server,
  path: "/ws",
  maxPayload: 1024 * 1024
});

app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.json({ online: true });
});

const rooms = new Map();
const MAX_HISTORY = 50;
const MAX_CLIENTS = 100;

function getRoom(name) {
  if (!rooms.has(name)) {
    rooms.set(name, []);
  }
  return rooms.get(name);
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(room, data) {
  for (const client of wss.clients) {
    if (
      client.readyState === WebSocket.OPEN &&
      client.room === room
    ) {
      send(client, data);
    }
  }
}

wss.on("connection", (ws) => {
  if (wss.clients.size > MAX_CLIENTS) {
    ws.close(1013, "Servidor ocupado");
    return;
  }

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (data.type === "join") {
      const room = String(data.room || "");

      if (!/^[a-zA-Z0-9_-]{8,40}$/.test(room)) {
        send(ws, {
          type: "error",
          text: "Código de sala inválido."
        });
        return;
      }

      ws.room = room;

      send(ws, {
        type: "history",
        messages: getRoom(room)
      });

      broadcast(room, { type: "presence" });
      return;
    }

    if (!ws.room) return;

    if (data.type === "chat") {
      const text = String(data.text || "").trim();

      if (!text || text.length > 2000) return;

      const message = {
        id: randomUUID(),
        type: "chat",
        text,
        sender: String(data.sender || "Visitante").slice(0, 30),
        time: new Date().toISOString()
      };

      const history = getRoom(ws.room);
      history.push(message);

      if (history.length > MAX_HISTORY) {
        history.shift();
      }

      broadcast(ws.room, message);
    }

    if (data.type === "image") {
      const image = String(data.data || "");

      if (!/^data:image\/(png|jpeg|webp|gif);base64,/.test(image)) {
        return;
      }

      if (image.length > 600000) return;

      const message = {
        id: randomUUID(),
        type: "image",
        data: image,
        sender: String(data.sender || "Visitante").slice(0, 30),
        time: new Date().toISOString()
      };

      const history = getRoom(ws.room);
      history.push(message);

      if (history.length > MAX_HISTORY) {
        history.shift();
      }

      broadcast(ws.room, message);
    }
  });

  ws.on("close", () => {
    if (ws.room) {
      broadcast(ws.room, { type: "presence" });
    }
  });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor iniciado na porta " + PORT);
});
