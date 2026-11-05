// Drag Fighter: servidor de salas en línea (para Render)
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Servidor de Drag Fighter funcionando');
});

const rooms = new Map();
const NAME = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const roomOf = n => { if (!rooms.has(n)) rooms.set(n, new Map()); return rooms.get(n); };
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
function broadcast(n, o, except) {
  const r = rooms.get(n); if (!r) return;
  const msg = JSON.stringify(o);
  for (const [peer, m] of r) if (peer !== except && m.ws.readyState === 1) m.ws.send(msg);
}
function join(ws, n) {
  if (!NAME.test(n)) return;
  const r = roomOf(n);
  if (r.has(ws.peer) || r.size >= (n === 'lobby' ? 500 : 4)) return;
  const snap = [...r].map(([peer, m]) => ({ peer, p: m.p }));
  r.set(ws.peer, { ws, p: {} }); ws.rooms.add(n);
  send(ws, { t: 'snap', room: n, peers: snap });
}
function leave(ws, n) {
  const r = rooms.get(n); if (!r || !r.has(ws.peer)) return;
  r.delete(ws.peer); ws.rooms.delete(n);
  broadcast(n, { t: 'left', room: n, peer: ws.peer });
  if (r.size === 0 && n !== 'lobby') rooms.delete(n);
}

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8 * 1024 });
wss.on('connection', ws => {
  ws.peer = crypto.randomBytes(8).toString('hex');
  ws.rooms = new Set(); ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  send(ws, { t: 'hello', peer: ws.peer });
  join(ws, 'lobby');
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.room !== 'string') return;
    if (m.t === 'join') join(ws, m.room);
    else if (m.t === 'leave' && m.room !== 'lobby') leave(ws, m.room);
    else if (m.t === 'pres') {
      const me = rooms.get(m.room) && rooms.get(m.room).get(ws.peer); if (!me) return;
      me.p = (m.p && typeof m.p === 'object') ? m.p : {};
      broadcast(m.room, { t: 'pres', room: m.room, peer: ws.peer, p: me.p }, ws.peer);
    }
  });
  ws.on('close', () => { for (const n of [...ws.rooms]) leave(ws, n); });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 20000);

server.listen(PORT, () => console.log('Drag Fighter listo en el puerto ' + PORT));
