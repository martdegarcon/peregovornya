// Переговорный стол — сервер: раздаёт страницу, держит комнаты, синхронизирует игроков
// и передаёт сигналы WebRTC для видеосвязи. Всё хранится в памяти.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const CASES = require('./cases');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const AGR_FIELDS = ['who', 'when', 'terms', 'breach', 'extra'];
const PREP_FIELDS = ['must', 'give', 'batna'];
const MAX = 400;

function iceServers() {
  const list = [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun1.l.google.com:19302' }];
  if (process.env.TURN_URL) {
    list.push({ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USER, credential: process.env.TURN_PASS });
  }
  return list;
}

/* ---------- http ---------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/health') { res.end('ok'); return; }
  const p = url.pathname === '/' || /^\/r\/\d{4}$/.test(url.pathname) ? '/index.html' : url.pathname;
  const file = path.join(PUBLIC, path.normalize(p).replace(/^(\.\.[/\\])+/, ''));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); res.end(); return; }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

/* ---------- rooms ---------- */
const rooms = new Map();
const other = r => (r === 'A' ? 'B' : 'A');
const clip = (s, n = MAX) => String(s ?? '').slice(0, n);

function newCode() {
  let c;
  do { c = String(Math.floor(1000 + Math.random() * 9000)); } while (rooms.has(c));
  return c;
}
function freshRound(room) {
  Object.assign(room, {
    phase: 0, at: {}, result: null,
    ready: { A: false, B: false },
    prep: { A: {}, B: {} },
    notes: { A: '', B: '' },
    agr: Object.fromEntries(AGR_FIELDS.map(f => [f, ''])),
    agrBy: {},
    sign: { A: false, B: false },
    nodeal: { A: false, B: false },
    disputes: [],
    review: { A: { score: {}, hm: [], hl: [] }, B: { score: {}, hm: [], hl: [] } },
    rematch: { A: false, B: false },
  });
}
function newRoom(caseId) {
  const room = { code: newCode(), caseId, round: 1, touched: Date.now(), rtcEpoch: 0, players: { A: null, B: null } };
  freshRound(room);
  rooms.set(room.code, room);
  return room;
}
function caseOf(room) { return CASES.find(c => c.id === room.caseId) || CASES[0]; }
function setPhase(room, n, result) {
  if (n <= room.phase) return;
  room.phase = n;
  room.at[n] = Date.now();
  if (result) room.result = result;
}

// Каждый игрок получает только то, что ему положено видеть.
function viewFor(room, role) {
  const c = caseOf(room);
  const reveal = room.phase >= 3;
  const P = room.players;
  const mine = x => (reveal ? x : { [role]: x[role] });
  return {
    type: 'state', now: Date.now(),
    code: room.code, round: room.round, me: role,
    phase: room.phase, at: room.at, result: room.result,
    case: {
      id: c.id, title: c.title, dur: c.dur, context: c.context, constraints: c.constraints,
      noDeal: reveal ? c.noDeal : null,
      names: { A: c.roles.A.name, B: c.roles.B.name },
      roles: reveal ? c.roles : { [role]: c.roles[role] },
    },
    players: {
      A: P.A ? { name: P.A.name, online: !!P.A.ws } : null,
      B: P.B ? { name: P.B.name, online: !!P.B.ws } : null,
    },
    ready: room.ready,
    prep: mine(room.prep),
    notes: mine(room.notes),
    agr: room.agr, agrBy: room.agrBy,
    sign: room.sign, nodeal: room.nodeal,
    disputes: room.disputes,
    review: mine(room.review),
    rematch: room.rematch,
    rtcEpoch: room.rtcEpoch,
  };
}
function send(ws, msg) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); }
function broadcast(room) {
  room.touched = Date.now();
  for (const r of ['A', 'B']) { const p = room.players[r]; if (p && p.ws) send(p.ws, viewFor(room, r)); }
}

/* ---------- websocket ---------- */
const wss = new WebSocketServer({ server });
wss.on('connection', ws => {
  ws.ctx = { clientId: null, name: '', room: null, role: null };
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, {
    type: 'hello', ice: iceServers(),
    cases: CASES.map(c => ({ id: c.id, title: c.title, dur: c.dur, blurb: c.context[0], names: { A: c.roles.A.name, B: c.roles.B.name }, wants: { A: c.roles.A.want, B: c.roles.B.want } })),
  });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    try { handle(ws, m); } catch (e) { console.error(e); }
  });
  ws.on('close', () => detach(ws));
});

function attach(ws, room, role) {
  const p = room.players[role];
  if (p.ws && p.ws !== ws) { send(p.ws, { type: 'kicked', msg: 'Игра открыта в другой вкладке.' }); p.ws.ctx.room = null; p.ws.close(); }
  p.ws = ws;
  ws.ctx.room = room; ws.ctx.role = role;
  room.rtcEpoch++;
  broadcast(room);
}
function detach(ws) {
  const { room, role } = ws.ctx;
  if (!room || !room.players[role] || room.players[role].ws !== ws) return;
  room.players[role].ws = null;
  room.rtcEpoch++;
  broadcast(room);
}

function handle(ws, m) {
  const ctx = ws.ctx;
  if (m.type === 'id') { ctx.clientId = clip(m.clientId, 64); ctx.name = clip(m.name, 40).trim() || 'Игрок'; return; }
  if (!ctx.clientId) return;

  if (m.type === 'create') {
    const caseId = CASES.some(c => c.id === m.caseId) ? m.caseId : CASES[0].id;
    const room = newRoom(caseId);
    const role = m.role === 'A' || m.role === 'B' ? m.role : (Math.random() < 0.5 ? 'A' : 'B');
    room.players[role] = { clientId: ctx.clientId, name: ctx.name, ws: null };
    return attach(ws, room, role);
  }
  if (m.type === 'join') {
    const room = rooms.get(clip(m.code, 4));
    if (!room) return send(ws, { type: 'error', msg: 'Комнаты с таким кодом нет. Проверь цифры или попроси новую ссылку.' });
    for (const r of ['A', 'B']) {
      const p = room.players[r];
      if (p && p.clientId === ctx.clientId) { p.name = ctx.name || p.name; return attach(ws, room, r); }
    }
    const free = ['A', 'B'].find(r => !room.players[r]);
    if (!free) return send(ws, { type: 'error', msg: 'В этой комнате уже играют двое.' });
    room.players[free] = { clientId: ctx.clientId, name: ctx.name, ws: null };
    return attach(ws, room, free);
  }

  const room = ctx.room, me = ctx.role;
  if (!room || !me) return;
  const them = other(me);

  switch (m.type) {
    case 'leave': {
      room.players[me].ws = null; ctx.room = null; ctx.role = null;
      if (room.phase === 0) room.players[me] = null;
      room.rtcEpoch++;
      broadcast(room);
      if (!room.players.A && !room.players.B) rooms.delete(room.code);
      return;
    }
    case 'start':
      if (room.phase === 0 && room.players.A && room.players.B) setPhase(room, 1);
      break;
    case 'ready':
      if (room.phase !== 1) return;
      room.ready[me] = !!m.v;
      if (room.ready.A && room.ready.B) setPhase(room, 2);
      break;
    case 'skipPrep':
      if (room.phase === 1) setPhase(room, 2);
      break;
    case 'prep':
      if (room.phase > 2) return;
      for (const f of PREP_FIELDS) if (f in (m.data || {})) room.prep[me][f] = clip(m.data[f]);
      return; // видно только автору до разбора
    case 'notes':
      if (room.phase > 2) return;
      room.notes[me] = clip(m.value, 1500);
      return;
    case 'agr': {
      if (room.phase !== 2 || !AGR_FIELDS.includes(m.field)) return;
      const v = clip(m.value);
      if (room.agr[m.field] === v) return;
      room.agr[m.field] = v; room.agrBy[m.field] = me;
      room.sign.A = room.sign.B = false; // любая правка снимает подписи
      break;
    }
    case 'sign':
      if (room.phase !== 2) return;
      room.sign[me] = !!m.v; if (m.v) room.nodeal[me] = false;
      if (room.sign.A && room.sign.B) setPhase(room, 3, 'deal');
      break;
    case 'nodeal':
      if (room.phase !== 2) return;
      room.nodeal[me] = !!m.v; if (m.v) room.sign[me] = false;
      if (room.nodeal.A && room.nodeal.B) setPhase(room, 3, 'nodeal');
      break;
    case 'dispute': {
      if (room.phase !== 2 || room.disputes.length >= 20) return;
      const text = clip(m.text, 200).trim();
      if (!text) return;
      room.disputes.push({ id: room.disputes.length + 1, by: me, text, status: 'pending', t: Date.now() });
      break;
    }
    case 'disputeResolve': {
      const d = room.disputes.find(x => x.id === m.id);
      if (!d || d.by === me || d.status !== 'pending') return;
      d.status = m.v ? 'accepted' : 'rejected';
      break;
    }
    case 'review': {
      if (room.phase !== 3) return;
      const r = room.review[me];
      if (m.score && typeof m.score === 'object') for (const [k, v] of Object.entries(m.score)) if (/^[a-z]{2,8}$/.test(k)) r.score[k] = Math.max(0, Math.min(5, +v || 0));
      if (Array.isArray(m.hm)) r.hm = m.hm.slice(0, 10).map(x => +x || 0);
      if (Array.isArray(m.hl)) r.hl = m.hl.slice(0, 10).map(x => (x ? 1 : 0));
      break;
    }
    case 'rematch': {
      if (room.phase !== 3) return;
      room.rematch[me] = !!m.v;
      if (room.rematch.A && room.rematch.B) {
        // реванш: меняемся ролями, остаёмся в той же комнате
        const { A, B } = room.players;
        room.players = { A: B, B: A };
        for (const r of ['A', 'B']) { const p = room.players[r]; if (p && p.ws) p.ws.ctx.role = r; }
        room.round++;
        freshRound(room);
        setPhase(room, 1);
        room.rtcEpoch++;
      }
      break;
    }
    case 'rtc': {
      if (m.epoch !== room.rtcEpoch) return;
      const p = room.players[them];
      if (p && p.ws) send(p.ws, { type: 'rtc', epoch: m.epoch, data: m.data });
      return;
    }
    default: return;
  }
  broadcast(room);
}

setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); });
}, 25000);
setInterval(() => {
  const cutoff = Date.now() - 3 * 3600e3;
  for (const [code, r] of rooms) {
    const empty = !['A', 'B'].some(x => r.players[x] && r.players[x].ws);
    if (empty && r.touched < cutoff) rooms.delete(code);
  }
}, 10 * 60e3);

server.listen(PORT, () => console.log('Переговорный стол: http://localhost:' + PORT));
