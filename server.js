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
// room.people — до двух участников: { id, name, ws, role: 'A'|'B'|null, rulesOk }
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
  const room = { code: newCode(), caseId, round: 1, touched: Date.now(), rtcEpoch: 0, people: [], swap: null, history: [] };
  freshRound(room);
  rooms.set(room.code, room);
  return room;
}
const caseOf = room => CASES.find(c => c.id === room.caseId) || CASES[0];
const byRole = (room, r) => room.people.find(p => p.role === r) || null;
const partnerOf = (room, p) => room.people.find(x => x !== p) || null;
function setPhase(room, n, result) {
  if (n <= room.phase) return;
  room.phase = n;
  room.at[n] = Date.now();
  if (result) room.result = result;
}
// Очки за партию: считаются из того, что оба отметили на разборе.
const KEY4 = ['who', 'when', 'terms', 'breach'];
const VAGUE = /(постара|подума|посмотрим|попробу|по возможности|как.?нибудь|когда.?нибудь|в ближайшее время|обсудим|потом решим|как получится|если получится)/i;
function scoreRound(room) {
  const c = caseOf(room), out = { A: { items: [], total: 0 }, B: { items: [], total: 0 } };
  const add = (r, label, pts) => { out[r].items.push([label, pts]); out[r].total += pts; };
  if (room.result === 'deal') {
    const good = KEY4.filter(k => (room.agr[k] || '').trim().length > 3 && !VAGUE.test(room.agr[k])).length;
    for (const r of ['A', 'B']) good >= 3 ? add(r, 'Договорились конкретно', 3) : add(r, 'Договорились, но размыто', 1);
  } else for (const r of ['A', 'B']) add(r, 'Не договорились', 0);
  for (const r of ['A', 'B']) {
    const o = other(r), mine = room.review[r], theirs = room.review[o];
    c.roles[o].hidden.forEach((_, i) => {
      if (!mine.hl[i]) return;
      if (theirs.hm[i] === 2) add(r, `Вытащил(а) секрет собеседника №${i + 1}`, 2);
      else if (theirs.hm[i] === 1) add(r, `Узнал(а) секрет №${i + 1} (раскрыли сами)`, 1);
    });
    c.roles[r].hidden.forEach((_, i) => { if (mine.hm[i] === 0 && !theirs.hl[i]) add(r, `Удержал(а) свой секрет №${i + 1}`, 1); });
  }
  for (const d of room.disputes) if (d.status === 'accepted') add(other(d.by), 'Пришлось снять придуманную деталь', -1);
  return out;
}
function board(room) {
  const cur = room.phase >= 3 ? scoreRound(room) : null;
  return {
    games: room.history.length + (cur ? 1 : 0),
    people: room.people.map(p => ({ name: p.name, id: p.id, total: room.history.reduce((a, h) => a + (h.pts[p.id] || 0), 0) + (cur && p.role ? cur[p.role].total : 0) })),
  };
}
const pub = p => (p ? { name: p.name, online: !!p.ws, role: p.role, rulesOk: p.rulesOk } : null);

// Каждый получает только то, что ему положено видеть.
function viewFor(room, person) {
  const c = caseOf(room);
  const role = person.role;
  const reveal = room.phase >= 3;
  const own = x => (reveal ? x : role ? { [role]: x[role] } : {});
  const partner = partnerOf(room, person);
  return {
    type: 'state', now: Date.now(),
    code: room.code, round: room.round, me: role,
    polite: room.people.indexOf(person) === 1,
    phase: room.phase, at: room.at, result: room.result,
    case: {
      id: c.id, title: c.title, dur: c.dur, context: c.context, constraints: c.constraints,
      noDeal: reveal ? c.noDeal : null,
      names: { A: c.roles.A.name, B: c.roles.B.name },
      // карточки: своя — только с начала подготовки, чужая — только на разборе
      roles: reveal ? c.roles : role && room.phase >= 1 ? { [role]: c.roles[role] } : {},
    },
    self: pub(person), partner: pub(partner),
    players: { A: pub(byRole(room, 'A')), B: pub(byRole(room, 'B')) },
    swap: room.swap ? (room.swap === person.id ? 'me' : 'them') : null,
    ready: room.ready,
    prep: own(room.prep), notes: own(room.notes),
    agr: room.agr, agrBy: room.agrBy,
    sign: room.sign, nodeal: room.nodeal,
    disputes: room.disputes,
    review: own(room.review),
    rematch: room.rematch,
    score: room.phase >= 3 ? scoreRound(room) : null,
    board: (() => { const b = board(room); return { games: b.games, people: b.people.map(x => ({ name: x.name, total: x.total, me: x.id === person.id })) }; })(),
    rtcEpoch: room.rtcEpoch,
  };
}
function send(ws, msg) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); }
function broadcast(room) {
  room.touched = Date.now();
  for (const p of room.people) if (p.ws) send(p.ws, viewFor(room, p));
}

/* ---------- websocket ---------- */
const wss = new WebSocketServer({ server });
wss.on('connection', ws => {
  ws.ctx = { clientId: null, name: '', room: null, person: null };
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, {
    type: 'hello', ice: iceServers(),
    cases: CASES.map(c => ({ id: c.id, title: c.title, dur: c.dur, blurb: c.context[0], names: { A: c.roles.A.name, B: c.roles.B.name } })),
  });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    try { handle(ws, m); } catch (e) { console.error(e); }
  });
  ws.on('close', () => detach(ws));
});

function attach(ws, room, person) {
  if (person.ws && person.ws !== ws) { send(person.ws, { type: 'kicked', msg: 'Игра открыта в другой вкладке.' }); person.ws.ctx.room = null; person.ws.close(); }
  person.ws = ws;
  ws.ctx.room = room; ws.ctx.person = person;
  room.rtcEpoch++;
  broadcast(room);
}
function detach(ws) {
  const { room, person } = ws.ctx;
  if (!room || !person || person.ws !== ws) return;
  person.ws = null;
  room.rtcEpoch++;
  broadcast(room);
}

function handle(ws, m) {
  const ctx = ws.ctx;
  if (m.type === 'id') { ctx.clientId = clip(m.clientId, 64); ctx.name = clip(m.name, 40).trim() || 'Игрок'; return; }

  // что за комната по ссылке — до входа, без спойлеров
  if (m.type === 'peek') {
    const room = rooms.get(clip(m.code, 4));
    if (!room) return send(ws, { type: 'peek', ok: false });
    const host = room.people[0];
    const mine = ctx.clientId && room.people.some(p => p.id === ctx.clientId);
    return send(ws, { type: 'peek', ok: true, code: room.code, title: caseOf(room).title, dur: caseOf(room).dur, host: host ? host.name : null, member: !!mine, full: room.people.length >= 2 && !mine });
  }
  if (!ctx.clientId) return;

  if (m.type === 'create') {
    const caseId = CASES.some(c => c.id === m.caseId) ? m.caseId : CASES[0].id;
    const room = newRoom(caseId);
    const person = { id: ctx.clientId, name: ctx.name, ws: null, role: null, rulesOk: false };
    room.people.push(person);
    return attach(ws, room, person);
  }
  if (m.type === 'join') {
    const room = rooms.get(clip(m.code, 4));
    if (!room) return send(ws, { type: 'error', msg: 'Комнаты с таким кодом нет. Проверь цифры или попроси новую ссылку.' });
    let person = room.people.find(p => p.id === ctx.clientId);
    if (person) { person.name = ctx.name || person.name; return attach(ws, room, person); }
    if (room.people.length >= 2) return send(ws, { type: 'error', msg: 'В этой комнате уже играют двое.' });
    person = { id: ctx.clientId, name: ctx.name, ws: null, role: null, rulesOk: false };
    room.people.push(person);
    return attach(ws, room, person);
  }

  const room = ctx.room, person = ctx.person;
  if (!room || !person) return;
  const partner = partnerOf(room, person);
  const me = person.role;

  switch (m.type) {
    case 'leave': {
      person.ws = null; ctx.room = null; ctx.person = null;
      if (room.phase === 0) { room.people = room.people.filter(p => p !== person); room.swap = null; }
      room.rtcEpoch++;
      if (!room.people.some(p => p.ws)) { if (room.phase === 0 && !room.people.length) rooms.delete(room.code); }
      broadcast(room);
      return;
    }

    /* --- лобби --- */
    case 'pickRole': {
      if (room.phase !== 0 || (m.role !== 'A' && m.role !== 'B')) return;
      const holder = byRole(room, m.role);
      if (holder === person) return;
      if (!holder) { person.role = m.role; room.swap = null; break; }
      // роль занята собеседником — предлагаем обмен
      if (me) room.swap = person.id;
      break;
    }
    case 'swapAnswer': {
      if (room.phase !== 0 || !room.swap || room.swap === person.id || !partner) return;
      if (m.v) { const t = person.role; person.role = partner.role; partner.role = t; }
      room.swap = null;
      break;
    }
    case 'randomRoles': {
      if (room.phase !== 0) return;
      const r = Math.random() < 0.5 ? 'A' : 'B';
      person.role = r; if (partner) partner.role = other(r);
      room.swap = null;
      break;
    }
    case 'rulesOk':
      if (room.phase !== 0) return;
      person.rulesOk = !!m.v;
      break;
    case 'start':
      if (room.phase === 0 && room.people.length === 2 && room.people.every(p => p.role && p.rulesOk) && room.people[0].role !== room.people[1].role) setPhase(room, 1);
      break;

    /* --- подготовка --- */
    case 'ready':
      if (room.phase !== 1) return;
      room.ready[me] = !!m.v;
      if (room.ready.A && room.ready.B) setPhase(room, 2);
      break;
    case 'skipPrep':
      if (room.phase === 1) setPhase(room, 2);
      break;
    case 'prep':
      if (room.phase < 1 || room.phase > 2) return;
      for (const f of PREP_FIELDS) if (f in (m.data || {})) room.prep[me][f] = clip(m.data[f]);
      return;
    case 'notes':
      if (room.phase < 1 || room.phase > 2) return;
      room.notes[me] = clip(m.value, 1500);
      return;

    /* --- переговоры --- */
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

    /* --- разбор --- */
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
        const sc = scoreRound(room);
        room.history.push({ round: room.round, caseId: room.caseId, result: room.result, pts: Object.fromEntries(room.people.map(p => [p.id, sc[p.role].total])) });
        for (const p of room.people) p.role = other(p.role); // меняемся ролями
        room.round++;
        freshRound(room);
        setPhase(room, 1);
        room.rtcEpoch++;
      }
      break;
    }

    case 'rtc': {
      if (m.epoch !== room.rtcEpoch || !partner) return;
      if (partner.ws) send(partner.ws, { type: 'rtc', epoch: m.epoch, data: m.data });
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
  for (const [code, r] of rooms) if (!r.people.some(p => p.ws) && r.touched < cutoff) rooms.delete(code);
}, 10 * 60e3);

server.listen(PORT, () => console.log('Переговорный стол: http://localhost:' + PORT));
