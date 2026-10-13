// raft-kv dashboard: cluster view, read/write box, event feed.
// Node state comes straight from each node's HTTP gateway (/status). Start, stop
// and kill buttons need the control server (go run ./control) and are hidden
// when the page is served by something else, e.g. nginx in docker compose.
"use strict";

const $ = (id) => document.getElementById(id);
const fmt = (n) => Number(n).toLocaleString();

const params = new URLSearchParams(location.search);
const NODE_URLS = (params.get("nodes") || "")
  .split(",").map((s) => s.trim()).filter(Boolean);
if (NODE_URLS.length === 0) {
  for (let i = 1; i <= 5; i++) NODE_URLS.push("http://localhost:800" + i);
}
const POLL_MS = 500;
if (params.get("theme")) document.documentElement.dataset.theme = params.get("theme");

// One entry per node. `up` is whether the last poll succeeded; `data` is the
// last good /status payload (kept while down so the row can still show an id).
const nodes = NODE_URLS.map((url, i) => ({
  url, idx: i, id: "n" + (i + 1), up: false, seen: false, data: null, ui: null,
}));
let control = null; // last /cluster response, or null when no control server
let renderQueued = false;

// ---------------------------------------------------------------- polling

async function fetchJSON(url, ms) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, { signal: ctrl.signal });
    if (!r.ok) throw new Error("HTTP " + r.status);
    return await r.json();
  } finally {
    clearTimeout(timer);
  }
}

function pollNode(n) {
  fetchJSON(n.url + "/status", 1000)
    .then((d) => observe(n, true, d))
    .catch(() => observe(n, false, null))
    .finally(() => setTimeout(() => pollNode(n), POLL_MS));
}

async function pollControl() {
  try {
    const r = await fetch("/cluster", { cache: "no-store" });
    const isJSON = (r.headers.get("content-type") || "").includes("json");
    control = r.ok && isJSON ? await r.json() : null;
  } catch {
    control = null;
  }
  scheduleRender();
  setTimeout(pollControl, 1500);
}

// ---------------------------------------------------------------- events

const eventList = $("events");

function addEvent(text, kind) {
  $("events-empty").hidden = true;
  const li = document.createElement("li");
  if (kind) li.className = kind;
  const t = document.createElement("time");
  t.textContent = new Date().toTimeString().slice(0, 8);
  const what = document.createElement("span");
  what.className = "what";
  what.textContent = text;
  li.append(t, what);
  eventList.prepend(li);
  while (eventList.children.length > 60) eventList.lastChild.remove();
}

// observe records one poll result and turns state changes into feed entries.
function observe(n, up, d) {
  const prev = { up: n.up, seen: n.seen, d: n.data };
  n.up = up;
  if (up) {
    n.data = d;
    n.id = d.nodeId || n.id;
    n.seen = true;
  }
  if (prev.seen) {
    if (prev.up && !up) addEvent(n.id + " stopped responding", "bad");
    if (!prev.up && up) {
      addEvent(n.id + " is back and catching up (term " + d.term + ")", "good");
    }
    if (up && prev.up) diffNode(n, prev.d, d);
  } else if (up && !prev.seen && nodes.every((x) => !x.seen || x === n)) {
    addEvent("Connected to the cluster");
  }
  scheduleRender();
}

function diffNode(n, a, b) {
  if (!a) return;
  if (a.role !== "leader" && b.role === "leader") {
    addEvent(n.id + " became leader for term " + b.term, "good");
  } else if (a.role !== "candidate" && b.role === "candidate") {
    addEvent(n.id + " started an election for term " + b.term);
  }
  if (b.snapshotIndex > a.snapshotIndex) {
    addEvent(n.id + " compacted its log up to entry " + fmt(b.snapshotIndex));
  }
}

// ---------------------------------------------------------------- derived state

function clusterView() {
  const total = nodes.length;
  const quorum = Math.floor(total / 2) + 1;
  const up = nodes.filter((n) => n.up);
  const leaders = up.filter((n) => n.data.role === "leader");
  // If a stale leader lingers across a partition, the higher term is current.
  leaders.sort((a, b) => b.data.term - a.data.term);
  return { total, quorum, up: up.length, leader: leaders[0] || null };
}

function leaderURL() {
  const v = clusterView();
  return v.leader ? v.leader.url : null;
}

function nodeByGRPC(addr) {
  // Leader hints are gRPC addresses; the HTTP port is the gRPC port minus 1000.
  const m = /:(\d+)$/.exec(addr || "");
  if (!m) return null;
  const port = parseInt(m[1], 10) - 1000;
  return nodes.find((n) => n.url.endsWith(":" + port)) || null;
}

// ---------------------------------------------------------------- rendering

function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => { renderQueued = false; render(); });
}

function render() {
  renderBanner();
  renderRows();
  renderClusterActions();
  renderReadSelect();
  $("tests").hidden = !control;
}

function renderBanner() {
  const v = clusterView();
  const b = $("banner");
  let cls = "", title = "", detail = "";
  const everSeen = nodes.some((n) => n.seen);

  if (control && control.busy) {
    title = "A " + control.kind + " test is running";
    detail = "The demo cluster is paused while it runs. Start it again when the test finishes.";
  } else if (v.up === 0) {
    cls = !control && everSeen ? "bad" : "";
    if (control) {
      title = "No cluster running";
      detail = "Press Start cluster to launch 5 nodes.";
    } else if (everSeen) {
      title = "No nodes are responding";
      detail = "Every node is down or unreachable from this page.";
    } else {
      title = "Connecting to the cluster";
      detail = "Looking for nodes on " + NODE_URLS.join(", ");
    }
  } else if (v.up < v.quorum) {
    cls = "bad";
    title = "No majority: writes are blocked";
    detail = v.up + " of " + v.total + " nodes are up and " + v.quorum +
      " are needed. Bring a node back and the cluster recovers by itself.";
  } else if (!v.leader) {
    cls = "warn";
    title = "Electing a leader";
    detail = v.up + " of " + v.total + " nodes are up. This usually takes about a second.";
  } else if (v.up < v.total) {
    cls = "warn";
    const spare = v.up - v.quorum;
    title = "Degraded: " + v.up + " of " + v.total + " nodes up";
    detail = v.leader.id + " is leader (term " + v.leader.data.term + "). Reads and writes still work. " +
      (spare > 0 ? "The cluster can lose " + spare + " more " + (spare === 1 ? "node." : "nodes.")
                 : "Losing one more node would block writes.");
  } else {
    cls = "ok";
    title = "Healthy: all " + v.total + " nodes up";
    detail = v.leader.id + " is leader (term " + v.leader.data.term + "). The cluster can lose " +
      (v.total - v.quorum) + " nodes and keep working.";
  }
  b.className = "banner" + (cls ? " " + cls : "");
  $("banner-title").textContent = title;
  $("banner-detail").textContent = detail;
}

function buildRow(n) {
  const row = document.createElement("div");
  row.className = "row";
  row.innerHTML =
    '<div class="who"><span class="id"></span><span class="term"></span></div>' +
    '<div class="role"></div>' +
    '<div class="logcol"><div class="bar"><i class="snap"></i><i class="done"></i><i class="tail"></i></div>' +
    '<div class="barnote"></div></div>' +
    '<div class="nums"><span class="keys"></span><span class="commit"></span></div>' +
    '<div class="act"><button class="btn small" type="button" hidden></button></div>';
  const q = (s) => row.querySelector(s);
  n.ui = {
    row, id: q(".id"), term: q(".term"), role: q(".role"),
    snap: q(".bar .snap"), done: q(".bar .done"), tail: q(".bar .tail"),
    note: q(".barnote"), keys: q(".keys"), commit: q(".commit"), btn: q(".act button"),
  };
  n.ui.btn.addEventListener("click", () => nodeAction(n));
  $("rows").appendChild(row);
}

function setRole(el, cls, text, tag) {
  el.textContent = "";
  const s = document.createElement("span");
  s.className = (tag ? "tag " : "") + cls;
  s.textContent = text;
  el.appendChild(s);
}

function renderRows() {
  if (!nodes[0].ui) nodes.forEach(buildRow);
  const scale = Math.max(20, ...nodes.map((n) => (n.up ? lastIndex(n.data) : 0)));
  const managed = control ? new Map(control.nodes.map((c) => [c.id, c.running])) : null;

  for (const n of nodes) {
    const u = n.ui, d = n.data;
    u.row.classList.toggle("is-down", !n.up);
    u.id.textContent = n.id;
    u.term.textContent = n.up ? "term " + d.term : "";

    if (!n.up) {
      const stopped = managed && managed.has(n.id) && !managed.get(n.id);
      if (stopped) setRole(u.role, "stopped", "Stopped");
      else setRole(u.role, n.seen ? "down" : "stopped", n.seen ? "Not responding" : "Waiting");
      for (const s of [u.snap, u.done, u.tail]) { s.style.left = "0"; s.style.width = "0"; }
      u.note.textContent = n.seen ? "last seen at entry " + fmt(lastIndex(d)) : "";
      u.keys.textContent = ""; u.commit.textContent = "";
    } else {
      if (d.role === "leader") setRole(u.role, "leader", "Leader", true);
      else if (d.role === "candidate") setRole(u.role, "candidate", "Candidate");
      else setRole(u.role, "follower", "Follower");

      const snap = d.snapshotIndex, commit = Math.max(d.commitIndex, snap), last = Math.max(lastIndex(d), commit);
      const pct = (x) => (100 * x / scale).toFixed(2) + "%";
      u.snap.style.left = "0"; u.snap.style.width = pct(snap);
      u.done.style.left = pct(snap); u.done.style.width = pct(commit - snap);
      u.tail.style.left = pct(commit); u.tail.style.width = pct(last - commit);
      const open = last - commit;
      u.note.textContent = fmt(last) + (last === 1 ? " entry" : " entries") + (open > 0 ? ", " + fmt(open) + " not yet committed" : "");
      u.keys.textContent = fmt(d.keys) + " keys";
      u.commit.textContent = "commit " + fmt(d.commitIndex);
    }

    // Per-node control: Kill when it is running, Start when it is down.
    const known = managed && managed.has(n.id);
    const busy = control && control.busy;
    u.btn.hidden = !known;
    if (known) {
      const running = managed.get(n.id);
      u.btn.textContent = running ? "Kill" : "Start";
      u.btn.classList.toggle("kill", running);
      u.btn.dataset.action = running ? "kill" : "start";
      u.btn.disabled = !!busy;
      u.btn.setAttribute("aria-label", (running ? "Kill " : "Start ") + n.id);
    }
  }
  const hint = $("nodes-hint");
  hint.textContent = control
    ? "Kill stops a node the hard way, like a crash. Start brings it back from its saved data."
    : "";
}

function lastIndex(d) {
  return d ? (d.snapshotIndex || 0) + (d.logLen || 0) : 0;
}

function renderClusterActions() {
  const box = $("cluster-actions");
  box.hidden = !control;
  if (!control) return;
  const any = control.nodes.some((c) => c.running);
  const all = control.nodes.every((c) => c.running);
  const busy = control.busy;
  $("btn-start-all").hidden = all;
  $("btn-start-all").textContent = any ? "Start missing nodes" : "Start cluster";
  $("btn-start-all").disabled = busy;
  $("fresh-wrap").hidden = any;
  $("btn-stop-all").hidden = !any;
  $("btn-stop-all").disabled = busy;
}

let readOptionsKey = "";
function renderReadSelect() {
  const key = nodes.map((n) => n.id).join(",");
  if (key === readOptionsKey) return;
  readOptionsKey = key;
  const sel = $("read-from");
  const cur = sel.value;
  sel.textContent = "";
  sel.add(new Option("Leader", "auto"));
  nodes.forEach((n) => sel.add(new Option(n.id, String(n.idx))));
  sel.value = cur || "auto";
}

// ---------------------------------------------------------------- control actions

async function post(path) {
  try {
    const r = await fetch(path, { method: "POST" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || "HTTP " + r.status);
    return null;
  } catch (e) {
    return e.message;
  }
}

async function nodeAction(n) {
  const action = n.ui.btn.dataset.action;
  n.ui.btn.disabled = true;
  const err = await post("/node/" + action + "?id=" + encodeURIComponent(n.id));
  if (err) addEvent("Could not " + action + " " + n.id + ": " + err, "bad");
  else if (action === "kill") addEvent("You killed " + n.id);
  else addEvent("You started " + n.id);
  pollControl();
}

$("btn-start-all").addEventListener("click", async () => {
  const fresh = $("fresh").checked ? "1" : "0";
  $("btn-start-all").disabled = true;
  const err = await post("/cluster/start?fresh=" + fresh);
  if (err) addEvent("Could not start the cluster: " + err, "bad");
  else addEvent("Starting 5 nodes");
});

$("btn-stop-all").addEventListener("click", async () => {
  $("btn-stop-all").disabled = true;
  const err = await post("/cluster/stop");
  if (err) addEvent("Could not stop the cluster: " + err, "bad");
  else addEvent("You stopped every node");
});

// ---------------------------------------------------------------- key-value box

function say(kind, text) {
  const el = $("kv-result");
  el.className = "result" + (kind ? " " + kind : "");
  el.textContent = text;
}

// writeKey sends one write to the leader, following a redirect if the node it
// reached is not the leader any more. Resolves to null or an error string.
async function writeKey(path, depth = 0) {
  let url = leaderURL();
  if (!url) return "There is no leader right now, so the write cannot be accepted.";
  for (let hop = depth; hop < 4; hop++) {
    let j;
    try {
      j = await fetchJSON(url + path, 3000);
    } catch {
      return "The leader did not answer in time.";
    }
    if (j.ok) return null;
    const next = nodeByGRPC(j.leaderHint);
    if (next && next.url !== url) { url = next.url; continue; }
    return j.error || "The write was not accepted.";
  }
  return "The leader changed too many times; try again.";
}

async function doSet() {
  const key = $("kv-key").value.trim(), value = $("kv-value").value;
  if (!key) return say("err", "Enter a key first.");
  const t0 = performance.now();
  const err = await writeKey("/set?key=" + encodeURIComponent(key) + "&value=" + encodeURIComponent(value));
  if (err) return say("err", err);
  say("ok", "Saved " + key + " = " + value + " (" + Math.round(performance.now() - t0) + " ms). A majority of nodes has it.");
}

async function doDelete() {
  const key = $("kv-key").value.trim();
  if (!key) return say("err", "Enter a key first.");
  const err = await writeKey("/del?key=" + encodeURIComponent(key));
  say(err ? "err" : "ok", err || "Deleted " + key + ".");
}

async function doGet() {
  const key = $("kv-key").value.trim();
  if (!key) return say("err", "Enter a key first.");
  const pick = $("read-from").value;
  let target = pick === "auto" ? nodes.find((n) => n.url === leaderURL()) : nodes[Number(pick)];
  if (!target) return say("err", "There is no leader to read from.");
  if (!target.up) return say("err", target.id + " is not responding.");
  try {
    let j = await fetchJSON(target.url + "/get?key=" + encodeURIComponent(key), 3000);
    if (!j.found && j.leaderHint) {
      const lead = nodeByGRPC(j.leaderHint);
      if (lead) { target = lead; j = await fetchJSON(lead.url + "/get?key=" + encodeURIComponent(key), 3000); }
    }
    say(j.found ? "ok" : "", j.found ? key + " = " + j.value + "  (read from " + target.id + ")"
                                     : "No value for " + key + " on " + target.id + ".");
  } catch {
    say("err", target.id + " did not answer in time.");
  }
}

async function doBurst() {
  const btn = $("btn-burst");
  btn.disabled = true;
  const t0 = performance.now();
  let failed = 0;
  for (let batch = 0; batch < 10; batch++) {
    const jobs = [];
    for (let i = 0; i < 10; i++) {
      const k = "demo-" + Math.floor(Math.random() * 1e6);
      jobs.push(writeKey("/set?key=" + k + "&value=" + batch + "-" + i));
    }
    failed += (await Promise.all(jobs)).filter(Boolean).length;
  }
  const secs = (performance.now() - t0) / 1000;
  btn.disabled = false;
  if (failed === 100) return say("err", "No writes went through. Is there a leader?");
  const done = 100 - failed;
  say(failed ? "err" : "ok", "Wrote " + done + " keys in " + secs.toFixed(2) + " s" + (failed ? ", " + failed + " failed." : "."));
  addEvent("Wrote " + done + " random keys through the leader");
}

$("kv-form").addEventListener("submit", (e) => { e.preventDefault(); doSet(); });
$("btn-get").addEventListener("click", doGet);
$("btn-del").addEventListener("click", doDelete);
$("btn-burst").addEventListener("click", doBurst);

// ---------------------------------------------------------------- start

nodes.forEach(pollNode);
pollControl();
render();
