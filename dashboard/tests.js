// Load test and fault test panel. Talks to the control server, which runs the
// PowerShell harnesses and streams their output back over Server-Sent Events.
"use strict";

(function () {
  const $ = (id) => document.getElementById(id);
  const fmt = (n) => Number(n).toLocaleString();

  let running = false;
  let kind = null;
  let bench = {};   // { WRITE: {...}, READ: {...} }
  let phase = null;
  let rounds = [];  // [{ n, victim, ok }]
  let converged = null;
  let verdict = null;

  // ---------------------------------------------------------- stream

  function connect() {
    const es = new EventSource("/stream");
    es.onmessage = (ev) => {
      let e;
      try { e = JSON.parse(ev.data); } catch { return; }
      if (e.t === "start") onStart(e);
      else if (e.t === "line") onLine(e.s || "");
      else if (e.t === "end") onEnd(e.code);
    };
  }

  function onStart(e) {
    running = true; kind = e.kind;
    bench = {}; phase = null; rounds = []; converged = null; verdict = null;
    $("raw-out").textContent = "";
    $("results").textContent = "";
    setStatus(true);
    renderResults();
  }

  function onEnd(code) {
    running = false;
    setStatus(false, code);
    if (kind === "chaos") {
      const bad = rounds.filter((r) => !r.ok).length;
      verdict = code === 0 && bad === 0 && converged !== false
        ? { pass: true, text: "Passed. " + rounds.length + " rounds, no committed data lost." }
        : { pass: false, text: "Failed. " + (bad ? bad + " bad round(s). " : "") + (converged === false ? "Nodes did not converge." : "") };
    }
    renderResults();
    kind = null;
  }

  function onLine(s) {
    const raw = $("raw-out");
    raw.textContent += s + "\n";
    raw.scrollTop = raw.scrollHeight;
    if (kind === "bench") parseBench(s);
    else if (kind === "chaos") parseChaos(s);
  }

  // ---------------------------------------------------------- parsing

  // WRITE   ops=26020     6502 ops/s  errors=24
  //         latency  p50=2.8ms  p95=7.11ms  p99=24.48ms  max=68.71ms
  function parseBench(s) {
    let m = s.match(/^(WRITE|READ)\s+ops=(\d+)\s+([\d.]+)\s+ops\/s\s+errors=(\d+)/);
    if (m) {
      phase = m[1];
      bench[phase] = Object.assign(bench[phase] || {}, { rate: +m[3], errors: +m[4], ops: +m[2] });
      return renderResults();
    }
    m = s.match(/latency\s+p50=(\S+)\s+p95=(\S+)\s+p99=(\S+)/);
    if (m && phase) {
      Object.assign(bench[phase], { p50: m[1], p95: m[2], p99: m[3] });
      renderResults();
    }
  }

  // -- round 3/8 : killing :9002 (n2, leader) --
  function parseChaos(s) {
    let m = s.match(/round\s+(\d+)\/\d+\s*:\s*killing\s+:\d+\s+\((n\d+)(?:,\s*([^)]+))?\)/i);
    if (m) {
      rounds.push({ n: +m[1], victim: m[2] + (m[3] ? " (" + m[3] + ")" : ""), ok: null });
      return renderResults();
    }
    const cur = rounds[rounds.length - 1];
    if (cur && cur.ok === null) {
      if (/integrity ok/i.test(s)) cur.ok = true;
      else if (/mismatch|lost quorum|FAIL/i.test(s)) cur.ok = false;
      else return;
      return renderResults();
    }
    if (/all \d+ nodes hold/i.test(s)) { converged = true; renderResults(); }
    else if (/nodes diverge|lag/i.test(s) && /FAIL/.test(s)) { converged = false; renderResults(); }
  }

  // ---------------------------------------------------------- rendering

  function cell(tr, text, cls) {
    const td = document.createElement("td");
    td.textContent = text;
    if (cls) td.className = cls;
    tr.appendChild(td);
  }

  function head(tr, labels) {
    labels.forEach((t) => { const th = document.createElement("th"); th.textContent = t; tr.appendChild(th); });
  }

  function renderResults() {
    const out = $("results");
    out.textContent = "";
    if (kind === "bench" || (!kind && Object.keys(bench).length)) {
      const t = document.createElement("table");
      const h = t.createTHead().insertRow();
      head(h, ["", "ops/s", "p50", "p95", "p99", "errors"]);
      const body = t.createTBody();
      [["WRITE", "Writes"], ["READ", "Reads"]].forEach(([k, label]) => {
        const d = bench[k];
        if (!d) return;
        const tr = body.insertRow();
        cell(tr, label);
        cell(tr, d.rate != null ? fmt(Math.round(d.rate)) : "");
        cell(tr, d.p50 || ""); cell(tr, d.p95 || ""); cell(tr, d.p99 || "");
        cell(tr, d.errors != null ? fmt(d.errors) : "");
      });
      out.appendChild(t);
      if (!running && Object.keys(bench).length) {
        const p = document.createElement("p");
        p.className = "note";
        p.textContent = "A few errors per worker are normal: each worker's first request can reach a follower before it learns who the leader is.";
        out.appendChild(p);
      }
    } else if (kind === "chaos" || rounds.length) {
      const t = document.createElement("table");
      t.className = "rounds";
      head(t.createTHead().insertRow(), ["Round", "Killed", "Result"]);
      const body = t.createTBody();
      rounds.forEach((r) => {
        const tr = body.insertRow();
        cell(tr, "Round " + r.n);
        cell(tr, r.victim, "left");
        cell(tr, r.ok === null ? "checking" : r.ok ? "data intact" : "DATA MISSING", r.ok === false ? "bad" : r.ok ? "good" : "");
      });
      if (converged !== null) {
        const tr = body.insertRow();
        cell(tr, "Final");
        cell(tr, "all nodes compared", "left");
        cell(tr, converged ? "all agree" : "nodes differ", converged ? "good" : "bad");
      }
      out.appendChild(t);
      if (verdict) {
        const v = document.createElement("p");
        v.className = "verdict " + (verdict.pass ? "pass" : "fail");
        v.textContent = verdict.text;
        out.appendChild(v);
      }
    }
  }

  function setStatus(live, code) {
    const el = $("run-status");
    el.className = "run-status" + (live ? " live" : "");
    el.textContent = live ? "Running a " + (kind === "chaos" ? "fault" : "load") + " test..."
      : code === undefined ? "No test running." : "Finished (exit code " + code + ").";
    $("btn-stop-run").disabled = !live;
    ["b-run", "c-run"].forEach((id) => { $(id).disabled = live; });
  }

  // ---------------------------------------------------------- controls

  async function start(path, params) {
    const r = await fetch(path + "?" + new URLSearchParams(params), { method: "POST" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) $("run-status").textContent = j.error || "Could not start the test.";
  }

  $("load-form").addEventListener("submit", (e) => {
    e.preventDefault();
    start("/bench", {
      writers: $("b-writers").value, readers: $("b-readers").value,
      duration: ($("b-seconds").value || "5") + "s", threshold: 2000,
    });
  });

  $("fault-form").addEventListener("submit", (e) => {
    e.preventDefault();
    start("/chaos", {
      rounds: $("c-rounds").value, threshold: 25,
      killLeader: $("c-killleader").checked ? "1" : "0",
    });
  });

  $("btn-stop-run").addEventListener("click", async () => {
    $("btn-stop-run").disabled = true;
    await fetch("/stop", { method: "POST" }).catch(() => {});
  });

  connect();
  setStatus(false);
})();
