// Load test and fault test panel. Talks to the control server, which runs the
// PowerShell harnesses and streams their output back over Server-Sent Events.
// On (re)connect the server replays the current run, so a reload mid-run loses
// nothing. Uses $, fmt and postAction from app.js, which loads first.
"use strict";

(function () {
  let kind = null;
  let bench = {};       // { WRITE: {...}, READ: {...} }
  let phase = null;
  let rounds = [];      // [{ n, victim, ok, detail }]
  let convergence = null; // { ok, detail }
  let verdict = null;     // { pass, text }

  // ---------------------------------------------------------- stream

  function connect() {
    const es = new EventSource("/stream");
    es.onmessage = (ev) => {
      let e;
      try { e = JSON.parse(ev.data); } catch { return; }
      if (e.t === "reset") { reset(null); setStatus(false); } // replayed events set the real status
      else if (e.t === "start") onStart(e);
      else if (e.t === "line") onLine(e.s || "");
      else if (e.t === "end") onEnd(e.code);
    };
  }

  function reset(newKind) {
    kind = newKind;
    bench = {}; phase = null; rounds = []; convergence = null; verdict = null;
    $("raw-out").textContent = "";
    renderResults();
  }

  function onStart(e) {
    reset(e.kind);
    setStatus(true);
  }

  function onEnd(code) {
    setStatus(false, code);
    if (kind === "chaos" && !verdict) {
      // The script ended (or was stopped) before printing its verdict line.
      verdict = { pass: false, text: "Did not finish (exit code " + code + "). See the raw output." };
    } else if (kind === "bench" && code !== 0) {
      verdict = { pass: false, text: "The load test reported a problem (exit code " + code + "). See the errors above or the raw output." };
    }
    renderResults();
  }

  function onLine(s) {
    const raw = $("raw-out");
    raw.textContent += s + "\n";
    raw.scrollTop = raw.scrollHeight;
    if (kind === "bench") parseBench(s);
    else if (kind === "chaos") parseChaos(s);
  }

  // ---------------------------------------------------------- parsing
  // These follow the output formats documented in benchmark/main.go (print) and
  // at the top of scripts/chaos.ps1. Patterns are anchored so that stray text
  // (an error message containing "FAIL", say) can never be mistaken for a result.

  // WRITE   ops=26020     6502 ops/s  errors=0
  //         error x3: context deadline exceeded
  //         latency  p50=2.80ms  p95=7.11ms  p99=24.48ms  max=68.71ms
  function parseBench(s) {
    let m = s.match(/^(WRITE|READ)\s+ops=(\d+)\s+([\d.]+)\s+ops\/s\s+errors=(\d+)/);
    if (m) {
      phase = m[1];
      bench[phase] = { ops: +m[2], rate: +m[3], errors: +m[4], reasons: [] };
    } else if (phase && (m = s.match(/^\s+error x(\d+): (.*)$/))) {
      bench[phase].reasons.push(m[2] + " (" + fmt(+m[1]) + ")");
    } else if (phase && (m = s.match(/^\s+latency\s+p50=(\S+)\s+p95=(\S+)\s+p99=(\S+)/))) {
      Object.assign(bench[phase], { p50: m[1], p95: m[2], p99: m[3] });
    } else {
      return;
    }
    renderResults();
  }

  // ROUND 3 KILL n2 (leader)
  // ROUND 3 RESULT=OK all 33 acknowledged keys read back
  // CONVERGENCE RESULT=FAIL ...
  // CHAOS RESULT=PASS 8 rounds, 38 acknowledged writes, none lost
  function parseChaos(s) {
    let m;
    if ((m = s.match(/^ROUND (\d+) KILL (n\d+) \(([^)]+)\)/))) {
      rounds.push({ n: +m[1], victim: m[2] + " (" + m[3] + ")", ok: null, detail: "" });
    } else if ((m = s.match(/^ROUND (\d+) RESULT=(OK|FAIL) ?(.*)$/))) {
      const r = rounds.find((x) => x.n === +m[1]);
      if (r) { r.ok = m[2] === "OK"; r.detail = m[3]; }
    } else if ((m = s.match(/^CONVERGENCE RESULT=(OK|FAIL) ?(.*)$/))) {
      convergence = { ok: m[1] === "OK", detail: m[2] };
    } else if ((m = s.match(/^CHAOS RESULT=(PASS|FAIL) ?(.*)$/))) {
      verdict = { pass: m[1] === "PASS", text: (m[1] === "PASS" ? "Passed: " : "Failed: ") + m[2] };
    } else if ((m = s.match(/^ERROR: (.*)$/))) {
      rounds.push({ n: "-", victim: "", ok: false, detail: m[1] });
    } else {
      return;
    }
    renderResults();
  }

  // ---------------------------------------------------------- rendering

  function cell(tr, text, cls) {
    const td = document.createElement("td");
    td.textContent = text;
    if (cls) td.className = cls;
    tr.appendChild(td);
  }

  function table(labels, cls) {
    const t = document.createElement("table");
    if (cls) t.className = cls;
    const h = t.createTHead().insertRow();
    labels.forEach((l) => { const th = document.createElement("th"); th.textContent = l; h.appendChild(th); });
    return t;
  }

  function note(text, cls) {
    const p = document.createElement("p");
    p.className = cls;
    p.textContent = text;
    return p;
  }

  function renderBench(out) {
    const t = table(["", "ops/s", "p50", "p95", "p99", "errors"]);
    const body = t.createTBody();
    const reasons = [];
    [["WRITE", "Writes"], ["READ", "Reads"]].forEach(([k, label]) => {
      const d = bench[k];
      if (!d) return;
      const tr = body.insertRow();
      cell(tr, label);
      cell(tr, fmt(Math.round(d.rate)));
      cell(tr, d.p50 || ""); cell(tr, d.p95 || ""); cell(tr, d.p99 || "");
      cell(tr, fmt(d.errors), d.errors ? "bad" : "");
      d.reasons.forEach((r) => reasons.push(label + ": " + r));
    });
    out.appendChild(t);
    if (reasons.length) out.appendChild(note("Errors: " + reasons.join("; "), "note"));
    if (verdict) out.appendChild(note(verdict.text, "verdict fail"));
  }

  function renderChaos(out) {
    const t = table(["Round", "Killed", "Result"], "rounds");
    const body = t.createTBody();
    rounds.forEach((r) => {
      const tr = body.insertRow();
      cell(tr, r.n === "-" ? "Error" : "Round " + r.n);
      cell(tr, r.victim, "left");
      const text = r.ok === null ? "checking" : r.ok ? "all data intact" : "FAILED: " + r.detail;
      cell(tr, text, r.ok === false ? "bad left" : r.ok ? "good" : "");
    });
    if (convergence) {
      const tr = body.insertRow();
      cell(tr, "Final");
      cell(tr, "all nodes compared", "left");
      cell(tr, convergence.ok ? "identical on every node" : "FAILED: " + convergence.detail,
        convergence.ok ? "good" : "bad left");
    }
    out.appendChild(t);
    if (verdict) out.appendChild(note(verdict.text, "verdict " + (verdict.pass ? "pass" : "fail")));
  }

  function renderResults() {
    const out = $("results");
    out.textContent = "";
    if (kind === "bench" && (Object.keys(bench).length || verdict)) renderBench(out);
    else if (kind === "chaos" && (rounds.length || verdict)) renderChaos(out);
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
    const err = await postAction(path + "?" + new URLSearchParams(params));
    if (err) $("run-status").textContent = "Could not start the test: " + err;
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

  $("btn-stop-run").addEventListener("click", () => {
    $("btn-stop-run").disabled = true;
    postAction("/stop");
  });

  connect();
  setStatus(false);
})();
