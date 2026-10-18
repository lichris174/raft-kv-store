# UI redesign

The project had two separate pages (a cluster console and a benchmark/chaos page) that
looked like a stock dark "ops dashboard". This documents what was researched, what the
new UI does, why, and how it was checked.

## Goals

1. **Intuitive:** someone who has never heard of Raft should be able to open the page,
   break the cluster, and understand what happened.
2. **Demoable end to end** from one page: start the cluster, write, kill a node, watch
   recovery, run the tests. No terminal and no second page.
3. **Does not look AI-generated.**

## Research: what makes a UI read as "AI"

Sources: [impeccable.style, "The visible tells of AI design"](https://impeccable.style/slop),
[SaaS UI, "SaaS UI looks AI generated"](https://www.saasui.design/blog/saas-ui-looks-ai-generated),
and for operator dashboards a few practitioner guides (no authoritative standard turned up,
so these are heuristics, not rules): [dashboard UI design](https://uxmagic.ai/blog/dashboard-ui-design),
[dashboard design with Claude Code](https://github.com/Laith0003/ux-skill/wiki/How-to-design-a-dashboard-with-Claude-Code).

The recurring tells:

| Area | Tell |
|---|---|
| Type | Inter (or Geist) as the default; flat hierarchy; tiny tracked-out uppercase labels; an "eyebrow" label above a heading that repeats it |
| Color | Purple/indigo gradients; neon on dark; glow and halos; gradient text; cream/beige by reflex |
| Layout | Grids of identical cards; cards inside cards; hero-metric tiles; thick colored stripe on a rounded card; equal spacing everywhere |
| Motion | Pulsing status dots when nothing changes; marquees; hover zoom; bounce easing |
| Copy | Hype words ("supercharge"), forced contrast, an em dash in every sentence |

For operator tools the guides add: reserve status color for real state, never rely on color
alone, favor density over decoration, keep critical actions visible instead of hidden in
menus, and design the empty, error and degraded states, not just the happy path.

### The old UI, honestly

Measured against that list, the previous dashboard had: tiny (8 to 10 px) tracked-out
uppercase labels throughout, an eyebrow line above the title, a grid of identical node
cards with a colored top stripe, a scrolling text ticker, amber-on-near-black "terminal"
styling, and a split between two pages. Not egregious, but recognizably the default.

## Direction

An operator tool, not a landing page: the first screen *is* the product. Light by default
with a dark variant that follows the system (`?theme=light|dark` overrides it). Color only
means something: ink for structure, green/amber/red for cluster state, amber for "not yet
committed". One accent behavior: the leader is the only filled tag on the page.

| Tell | What the UI does instead |
|---|---|
| Inter | IBM Plex Sans for text, IBM Plex Mono for ids and numbers, with system fallbacks if the font host is unreachable |
| Card grid | Nodes are table-like rows separated by hairlines, no cards at all |
| Hero metric tiles | None. The headline is a plain-language sentence about cluster health |
| Tiny uppercase labels | Sentence-case labels at normal size; section headings are ordinary headings |
| Eyebrow above title | None |
| Glow, gradients, glass | None. No shadows, one hairline border style, 4 px radius |
| Pulsing dots, marquee | No ambient animation. The only motion is the log bars easing to their new length |
| Hype copy | Copy states what is true, e.g. "The cluster can lose 2 nodes and keep working." |
| Everything is the same size | Banner sentence, node rows and the side column have distinct weight and spacing |

## What the page does

- **Health banner.** One sentence of state, computed from node status: *Healthy*,
  *Degraded* (and how many more nodes can fail), *Electing a leader*, *No majority: writes
  are blocked*, plus the "no cluster running" and "test running" states. Text, not just color.
- **Node rows.** Role, term, key count, commit index, and a **log bar** per node built from
  real status fields: grey = folded into a snapshot, dark = committed, amber = not yet
  committed. Replication, lag and compaction are visible without explanation.
- **Kill and Start** on each row, **Start cluster / Stop all** above. These are real process
  kills (a crash, no clean shutdown); Start resumes from the node's saved data.
- **Read and write box** with a **Read from** selector (leader or any node), so replication
  can be demonstrated by reading the same key from different nodes. **Write 100 random keys**
  makes the log bars and compaction move.
- **"What just happened" feed**, derived by diffing successive status polls: elections,
  leader changes, nodes stopping and returning, log compaction, your own actions.
- **Tests** (load and fault) on the same page with a results table instead of a wall of
  text; the raw output sits in a collapsed section.
- **Degrades cleanly.** Served by nginx under Docker (no control server) the page still shows
  the cluster and the key-value box; kill/start and tests are hidden.

## Architecture

- `dashboard/index.html`, `style.css`, `app.js` (cluster, rows, feed, key-value),
  `tests.js` (load and fault test panel). Plain files, no build step, no framework.
- `control/cluster.go` owns the five demo `raftnode` processes (start, kill, start all,
  stop all, wipe data). `control/main.go` exposes it and the test runners.
- Node ports follow the existing convention (gRPC 9001 to 9005, HTTP 8001 to 8005). The demo
  cluster uses `--snapshot-threshold 50` so a burst of 100 writes visibly compacts the log, and
  `--read-mode follower` so any node can serve a read.
- The old `control-dashboard/` page was removed; its features live in the Tests section.

### Security changes made while in there

The control server previously listened on all interfaces with `Access-Control-Allow-Origin: *`
and started or killed processes on a plain GET. Any web page the user visited could have
driven it (including `/stop`, which kills every `raftnode`). Now it listens on `127.0.0.1`,
sends no CORS headers, and every mutating endpoint requires POST. The node HTTP gateway
(`/set`, `/del` over GET with open CORS) was fixed later: writes need POST and browser
requests must come from a local page (see `RELIABILITY.md`).

## A bug the new UI exposed

The feed made a real problem visible: after restarting a killed node the term jumped by about
10 within seconds. See [`HARDENING.md`, step 4](HARDENING.md#step-4-a-rejoining-node-must-not-depose-the-leader).
The earlier chaos tests could not see it because they only check that data survives.

## Verification

- Driven in a real browser: start cluster from the page, write 100 keys, kill the leader
  (election events, Degraded banner, term +1), restart it (catch-up), run the fault test from
  the page (live results table), light and dark themes, and a 390 px wide layout.
- `go test ./...`: 41 pass, including new tests for the cluster manager's argument building and
  error handling.
- The browser tooling could not resize the real window, so the narrow layout was checked
  through a temporary 390 px iframe (since deleted).

## Not done

- No automated UI tests (the page was exercised manually). A Playwright smoke test for the
  start, kill, restart path would be the next addition.
- The load-test results table shows throughput, percentiles and errors but no chart.
- Nodes are fixed at five; the page does not add or remove members.
- Control server and demo data paths are Windows-tested only; the harness scripts are PowerShell.
