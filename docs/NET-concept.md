# Mesh Networking for AndyChoir: Decentralized Discovery, Routing, and Dedup

## 1. Overview

AndyChoir supports a decentralized mesh network of hosts. Each host can connect to one or more neighbors, forming a graph. The goal is to enable events to reach any target host in the network without requiring a central server (root) or complex manual configuration.

The system achieves this through a combination of **Link-State Discovery**, **Local Shortest-Path Routing**, and **Probabilistic Dedup**.

## 2. Decentralized Discovery (Link-State)

Instead of a central coordinator, every host independently maintains a **Link-State Database (LSDB)** — a local map of the entire network topology.

### Hello Protocol
- **HELLO Packets:** Every host periodically broadcasts a `HELLO` message to its direct neighbors.
- **Payload:** A `HELLO` message contains:
  - `source_id`: The UUID of the sending host (must be a UUID v4/v7, not a human-readable string — see §5).
  - `neighbors`: A list of hosts this node is currently connected to.
  - `capabilities`: A set of tools/plugins available on this host.
- **Propagation:** When a host receives a `HELLO` from a neighbor, it updates its local LSDB and floods the message to other neighbors (if the information is new or updated).

### LSDB Maintenance
- **Convergence:** Within a few rounds, every host has a complete graph of the network: `Node A -> [B, C], Node B -> [A, D], ...`.
- **Failure Detection:** If a neighbor is not heard from for $N$ seconds, it is marked as offline, and the LSDB is updated.
- **Graceful Leave:** A `BYE` message notifies neighbors to remove the host immediately.

## 3. Routing: Local Shortest Path

Since every host has a complete map of the network (LSDB), it can calculate the optimal route to any target without a central root.

### Route Calculation
- **Shortest Path:** Upon receiving an event for a distant `target_id`, the host runs a shortest-path algorithm (e.g., Dijkstra) on its local LSDB.
- **Next Hop:** The host identifies the immediate neighbor that is on the shortest path to the target and forwards the event only to that neighbor.
- **Loop Avoidance:** Shortest-path routing on a static snapshot of LSDB is inherently loop-free. If the topology changes during transit, the **TTL** (Time-To-Live) mechanism acts as a safety net.

### Routing Table
To avoid recalculating the path for every packet, each host maintains a local **Routing Table** (FIB):
`target_id` $\rightarrow$ `next_hop_neighbor`.

## 4. Dedup and Loop Protection

To ensure that events are processed exactly once and don't circulate forever, the system uses a combination of probabilistic dedup and TTL.

### Bloom Filter Dedup
To avoid storing every `event_id` (which would blow up memory), each host uses a **Bloom Filter**:
- **Fixed Size:** A small bit-array (e.g., 4 KB) is allocated.
- **Verification:** When an event arrives, its ID is hashed $k$ times. If all $k$ bits are set, it is marked as a "probable duplicate" and dropped.
- **Windowed Reset:** To prevent the filter from filling up, it is reset periodically (e.g., every 60 seconds). This creates a "sliding window" of dedup.
- **Cost:** Constant memory, $O(1)$ check, small false-positive rate.

### TTL (Time-To-Live)
As a final safety measure, every `NetMessage` includes a 1-byte `ttl` field:
- **Decrement:** Every hop decrements the `ttl` by 1.
- **Drop:** If `ttl` reaches 0, the packet is discarded.
- **Purpose:** Prevents infinite loops during LSDB convergence or in case of routing errors.

## 5. Wire Protocol Changes

To support this, the `NetMessage` is extended:
- `source_id`: The UUID of the original sender of the event (must be a UUID v4/v7, not a human-readable string).
- `event_id`: A unique UUID for the event (used by Bloom filter).
- `ttl`: Time-to-live counter.
- `message_type`: `HELLO` (topological update) or `EVENT` (actual data).

**Important:** The `source_id` and `event_id` fields **must** be UUIDs (v4 random or v7 time-ordered), not human-readable strings like `"host-a"` or `"console"`. This is required for:
1. **Global uniqueness** — no coordination needed to assign IDs; each host generates its own UUID on first boot.
2. **Dedup** — Bloom filter relies on uniform distribution of UUIDs for correct false-positive rates.
3. **Debugging** — UUIDs are opaque, avoiding accidental coupling to hostnames/roles.

If the current codebase uses string-based host identifiers (e.g., `"host-a"`, `"console"`), they must be replaced with UUIDs during the implementation.

## 6. Summary of Performance

| Component | Memory | Overhead | Complexity |
|----------|--------|-----------|------------|
| **Discovery** | $O(V + E)$ | Low (periodical) | $O(V+E)$ |
| **Routing** | $O(V)$ | Zero (table lookup) | $O(1)$ |
| **Dedup** | Fixed (4 KB) | Zero | $O(k)$ |
| **TTL** | 1 byte | Zero | $O(1)$ |

*(V = number of hosts, E = number of links)*

## 7. Recommended Rust Libraries

This section provides an analysis of existing Rust crates that can help implement the mechanisms described above. The analysis is based on the state of the Rust ecosystem as of 2024-2025.

### 7.1 Bloom Filter (Dedup)

| Crate | Description | Suitable? |
|-------|-------------|-----------|
| **`fastbloom`** | SIMD-optimized bloom filter (AVX2/NEON), `no_std`, fixed-size `[u64; N]`, XXH3 or SipHash | ✅ **Optimal** — production-tested, SIMD-accelerated, `check_and_add()` API |
| **`bloom`** | Classic bloom filter, fixed size, `no_std` | ⚠️ Works, but scalar (no SIMD) |
| **`growable-bloom`** | Growing bloom filter (multiple layers) | ❌ No — we don't need growth, only fixed + reset |
| **`twox-hash`** + custom | Fast XXH3 64-bit hash for k hash functions | ⚠️ Viable, but `fastbloom` is the same approach, production-tested |

**Recommendation:** `fastbloom` — SIMD-optimized (AVX2/NEON), `no_std` compatible, fixed-size `[u64; N]` array (exactly our 512-byte case), with `check_and_add(item) -> bool` API matching our dedup semantics. Use the `xxh3` feature flag for non-cryptographic hashing (optimal for dedup).

Example usage:
```rust
use fastbloom::FastBloom;

// 512 bytes = 4096 bits, k=3 hash functions, XXH3 hasher
let mut filter = FastBloom::with_num_bits_and_hasher(4096, 3, xxh3_64);

// Returns true if item was probably already seen (duplicate)
if filter.check_and_add(event_id.as_bytes()) {
    // drop duplicate
}

// Windowed reset (every 60 seconds)
filter.clear();
```

**Cargo.toml:**
```toml
[dependencies]
fastbloom = { version = "0.10", features = ["xxh3"] }
```

### 7.2 Graph + Shortest Path (Routing)

| Crate | Description | Suitable? |
|-------|-------------|-----------|
| **`petgraph`** | De-facto standard: Graph, DiGraph, BFS, Dijkstra, A* | ✅ Yes — `petgraph::algo::dijkstra` returns distance map, easy to extract next-hop |
| **`pathfinding`** | Dijkstra, A*, BFS, DFS — more specialized | ✅ Yes — slightly higher-level for pathfinding |
| **`graph`** | Alternative to petgraph | ⚠️ Less popular |

**Recommendation:** `petgraph` — most mature, well-documented, supports `no_std` via `alloc`. For andychoir (small graphs, <100 nodes), performance is not critical but API convenience matters.

Example usage:
```rust
use petgraph::graph::{Graph, NodeIndex, UnGraph};
use petgraph::algo::dijkstra;

let mut g = UnGraph::<&str, u32>::new_undirected();
let a = g.add_node("host-a");
let b = g.add_node("host-b");
let c = g.add_node("host-c");
g.add_edge(a, b, 1);
g.add_edge(b, c, 1);

// Dijkstra from target to find shortest paths
let dists = dijkstra(&g, c, None, |_| 1);
// next-hop = neighbor with dist == 1 on shortest path to target
```

### 7.3 Discovery / Gossip Protocol

| Crate | Description | Suitable? |
|-------|-------------|-----------|
| **`libp2p`** | Full stack: Kademlia DHT, mDNS, gossipsub, identify, ping | ⚠️ Heavy, but has everything — can use only `libp2p::kademlia` or `gossipsub` |
| **`memberlist`** | HashiCorp memberlist — SWIM gossip protocol | ❌ No official Rust port (unofficial exist but raw) |
| **`raft`** (e.g., `raft-rs`, `openraft`) | Consensus, not discovery | ❌ No — different task |
| **Custom** | HELLO packets on top of existing WS bridge | ✅ Optimal for andychoir |

**Recommendation:** For andychoir, **custom implementation** on top of the existing `net.rs` (WebSocket bridge). Reasons:
- We already have `NetMessage` and `forward()` — just add `HELLO`/`BYE` types
- Small scale (2-20 hosts) — full gossip is overkill
- `libp2p` pulls async, tokio, crypto — excessive for a trusted AI host network

If ready-made is desired — `libp2p::swarm` + `libp2p::ping` + `libp2p::identify` provides discovery out of the box, but at the cost of complexity and dependencies.

### 7.4 Supporting Components

| Task | Crate | Note |
|------|-------|------|
| **Event ID (UUID)** | `uuid` | v4 (random) or v7 (time-ordered) |
| **Hashes for Bloom** | `twox-hash` | XXH3, fast, non-cryptographic |
| **Periodic HELLO** | `tokio::time::interval` | already used in the project |
| **Topology config** | `serde` + existing `config.rs` | extend `remotes` |

### 7.5 Final Stack Recommendation

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Bloom filter | **`fastbloom`** (SIMD, `xxh3` feature) | Fixed 512 B, SIMD-accelerated, `check_and_add()` API |
| Graph + Dijkstra | **`petgraph`** | Mature, convenient API, `dijkstra` out of the box |
| Discovery | **Custom** on top of `net.rs` | Already has WS bridge + `NetMessage`, add HELLO/BYE |
| Event ID | **`uuid`** | Already used in the project |

**Additional dependencies:** only `petgraph` (if not writing graph by hand). Bloom filter and discovery are custom, no new dependencies.

## 8. Configuration & Examples

### 8.1 What the user configures

The user configures **only direct links** (`remotes`) — never the full topology.
Every host discovers the rest of the mesh automatically via HELLO flooding and
builds its own LSDB + FIB.

`NetConfig` (in `src/config/config.rs`):

```yaml
net:
  node_id: "00000000-0000-0000-0000-0000000000a1"  # UUID v4/v7 (auto-generated if invalid)
  listen_port: 8090        # 0 = do not listen (client-only host behind NAT)
  token: []                # tokens accepted on INCOMING connections
  remotes:                 # persistent OUTGOING WS connections (direct links only)
    - url: "ws://127.0.0.1:8091/net"
      token: "secret-b"
      targets: []          # optional pinned routing (FIB has priority)
```

- `remotes` is the list of **direct neighbors** you dial out to. You do **not**
  list every host in the mesh here — only the ones you have a direct link to.
- `targets` on a remote is **optional**. If empty, routing is purely by FIB
  (shortest path). If set, it provides explicit pinned routing as a fallback.
- `listen_port: 0` means "I don't accept incoming connections" (e.g. a host
  behind a NAT/router). It still connects outbound, and replies come back over
  that same WebSocket (bidirectional).

### 8.2 Event target format

To address a tool/agent on a **remote** host, the target uses the `host:` prefix:

```
host:<node_id>:<tool>
```

Examples:
- `host:00000000-0000-0000-0000-0000000000c3:tool:calculator` → run `tool:calculator`
  on host `…c3`, routed by FIB (possibly via intermediate hops).
- `tool:calculator` (no `host:` prefix) → local plugin, **not forwarded**.
- `agent:demo`, `mcp:…`, `session:…` → local (not forwarded).

When the bridge receives an event whose target starts with `host:`, it:
1. extracts `<node_id>`,
2. looks up `next_hop` in the FIB,
3. sends the event to `next_hop`'s outgoing/incoming channel.

The `target` field is **preserved unchanged** along the path — intermediate hops
re-route it the same way (store-and-forward by next-hop).

### 8.3 Three-host mesh example (A → B → C)

Topology: A and C both dial B directly; B is the relay. A never dials C.

**Host A** (`listen_port: 0`, dials B):
```yaml
net:
  node_id: "00000000-0000-0000-0000-0000000000a1"
  listen_port: 0
  remotes:
    - url: "ws://127.0.0.1:8091/net"
      token: "secret-b"
```

**Host B** (relay, listens on 8091, dials nobody):
```yaml
net:
  node_id: "00000000-0000-0000-0000-0000000000b2"
  listen_port: 8091
  token: ["secret-b", "secret-c"]
  remotes: []
```

**Host C** (`listen_port: 0`, dials B):
```yaml
net:
  node_id: "00000000-0000-0000-0000-0000000000c3"
  listen_port: 0
  remotes:
    - url: "ws://127.0.0.1:8091/net"
      token: "secret-c"
```

Flow when A emits `host:…c3:tool:calculator`:
1. A's HELLO (periodic) tells B "I see A". C's HELLO tells B "I see C".
   B floods both HELLOs → A learns `B sees C`, C learns `B sees A`.
2. A's FIB: `C → next_hop B`.
3. A forwards the event to B (its `outbound[B]` channel).
4. B receives it, re-routes by FIB (`C → next_hop C` — direct neighbor),
   forwards to C.
5. C executes `tool:calculator` locally.

No manual `targets`, no central server — pure link-state mesh.

### 8.4 Failure handling

- **Silent failure:** if a neighbor stops sending HELLO for `LSDB_TIMEOUT_SECS`
  (30s), it is removed from LSDB and the FIB is recomputed. Routes through it
  disappear; alternate paths (if any) take over.
- **Graceful leave:** on `drop()` of the bridge, a `BYE` is broadcast to all
  neighbors, so they drop the node immediately.
- **Loop protection:** every hop decrements `ttl` (starts at 16); at 0 the event
  is dropped. Shortest-path routing on a static LSDB snapshot is loop-free;
  TTL is the safety net during convergence.

