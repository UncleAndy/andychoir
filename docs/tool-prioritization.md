# Prioritized Tool Selection (Origin-Aware)

This document describes the design and implementation plan for prioritizing tools based on the origin of the session request.

## 1. Goal
When an agent decides to use a tool, the system should prioritize the host that initiated the request (the "origin host"). This reduces network latency, ensures data locality (access to client's local files/state), and decreases overall network load.

## 2. Architecture
The prioritization logic is implemented at the **Host/Orchestrator** level. The LLM agent remains "topology-blind" — it simply requests a tool by name (e.g., `tool:calculator`), and the host infrastructure resolves this into a specific network target.

### Tool Selection Hierarchy (The 3 Tiers)

When the Orchestrator receives a tool request `tool:T` for session $S$, it follows this priority order:

#### Tier 1: Source Host (Highest Priority)
*   **Check:** Does the host that initiated session $S$ possess tool `T`?
*   **Data Source:** `NetInner.session_origin` $\to$ `NetInner.origin_tools[source_id]`.
*   **Result:** If found $\to$ target = `host:<Source_ID>:tool:T`.

#### Tier 2: Local Host (Medium Priority)
*   **Check:** Does the current host (where the agent is running) possess tool `T`?
*   **Data Source:** `crate::plugin::engine::local_tools()`.
*   **Result:** If found $\to$ target = `tool:T` (local execution).

#### Tier 3: Network Mesh (Lowest Priority)
*   **Check:** Does any other host in the mesh possess tool `T`?
*   **Data Source:** `NetInner.origin_tools` (all other nodes).
*   **Result:** If found $\to$ target = `host:<Remote_ID>:tool:T`.
*   **Selection:** If multiple nodes provide the tool, the one with the shortest path in the FIB is chosen.

---

## 3. Logic Flow

1. **Request:** Agent returns `tool:T`.
2. **Origin Lookup:** Orchestrator retrieves `source_id` from `session_origin` for the current session.
3. **Tiered Search:**
    - Search `origin_tools[source_id]` $\to$ found? Return `host:source_id:tool:T`.
    - Search `local_tools()` $\to$ found? Return `tool:T`.
    - Search all `origin_tools` $\to$ found? Return `host:remote_id:tool:T`.
    - Not found $\to$ Return "Tool not available in mesh" error.
4. **Routing:** The resolved target is passed to `forward()`, which routes the event via the FIB.

**Special Case:** If the agent explicitly specifies a target (e.g., `host:C:tool:calculator`), the Orchestrator **must not** override it, allowing the agent to request a specific "professional" tool regardless of priority.

## 4. Implementation Plan

### Phase 1: API Extension
*   Implement `resolve_tool_target(session_id, tool_name)` helper function in the Orchestrator.
*   Ensure it has access to `NetInner` (for `session_origin` and `origin_tools`) and the local plugin registry.

### Phase 2: Integration
*   Integrate `resolve_tool_target` into the event loop: instead of blindly forwarding `tool:T`, the Orchestrator now resolves the optimal target first.
*   Add debug logging: `[Orchestrator] Tool 'T' resolved to host X (Tier N)`.

### Phase 3: Verification
*   **Unit Tests:** Mock `NetInner` with various tool distributions and verify the resolved targets.
*   **Integration Test (Ring Topology):**
    *   Setup: Node A (Front), Node B (Agent), Node C (Tool).
    *   Scenario 1: Tool exists on A $\to$ Verify request goes B $\to$ A.
    *   Scenario 2: Tool only on C $\to$ Verify request goes B $\to$ C.
    *   Scenario 3: Tool only on B $\to$ Verify local execution.
