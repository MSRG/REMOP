# Blocked Nested Loop Join: Design and Implementation

## Overview

This document describes the design and implementation of the **Blocked Nested Loop Join (NLJ) Optimizer** in DuckDB, which extends the standard nested loop join to handle large datasets that exceed available memory through blocked execution with intelligent memory management.

## Table of Contents

1. [Background and Motivation](#background-and-motivation)
2. [Architecture Overview](#architecture-overview)
3. [Operator Memory Policy](#operator-memory-policy)
4. [Execution Workflow](#execution-workflow)
5. [Memory Management](#memory-management)
6. [Block Pinning Strategy](#block-pinning-strategy)
7. [Result Buffering](#result-buffering)
8. [Implementation Details](#implementation-details)
9. [Blockwise NLJ Optimized](#blockwise-nlj-optimized)

---

## Background and Motivation

### Problem Statement

The standard nested loop join materializes the entire right-hand side (RHS) in memory, which causes memory exhaustion when:
- RHS exceeds available memory budget
- Both LHS and RHS are large
- Memory-intensive joins produce large intermediate results

### Solution: Blocked Execution

The optimized NLJ implements a **blocked execution strategy**:
1. **Materialize RHS once** into buffer-managed storage
2. **Buffer LHS chunks** up to a memory budget
3. **Process RHS in blocks** - iterate through RHS blocks while keeping LHS buffer pinned
4. **Buffer join results** to avoid blocking on output
5. **Repeat** until all LHS chunks are processed

### Key Benefits

- ✅ **Memory-bounded execution**: Respects configurable memory budgets
- ✅ **Reduced swap I/O**: Minimizes disk reads/writes through intelligent blocking
- ✅ **No query failure**: Handles arbitrarily large inputs without OOM
- ✅ **Buffer manager integration**: Leverages DuckDB's buffer management infrastructure
- ✅ **Performance**: Optimizes for memory-resident execution when possible

---

## Architecture Overview

### Component Hierarchy

```
PhysicalNestedLoopJoinOptimized (operator)
    ├── OperatorMemoryPolicy (memory configuration)
    │   └── NLJPolicyConfig (budget allocation)
    │
    ├── BlockedNLJGlobalState (shared state)
    │   ├── right_payload_data (RHS materialized)
    │   ├── right_condition_data (RHS conditions)
    │   ├── use_blocked_mode (decision flag)
    │   └── budgets (RHS, LHS, output)
    │
    └── BlockedNLJOperatorState (per-thread state)
        ├── Phase (execution state machine)
        ├── LHS Buffer (payload + conditions)
        ├── Block Pinning (handles for active blocks)
        ├── Result Buffer (join output accumulation)
        └── Scan States (LHS, RHS, results)
```

### Class Diagram

```
┌─────────────────────────────────────┐
│  PhysicalNestedLoopJoinOptimized    │
│  ─────────────────────────────────  │
│  + policy: OperatorMemoryPolicy     │
│  ─────────────────────────────────  │
│  + ExecuteInternal()                │
│  + ExecuteRegularNLJ()              │
│  + ExecuteBlockedNLJ()              │
│  + ProcessBlockedJoin()             │
│  + ComputeNextRHSBlock()            │
└─────────┬───────────────────────────┘
          │
          │ uses
          ▼
┌─────────────────────────────────────┐
│    OperatorMemoryPolicy             │
│  ─────────────────────────────────  │
│  + GetNestedLoopJoinConfig()        │
│  + GetBlockwiseNLJConfig()          │
└─────────┬───────────────────────────┘
          │
          │ returns
          ▼
┌─────────────────────────────────────┐
│       NLJPolicyConfig               │
│  ─────────────────────────────────  │
│  + memory_budget                    │
│  + input_buffer_ratio (0.5)         │
│  + short_table_buffer_ratio (0.5)   │
│  ─────────────────────────────────  │
│  + GetInputBufferBudget()           │
│  + GetShortTableBufferBudget()      │ (RHS)
│  + GetLongTableBufferBudget()       │ (LHS)
│  + GetOutputBufferBudget()          │
└─────────────────────────────────────┘
```

---

## Operator Memory Policy

### Design Philosophy

The **OperatorMemoryPolicy** provides a **flexible, extensible framework** for configuring memory-aware operators without hardcoding budget allocation logic.

### Budget Allocation Strategy

Given a total `memory_budget`, the policy allocates:

```
Total Memory Budget (100%)
    │
    ├─── Input Buffer (50% by default, configurable)
    │    │
    │    ├─── Short Table / RHS Buffer (50% of input = 25% total)
    │    │
    │    └─── Long Table / LHS Buffer (50% of input = 25% total)
    │
    └─── Output Buffer (50% by default, configurable)
         │
         └─── Result Buffer (for join output)
```

### Configuration Example

```cpp
struct NLJPolicyConfig {
    idx_t memory_budget = 100 * 1024 * 1024;  // 100 MB
    double input_buffer_ratio = 0.5;           // 50% for input
    double short_table_buffer_ratio = 0.5;     // 50% of input for RHS
    
    // Derived budgets:
    // RHS budget: 100MB × 0.5 × 0.5 = 25 MB
    // LHS budget: 100MB × 0.5 × 0.5 = 25 MB
    // Output budget: 100MB × 0.5 = 50 MB
};
```

### Policy Interface

```cpp
class OperatorMemoryPolicy {
public:
    // For regular NLJ (entire RHS in memory)
    virtual NLJPolicyConfig GetNestedLoopJoinConfig() const;
    
    // For blocked NLJ (RHS processed in blocks)
    virtual NLJPolicyConfig GetBlockwiseNLJConfig() const;
    
    // For other join algorithms
    virtual SortMergePolicyConfig GetSortMergeConfig() const;
};
```

### Budget Computation Methods

```cpp
// Input buffer total (LHS + RHS)
idx_t GetInputBufferBudget() const {
    return floor(memory_budget * input_buffer_ratio);
}

// RHS buffer (short table)
idx_t GetShortTableBufferBudget() const {
    return floor(memory_budget * input_buffer_ratio * short_table_buffer_ratio);
}

// LHS buffer (long table)
idx_t GetLongTableBufferBudget() const {
    idx_t input = GetInputBufferBudget();
    idx_t short = GetShortTableBufferBudget();
    return input - short;
}

// Output buffer (join results)
idx_t GetOutputBufferBudget() const {
    return floor(memory_budget * (1 - input_buffer_ratio));
}
```

---

## Execution Workflow

### High-Level Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    Sink Phase (Build)                        │
│                                                              │
│  1. Materialize entire RHS into buffer-managed               │
│     ColumnDataCollection (right_payload_data)                │
│  2. Evaluate and store RHS join conditions                   │
│     (right_condition_data)                                   │
│  3. Track total RHS size (rhs_total_bytes)                   │
│  4. Decision: use_blocked_mode = (rhs_total_bytes >          │
│                                   rhs_budget_bytes)          │
└───────────────────────┬──────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────┐
│               ExecuteInternal (Probe Phase)                  │
│                                                              │
│  if (!use_blocked_mode) {                                    │
│      return ExecuteRegularNLJ()  // Standard NLJ logic       │
│  } else {                                                    │
│      return ExecuteBlockedNLJ()  // Blocked execution        │
│  }                                                           │
└───────────────────────┬──────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────┐
│              Blocked NLJ State Machine                       │
│                                                              │
│  Phase 1: ACCUMULATE_LHS                                     │
│    ├─ Buffer incoming LHS chunks                             │
│    ├─ Evaluate and store LHS conditions                      │
│    ├─ Check: lhs_buffer_bytes > lhs_budget_bytes?            │
│    └─ Transition: → PROCESS_BLOCKS                           │
│                                                              │
│  Phase 2: PROCESS_BLOCKS                                     │
│    ├─ Compute next RHS block boundaries                      │
│    ├─ Pin LHS buffer blocks (lhs_payload_handles)            │
│    ├─ Pin RHS block (rhs_payload_handles)                    │
│    ├─ Nested loop: LHS buffer × current RHS block            │
│    ├─ Buffer results in result_buffer                        │
│    ├─ If result buffer full: drain results                   │
│    ├─ Unpin blocks when done                                 │
│    ├─ Check: more RHS blocks?                                │
│    │   YES → Next RHS block, reset LHS scan                  │
│    │   NO  → Transition: → DRAIN_FINAL_RESULTS               │
│    └─ Check: pending_input (more LHS)?                       │
│        YES → Transition: → ACCUMULATE_LHS                    │
│                                                              │
│  Phase 3: DRAIN_FINAL_RESULTS                                │
│    ├─ Flush remaining results from result_buffer             │
│    └─ Transition: → FINISHED                                 │
│                                                              │
│  Phase 4: FINISHED                                           │
│    └─ Return FINISHED                                        │
└──────────────────────────────────────────────────────────────┘
```

### Detailed Phase Transitions

```
START
  │
  ▼
┌──────────────────┐
│ ACCUMULATE_LHS   │◄──────────┐
│                  │           │
│ Buffer LHS until │           │
│ budget reached   │           │
└────────┬─────────┘           │
         │                     │
         │ Budget full or      │ More LHS input
         │ input exhausted     │ (pending_input)
         ▼                     │
┌──────────────────┐           │
│ PROCESS_BLOCKS   │───────────┘
│                  │
│ For each RHS     │
│ block:           │
│  - Pin blocks    │
│  - Join LHS×RHS  │
│  - Buffer results│
│  - Drain if full │
│  - Unpin blocks  │
└────────┬─────────┘
         │
         │ All RHS blocks done,
         │ no more LHS input
         ▼
┌──────────────────┐
│DRAIN_FINAL_RESULT│
│                  │
│ Flush remaining  │
│ results          │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│    FINISHED      │
└──────────────────┘
```

---

## Memory Management

### Memory Budget Hierarchy

```
┌────────────────────────────────────────────────────────────┐
│           DuckDB Buffer Manager (Global)                   │
│                                                            │
│  Manages all database memory, handles evictions            │
└──────────────────────┬─────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────┐
│        Operator Memory Policy (Per-Operator)               │
│                                                            │
│  Allocates operator budget into component budgets          │
└──────────────────────┬─────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┬─────────────────┐
        ▼              ▼              ▼                 ▼
  ┌─────────┐    ┌─────────┐    ┌─────────┐     ┌──────────┐
  │   RHS   │    │   LHS   │    │ Result  │     │ Overhead │
  │ Buffer  │    │ Buffer  │    │ Buffer  │     │  (scan   │
  │ (25 MB) │    │ (25 MB) │    │ (50 MB) │     │  states) │
  └─────────┘    └─────────┘    └─────────┘     └──────────┘
```

### ColumnDataCollection Memory Model

DuckDB's `ColumnDataCollection` is **buffer-managed**, meaning:

1. **Automatic buffer management**: Data automatically moves between memory and disk
2. **Transparent paging**: No explicit load/unload needed for normal operations
3. **Block-level granularity**: Data organized in blocks (typically 256 KB)
4. **LRU eviction**: Least recently used blocks evicted under memory pressure

### Memory Pressure Scenarios

#### Scenario 1: RHS Fits in Memory (Regular Mode)
```
Memory Budget: 100 MB
RHS Size: 20 MB
LHS: Streaming

┌──────────────────────────┐
│     Buffer Manager       │
│  ┌────────────────────┐  │
│  │  RHS (20 MB)       │  │ ← Fully resident
│  │  [................]│  │
│  └────────────────────┘  │
│                          │
│  Available: 80 MB        │
└──────────────────────────┘

Strategy: Use regular NLJ (fast, no blocking needed)
```

#### Scenario 2: RHS Exceeds Budget (Blocked Mode)
```
Memory Budget: 100 MB (50 MB input, 50 MB output)
RHS Size: 150 MB
RHS Budget: 25 MB (50% of input budget)
LHS Budget: 25 MB (50% of input budget)

┌──────────────────────────┐
│     Buffer Manager       │
│                          │
│  RHS Block 1 (25 MB)     │ ← Pinned during processing
│  [....pinned....]        │
│                          │
│  LHS Buffer (25 MB)      │ ← Pinned during processing
│  [....pinned....]        │
│                          │
│  Result Buffer (50 MB)   │ ← Accumulates join output
│  [..accumulating..]      │
└──────────────────────────┘

┌──────────────────────────┐
│  Temporary Storage       │
│  (Disk/Swap)             │
│                          │
│  RHS Block 2 (25 MB)     │ ← Swapped out
│  RHS Block 3 (25 MB)     │
│  RHS Block 4 (25 MB)     │
│  ...                     │
│  RHS Block 6 (25 MB)     │
└──────────────────────────┘

Strategy: Process RHS in 6 blocks, each 25 MB
Total passes: 6 (for each buffered LHS batch)
```

#### Scenario 3: Output Overflow
```
During processing, result buffer fills up:

┌──────────────────────────┐
│  Result Buffer (FULL)    │
│  [████████████████████]  │ ← 50 MB reached
└──────────────────────────┘
         │
         │ NEED_MORE_OUTPUT
         ▼
┌──────────────────────────┐
│  Drain Results           │
│  - Scan result_buffer    │
│  - Return chunks to      │
│    pipeline              │
│  - Mark draining_results │
└──────────────────────────┘
         │
         ▼
     Resume join processing when buffer space available
```

---

## Block Pinning Strategy

### Why Pinning?

When processing LHS buffer × RHS block:
1. **Data must stay resident** in memory for the entire nested loop
2. **Prevent eviction** by buffer manager's LRU policy
3. **Avoid thrashing** - repeated evict/reload cycles

### Pinning Mechanism

```cpp
// Pin all blocks in a ColumnDataCollection range
void PinBlocks(ColumnDataCollection &collection,
               idx_t start_row, idx_t end_row,
               unordered_map<uint32_t, BufferHandle> &handles) {
    
    // Identify chunk indices covering [start_row, end_row)
    idx_t start_chunk = start_row / STANDARD_VECTOR_SIZE;
    idx_t end_chunk = (end_row + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
    
    // Pin each chunk's segments
    for (idx_t chunk_idx = start_chunk; chunk_idx < end_chunk; ++chunk_idx) {
        auto segment = collection.GetSegment(chunk_idx);
        for (auto &block_info : segment->GetBlockInfo()) {
            // Pin block and store handle
            handles[block_info.block_id] = block_info.handle.Pin();
        }
    }
}
```

### Pinning Lifecycle

```
┌───────────────────────────────────────────────────────────┐
│          Processing LHS Buffer × RHS Block 1              │
└───────────────────────────────────────────────────────────┘

STEP 1: Pin LHS Buffer
┌───────────────────────────────────────────────────────────┐
│  lhs_payload_handles[block_id_1] = Pin()                  │
│  lhs_payload_handles[block_id_2] = Pin()                  │
│  lhs_condition_handles[block_id_1] = Pin()                │
│  lhs_condition_handles[block_id_2] = Pin()                │
│                                                           │
│  Status: All LHS blocks PINNED ✓                          │
└───────────────────────────────────────────────────────────┘

STEP 2: Pin RHS Block 1
┌───────────────────────────────────────────────────────────┐
│  rhs_payload_handles[block_id_10] = Pin()                 │
│  rhs_payload_handles[block_id_11] = Pin()                 │
│  rhs_condition_handles[block_id_10] = Pin()               │
│  rhs_condition_handles[block_id_11] = Pin()               │
│                                                           │
│  Status: RHS Block 1 blocks PINNED ✓                      │
└───────────────────────────────────────────────────────────┘

STEP 3: Process Join (Nested Loop)
┌───────────────────────────────────────────────────────────┐
│  for each LHS tuple in lhs_payload_buffer:                │
│      for each RHS tuple in RHS Block 1:                   │
│          if join_condition matches:                       │
│              append result to result_buffer               │
│                                                           │
│  Status: Data stays resident, NO EVICTIONS ✓              │
└───────────────────────────────────────────────────────────┘

STEP 4: Unpin RHS Block 1
┌───────────────────────────────────────────────────────────┐
│  rhs_payload_handles.clear()  // Unpins all               │
│  rhs_condition_handles.clear()                            │
│                                                           │
│  Status: RHS Block 1 can now be evicted                   │
└───────────────────────────────────────────────────────────┘

STEP 5: Move to RHS Block 2
┌───────────────────────────────────────────────────────────┐
│  (LHS still pinned)                                       │
│  Pin RHS Block 2...                                       │
│  Process join...                                          │
│  Unpin RHS Block 2...                                     │
└───────────────────────────────────────────────────────────┘

STEP 6: After All RHS Blocks Processed
┌───────────────────────────────────────────────────────────┐
│  lhs_payload_handles.clear()  // Unpin LHS                │
│  lhs_condition_handles.clear()                            │
│                                                           │
│  Status: All blocks unpinned, ready for next LHS batch    │
└───────────────────────────────────────────────────────────┘
```

### Pin Handle Storage

```cpp
class BlockedNLJOperatorState {
    // Store handles by block ID to prevent duplicates
    unordered_map<uint32_t, BufferHandle> lhs_payload_handles;
    unordered_map<uint32_t, BufferHandle> lhs_condition_handles;
    unordered_map<uint32_t, BufferHandle> rhs_payload_handles;
    unordered_map<uint32_t, BufferHandle> rhs_condition_handles;
};
```

**Key Design Decision**: Use `unordered_map` keyed by block ID to:
- Avoid duplicate pins of the same block
- Ensure cleanup unpins all blocks exactly once
- O(1) lookup for pin operations

---

## Result Buffering

### Why Buffer Results?

Without result buffering:
```
┌───────────────────────────────────────────────────────────┐
│  Problem: Synchronous Output                              │
│                                                           │
│  for each LHS tuple:                                      │
│      for each RHS tuple:                                  │
│          if match:                                        │
│              result_chunk.Append(...)                     │
│              if result_chunk.IsFull():                    │
│                  return NEED_MORE_OUTPUT ← BLOCKS!        │
│                                                           │
│  Result: Frequent blocking, poor cache locality           │
└───────────────────────────────────────────────────────────┘
```

With result buffering:
```
┌───────────────────────────────────────────────────────────┐
│  Solution: Buffered Output                                │
│                                                           │
│  for each LHS tuple:                                      │
│      for each RHS tuple:                                  │
│          if match:                                        │
│              result_build_chunk.Append(...)               │
│              if result_build_chunk.IsFull():              │
│                  result_buffer.Append(result_build_chunk) │
│                  result_buffer_bytes += EstimateSize(...) │
│                  if result_buffer_bytes > budget:         │
│                      return NEED_MORE_OUTPUT              │
│                                                           │
│  Result: Fewer interruptions, better throughput           │
└───────────────────────────────────────────────────────────┘
```

### Result Buffer Lifecycle

```
┌──────────────────────────────────────────────────────────┐
│  Phase: PROCESS_BLOCKS                                   │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  While processing LHS × RHS:                             │
│  ┌────────────────────────────────────────────────┐      │
│  │ result_build_chunk ← join matches              │      │
│  │ result_buffer.Append(result_build_chunk)       │      │
│  │ result_buffer_bytes += EstimateSize(...)       │      │
│  └────────────────────────────────────────────────┘      │
│                                                          │
│  If result_buffer_bytes > result_buffer_budget:          │
│  ┌────────────────────────────────────────────────┐      │
│  │ draining_results = true                        │      │
│  │ return NEED_MORE_OUTPUT                        │      │
│  └────────────────────────────────────────────────┘      │
│                     │                                    │
│                     ▼                                    │
│  ┌──────────────────────────────────────────────────┐    │
│  │ Next ExecuteInternal() call:                     │    │
│  │   if draining_results:                           │    │
│  │     Scan from result_buffer                      │    │
│  │     Return chunk to pipeline                     │    │
│  │     result_buffer_bytes -= returned_bytes        │    │
│  │     if result_buffer empty:                      │    │
│  │       draining_results = false                   │    │
│  │       resume join processing                     │    │
│  └──────────────────────────────────────────────────┘    │
│                                                          │
└──────────────────────────────────────────────────────────┘
```


### Memory Budget Impact

```
Scenario: 100 MB total budget, 50 MB output budget

WITHOUT result buffering:
├─ Output chunk: 1.2 MB (STANDARD_VECTOR_SIZE)
├─ Frequency: ~42 returns per 50 MB of results
└─ Overhead: High context switching, poor pipeline utilization

WITH result buffering:
├─ Buffer size: 50 MB
├─ Frequency: 1 return per 50 MB of results
└─ Benefit: ~42x fewer interruptions, better cache locality
```

---

## Implementation Details

### Key Files

| File | Purpose |
|------|---------|
| [`physical_nested_loop_join_optimized.hpp`](../../../include/duckdb/execution/operator/join/physical_nested_loop_join_optimized.hpp) | Operator class definition |
| [`physical_nested_loop_join_optimized.cpp`](physical_nested_loop_join_optimized.cpp) | Main implementation |
| [`operator_memory_policy.hpp`](../../../include/duckdb/execution/operator_memory_policy.hpp) | Memory policy framework |
| [`blocked_nlj_state.cpp`](blocked_nlj_state.cpp) | State management utilities |

### State Classes

#### BlockedNLJGlobalState
```cpp
class BlockedNLJGlobalState : public GlobalSinkState {
    // RHS materialized data (buffer-managed)
    ColumnDataCollection right_payload_data;
    ColumnDataCollection right_condition_data;
    
    // Mode decision
    bool use_blocked_mode;
    
    // Budgets from policy
    idx_t rhs_budget_bytes;  // Short table budget
    idx_t lhs_budget_bytes;  // Long table budget
    idx_t rhs_total_bytes;   // Actual RHS size
};
```

#### BlockedNLJOperatorState
```cpp
class BlockedNLJOperatorState : public CachingOperatorState {
    // Execution phase
    enum class Phase {
        ACCUMULATE_LHS,
        PROCESS_BLOCKS,
        DRAIN_FINAL_RESULTS,
        FINISHED
    };
    Phase current_phase;
    
    // LHS buffer (buffer-managed)
    ColumnDataCollection lhs_payload_buffer;
    ColumnDataCollection lhs_condition_buffer;
    idx_t lhs_buffer_bytes;
    
    // Block boundaries
    idx_t rhs_block_start_row;
    idx_t rhs_block_end_row;
    
    // Pin handles
    unordered_map<uint32_t, BufferHandle> lhs_payload_handles;
    unordered_map<uint32_t, BufferHandle> lhs_condition_handles;
    unordered_map<uint32_t, BufferHandle> rhs_payload_handles;
    unordered_map<uint32_t, BufferHandle> rhs_condition_handles;
    
    // Result buffer
    ColumnDataCollection result_buffer;
    idx_t result_buffer_bytes;
    bool draining_results;
};
```

### Core Algorithms

#### ComputeNextRHSBlock()
```cpp
bool ComputeNextRHSBlock(BlockedNLJGlobalState &gstate,
                         BlockedNLJOperatorState &state) const {
    // If we've processed all RHS rows, we're done
    if (state.rhs_block_start_row >= gstate.right_payload_data.Count()) {
        return false;
    }
    
    // Scan RHS to determine block boundary
    idx_t accumulated_bytes = 0;
    idx_t current_row = state.rhs_block_start_row;
    ColumnDataScanState scan;
    gstate.right_payload_data.InitializeScan(scan);
    gstate.right_payload_data.Seek(scan, current_row);
    
    DataChunk rhs_chunk;
    while (accumulated_bytes < state.rhs_budget_bytes &&
           current_row < gstate.right_payload_data.Count()) {
        gstate.right_payload_data.Scan(scan, rhs_chunk);
        accumulated_bytes += EstimateChunkSize(rhs_chunk);
        current_row += rhs_chunk.size();
    }
    
    state.rhs_block_end_row = current_row;
    return true;
}
```

#### ProcessBlockedJoin()
```cpp
OperatorResultType ProcessBlockedJoin(ExecutionContext &context,
                                      DataChunk &input,
                                      DataChunk &chunk,
                                      BlockedNLJGlobalState &gstate,
                                      BlockedNLJOperatorState &state) const {
    
    // Check if draining buffered results
    if (state.draining_results) {
        if (DrainResultBuffer(chunk, state)) {
            return OperatorResultType::NEED_MORE_OUTPUT;
        }
        state.draining_results = false;
    }
    
    // Pin LHS buffer blocks (if not already pinned)
    if (state.lhs_payload_handles.empty()) {
        PinLHSBlocks(state);
    }
    
    // Pin RHS block (if not already pinned)
    if (state.rhs_payload_handles.empty()) {
        PinRHSBlock(gstate, state);
    }
    
    // Nested loop: LHS buffer × RHS block
    while (true) {
        // Scan LHS buffer
        if (!state.lhs_payload_buffer.Scan(state.lhs_payload_scan,
                                            state.lhs_payload_chunk)) {
            // LHS buffer exhausted, move to next RHS block
            UnpinRHSBlock(state);
            
            if (ComputeNextRHSBlock(gstate, state)) {
                // Reset LHS scan for next RHS block
                state.lhs_payload_buffer.InitializeScan(state.lhs_payload_scan);
                PinRHSBlock(gstate, state);
                continue;
            } else {
                // All RHS blocks done
                UnpinLHSBlocks(state);
                return OperatorResultType::FINISHED;
            }
        }
        
        // Scan RHS block
        while (gstate.right_payload_data.Scan(state.rhs_payload_scan,
                                               state.rhs_payload_chunk,
                                               state.rhs_block_start_row,
                                               state.rhs_block_end_row)) {
            // Evaluate join condition and build results
            NestedLoopJoin::Perform(state.lhs_payload_chunk,
                                   state.lhs_condition_chunk,
                                   state.rhs_payload_chunk,
                                   state.rhs_condition_chunk,
                                   join_type,
                                   state.result_build_chunk);
            
            // Buffer results
            if (state.result_build_chunk.size() > 0) {
                state.result_buffer.Append(state.result_build_chunk);
                state.result_buffer_bytes += EstimateChunkSize(state.result_build_chunk);
                
                // Check if result buffer is full
                if (state.result_buffer_bytes > state.result_buffer_budget) {
                    state.draining_results = true;
                    if (DrainResultBuffer(chunk, state)) {
                        return OperatorResultType::NEED_MORE_OUTPUT;
                    }
                    state.draining_results = false;
                }
            }
        }
        
        // Reset RHS scan for next LHS chunk
        state.rhs_payload_scan.Reset();
    }
}
```

### Testing

#### Test Coverage

| Test File | Purpose |
|-----------|---------|
| [`test_blocked_nlj_optimized.test`](../../../../test/sql/join/test_blocked_nlj_optimized.test) | Functional tests for blocked NLJ |
| [`test_buffer_manager_profiling.test`](../../../../test/sql/storage/test_buffer_manager_profiling.test) | Buffer profiling tests |

#### Example Test Case
```sql
-- Test blocked execution with memory budget
SET operator_memory_budget='10MB';

-- Large join that triggers blocked mode
CREATE TABLE lhs AS SELECT range AS a FROM range(100000);
CREATE TABLE rhs AS SELECT range AS b FROM range(100000);

-- IS DISTINCT FROM forces NLJ
SELECT COUNT(*) FROM lhs 
JOIN rhs ON lhs.a IS DISTINCT FROM rhs.b;

-- Expected: Query succeeds without OOM, uses blocked execution
```

---

## Blockwise NLJ Optimized

### Overview

DuckDB has two nested loop join variants:
- **PhysicalNestedLoopJoin**: Uses decomposed `JoinCondition` with separate LHS/RHS expressions
- **PhysicalBlockwiseNLJoin**: Uses a single arbitrary `Expression` evaluated on cross-product

Both have optimized variants that implement the blocked execution pattern described in this document.

### Architecture Comparison

```
NLJ Optimized:
┌─────────────────────────────────────────────────────────────────┐
│  Sink: Materialize RHS payload + RHS conditions (separate)      │
│  Execute: Compare conditions → NestedLoopJoinInner::Perform()   │
│  Match: Selection vectors from condition comparison             │
└─────────────────────────────────────────────────────────────────┘

Blockwise NLJ Optimized:
┌─────────────────────────────────────────────────────────────────┐
│  Sink: Materialize RHS payload only (no condition extraction)   │
│  Execute: CrossProduct → SelectExpression (filter)              │
│  Match: Evaluate full expression on cross-product intermediate  │
└─────────────────────────────────────────────────────────────────┘
```

### What's Shared (Reused)

| Component | Description |
|-----------|-------------|
| Phase State Machine | `ACCUMULATE_LHS → PROCESS_BLOCKS → DRAIN_FINAL_RESULTS → FINISHED` |
| LHS Buffering | `ColumnDataCollection` for LHS payload storage |
| Result Buffering | Accumulate join output, drain when full |
| Memory Budget Model | `NLJPolicyConfig` with input/output ratios |
| Block Pinning | Pin active LHS/RHS blocks during processing |
| RHS Block Computation | Chunk-based block boundary calculation |
| Helper Methods | `EstimateChunkSize`, `TryAddLHSChunk`, `DrainResultBuffer` |

### Key Differences

| Aspect | NLJ Optimized | Blockwise NLJ Optimized |
|--------|---------------|-------------------------|
| **Condition Type** | `vector<JoinCondition>` | `unique_ptr<Expression>` |
| **Condition Storage** | Separate `right_condition_data` | None (evaluated on-the-fly) |
| **LHS Condition Buffer** | Required (`lhs_condition_buffer`) | Not needed |
| **Matching Logic** | `NestedLoopJoinInner::Perform()` | `CrossProductExecutor + SelectExpression` |
| **Intermediate Chunk** | Not needed | Required for cross-product output |

### Blockwise NLJ Blocked Mode Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  Phase 2: PROCESS_BLOCKS (Blockwise NLJ specific)               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  For each RHS block:                                             │
│    Pin RHS block                                                 │
│    For each LHS chunk in buffer:                                 │
│      For each (lhs_row, rhs_row) in batches:                     │
│        ┌───────────────────────────────────────────────────┐     │
│        │ 1. Build cross-product batch → intermediate_chunk │     │
│        │ 2. executor.SelectExpression(intermediate_chunk)  │     │
│        │ 3. Slice matches → result_build_chunk             │     │
│        │ 4. Buffer results (drain if full)                 │     │
│        └───────────────────────────────────────────────────┘     │
│    Unpin RHS block                                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Implementation Files

| File | Purpose |
|------|---------|
| [`physical_blockwise_nl_join_optimized.hpp`](../../../include/duckdb/execution/operator/join/physical_blockwise_nl_join_optimized.hpp) | Blockwise NLJ optimized class |
| [`physical_blockwise_nl_join_optimized.cpp`](physical_blockwise_nl_join_optimized.cpp) | Blockwise NLJ optimized implementation |


---

## Performance Considerations

### When to Use Blocked NLJ

✅ **Good fit:**
- Joins with `IS DISTINCT FROM` (forces NLJ)
- Small-medium RHS that doesn't fit in memory
- Queries where other join algorithms aren't applicable
- Memory-constrained environments

❌ **Poor fit:**
- Very large RHS (hash join better)
- Equi-joins (hash join better)
- Highly selective joins (index lookup better)

### Optimization Opportunities

1. **Adaptive block sizing**: Dynamically adjust RHS block size based on available memory
2. **Parallel execution**: Multiple threads process different RHS blocks
3. **Bloom filters**: Skip RHS blocks that can't match current LHS batch
4. **Compressed blocks**: Store blocks in compressed format to increase effective budget
5. **Statistics-guided ordering**: Process RHS blocks in selectivity order

---

## Related Documentation

- [Buffer Manager Profiling](../../../storage/buffer/BUFFER_PROFILING.md)
- [Operator Memory Policy](../../../include/duckdb/execution/operator_memory_policy.hpp)
- [ColumnDataCollection](../../../include/duckdb/common/types/column/column_data_collection.hpp)

---

## Future Work

### Planned Enhancements

1. **Policy Configuration**
   - Add `PRAGMA` settings for memory policy parameters
   - Per-query memory budget hints

2. **Profiling Integration**
   - Expose blocked NLJ metrics (blocks processed, pins, buffer sizes)
   - Integration with query profiler output

3. **Algorithm Selection**
   - Automatic fallback to hash join when appropriate
   - Cost-based decision for blocked vs. regular mode

4. **Optimization**
   - Vectorized condition evaluation
   - SIMD-accelerated join matching
   - Prefetching for RHS blocks

---

## Authors & Contributors

This design and implementation were developed as part of ongoing research into memory-efficient query processing in DuckDB.

**Key Design Principles:**
- Respect buffer manager abstraction
- Minimize code duplication
- Maintain compatibility with existing NLJ logic
- Extensible policy framework for future join algorithms

**Version History:**
- v1.0 : Initial blocked NLJ implementation
- v1.1 : Added result buffering
- v1.2 : Buffer manager profiling integration
