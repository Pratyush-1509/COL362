# COL362 – Database Management Systems

## Assignment 3: Out-of-Core Query Execution

Implements a pull-based volcano-model query executor in Rust that operates under a **64 MB virtual address space limit** (`RLIMIT_AS`). All operators spill to disk rather than accumulating data in memory.

---

### Operator Overview

#### `ScanOperator` (`executor/scan.rs`)
Reads a heap file one block at a time from the disk simulator. Schema is loaded from `DbContext`.

#### `FilterOperator` / `OwnedFilterOperator` (`executor/filter.rs`)
Row-at-a-time predicate evaluation. `OwnedFilterOperator` owns its predicate list (used during join-tree construction when lifetime of borrowed predicates is insufficient).

#### `ProjectOperator` (`executor/project.rs`)
Selects and optionally renames a subset of columns. Source indices are computed once at construction; `next()` is a single index-mapped clone.

#### `SortOperator` (`executor/sort.rs`)
External merge sort.
- **Phase 1 (run generation):** Accumulates rows up to `RUN_BUDGET_BYTES = 2 MB`, sorts in-place, serialises to anonymous disk blocks.
- **Phase 2 (k-way merge):** Opens one `RunReader` per run; `ExternalMerge::next_row()` does a linear-scan minimum each call (correct for any k).
- If all rows fit in one budget window, no disk I/O happens (in-memory path).

#### `CrossOperator` (`executor/cross.rs`)
Cartesian product via right-side spill.
- Spills the right child to disk in **256 KB batched writes** (`SPILL_BATCH_BLOCKS = 64` blocks per flush) — peak memory is O(batch) regardless of right-side size. The old implementation built a `Vec<Vec<u8>>` then flattened it, doubling peak memory.
- Streams the left child one row at a time, rewinding the right-side disk region for each left row.

#### `HashJoinOperator` (`executor/hashjoin.rs`)
Two-phase hash join with automatic grace-hash-join fallback.

**In-memory fast path** (right side ≤ `INMEM_BUILD_BUDGET = 4 MB`):
- Build a `HashMap<JoinKey, Vec<Row>>` from the right child.
- Stream the left child through it, emitting matches.

**Grace hash join fallback** (right side > 4 MB):
- Re-partitions already-buffered right rows plus remaining right rows into `NUM_PARTITIONS = 64` disk partitions (`FLUSH_THRESHOLD = 256 KB` per partition buffer).
- Partitions the left child into the same 64 buckets.
- Processes one partition pair at a time: load right partition into a fresh hash table, stream left partition through it.

Memory budget reasoning under 64 MB `RLIMIT_AS`:
- ~25 MB base overhead (Rust debug binary + libc/libm/ld-linux shared libraries)
- `INMEM_BUILD_BUDGET` 4 MB × 2.5× HashMap overhead = 10 MB heap
- 25 + 10 = **35 MB peak** during in-memory build — safe margin
- During grace partition phase: 64 × 256 KB = 16 MB partition buffers (sequential to hash table — hash table is dropped before probing)

---

### Join Planning (`executor/mod.rs`)

When a `Filter` node sits above a multi-table `Cross` tree, `build_operator` detects this pattern and replaces the naive cross-product + filter with a **hash-join tree**:

1. **`collect_join_parts`** – recursively decomposes a `Filter(Cross(...))` subtree into a flat list of leaf scan ops and all predicates.
2. **`scan_op_blocks`** – estimates table size for join ordering:
   - Reads `CardinalityData` statistics from `db_config.json` (max distinct-value count across all columns ≈ row count for fact tables).
   - Converts to a synthetic block count via `(max_card × 128 bytes/row) / 4096 bytes/block`.
   - Falls back to `get_file_num_blocks` when no stats are available.
3. **Greedy join ordering** in `build_join_tree`:
   - Start with the **largest table** (highest block estimate) as the first (probe/left) side — keeps the biggest table out of the build side.
   - Greedily extend by choosing the next table that maximises `(equi_join_key_count, table_size)` — avoids cross products when an equi-join is available, and prefers larger tables on the probe side as a tie-breaker.
4. For each join step: use `HashJoinOperator` if equi-join keys exist, else `CrossOperator`.
5. Apply residual predicates (non-equi or same-column filters) as a `FilterOperator` immediately after each join step.

This ordering ensures queries like `supplier ⋈ part ⋈ partsupp` (where supplier and part share no direct predicate) never produce a cross product — `partsupp` is placed first, then both dimension tables join against it.

---

### Memory Constants Summary

| Constant | Value | Location |
|---|---|---|
| `RUN_BUDGET_BYTES` | 2 MB | `sort.rs` |
| `INMEM_BUILD_BUDGET` | 4 MB | `hashjoin.rs` |
| `NUM_PARTITIONS` | 64 | `hashjoin.rs` |
| `FLUSH_THRESHOLD` | 256 KB | `hashjoin.rs` |
| `READ_BATCH` | 16 blocks | `hashjoin.rs` |
| `SPILL_BATCH_BLOCKS` | 64 blocks (256 KB) | `cross.rs` |

---

### Running Tests

```bash
# Build all binaries
cargo build --release

# Generate expected outputs from SQLite + write monitor_config.json
cargo run --release --bin tests_gen -- -c scratch/compiled_datasets/tpch -r scratch/runtimes/tpch

# Execute all queries through the database binary and verify correctness
cargo run --release --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

Tests 1–50 are correctness checks (exact output match against SQLite). Tests 51–60 are TPC-H benchmark queries (query_1 through query_10) that must complete within the time and memory limits.
