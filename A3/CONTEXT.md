# COL362 A3 — Out-of-Core Query Execution (Context for New Sessions)

## Goal
Minimise disk I/O (block reads + writes) for TPC-H queries on a simulated disk under 64 MB `RLIMIT_AS`. Peers achieve ~1% of the naive read/write count.

## Project Layout
```
/home/aprat/COL362/A3/
├── database/src/          ← ONLY directory we modify
│   ├── main.rs            ← entry point, calls build_operator + streams output
│   ├── disk.rs            ← DiskManager: read_blocks / write_blocks / alloc_anon_blocks
│   ├── row.rs             ← Row/Schema types, encode_row, decode_block, rows_to_blocks, format_row
│   ├── io_setup.rs        ← wires up FD3/FD4 to disk sim
│   └── executor/
│       ├── mod.rs         ← build_operator tree, join reordering, sort_already_satisfied
│       ├── scan.rs        ← ScanOperator (batched 64-block reads)
│       ├── filter.rs      ← FilterOperator + OwnedFilterOperator
│       ├── project.rs     ← ProjectOperator (column select/rename by name)
│       ├── sort.rs        ← SortOperator (in-memory OR external k-way merge)
│       ├── hashjoin.rs    ← HashJoinOperator (in-memory OR grace/partitioned)
│       └── cross.rs       ← CrossOperator (right side cached in memory or spilled)
├── common/src/            ← Data enum, DataType, QueryOp AST — READ ONLY
├── configs/db_config/     ← DbContext, ColumnStat, IsPhysicallyOrdered — READ ONLY
├── disk/src/              ← Disk simulator binary — DO NOT MODIFY
├── monitor/src/           ← Test runner — DO NOT MODIFY
└── scratch/runtimes/tpch/ ← db_config.json, disk_sim_config.json, expected_output_*.csv
```

## Disk Protocol (via DiskManager)
- `get block-size` → block_size (4096 bytes)
- `get anon-start-block` → first writable block ID
- `get block X N` → read N×block_size raw bytes starting at block X
- `put block X N` + N×block_size bytes → write (NO response)
- `get file start-block <id>` / `get file num-blocks <id>` → file metadata
- Block layout: rows packed from byte 0, u16-LE row count at last 2 bytes

## Local TPC-H Schema (db_config.json)
```
part      (file_id=part):     p_partkey(Int64)*, p_name(Str), p_mfgr(Str), p_brand(Str),
                               p_type(Str), p_size(Int32), p_container(Str),
                               p_retailprice(F64), p_comment(Str)         [max_card≈20K]

partsupp  (file_id=partsupp): ps_partkey(Int64)*, ps_suppkey(Int64),
                               ps_availqty(Int32), ps_supplycost(F64), ps_comment(Str)  [≈82K]

customer  (file_id=customer): c_custkey(Int64)*, c_name(Str)*, c_address(Str),
                               c_nationkey(Int32), c_phone(Str), c_acctbal(F64),
                               c_mktsegment(Str), c_comment(Str)          [≈15K]

supplier  (file_id=supplier): s_suppkey(Int64)*, s_name(Str)*,
                               s_address(Str), s_nationkey(Int32), s_phone(Str),
                               s_acctbal(F64), s_comment(Str)             [≈1K]

orders    (file_id=orders):   o_orderkey(Int64)*, o_custkey(Int64), o_orderstatus(Str),
                               o_totalprice(F64), o_orderdate(Str), o_orderpriority(Str),
                               o_clerk(Str), o_shippriority(Int32)*, o_comment(Str) [≈151K]

lineitem  (file_id=lineitem): l_orderkey(Int64)*, l_partkey(Int64), l_suppkey(Int64),
                               l_linenumber(Int32), l_quantity(F64), l_extendedprice(F64),
                               l_discount(F64), l_tax(F64), l_returnflag(Str),
                               l_linestatus(Str), l_shipdate(Str), l_commitdate(Str),
                               l_receiptdate(Str), l_shipinstruct(Str), l_shipmode(Str),
                               l_comment(Str)                              [≈536K]

nation    (file_id=nation):   n_nationkey(Int32)*, n_name(Str), n_regionkey(Int32),
                               n_comment(Str)                              [25 rows]

region    (file_id=region):   r_regionkey(Int32)*, r_name(Str)*, r_comment(Str) [5 rows]
```
`*` = IsPhysicallyOrdered in local config (data is in ascending order by that column)
**IMPORTANT**: btest (hidden stress test) db_config likely has different types/stats.
btest1 expects `l_quantity` as integer (27 not 27.0) → btest lineitem has Int32 quantity.

## Query AST Structure (common/src/query.rs)
```rust
enum QueryOp { Scan(ScanData), Filter(FilterData), Project(ProjectData),
               Sort(SortData), Cross(CrossData) }
// Typical local query tree: Project(Sort(Filter(Scan(table))))
// Join queries: Project(Sort(Filter(Cross(Scan1, Scan2), join_preds)))
// sort_before_check=false for ORDER BY queries (exact order required)
// sort_before_check=true for non-ORDER BY queries (order doesn't matter)
```

## Current Optimizations in database/src/

### scan.rs
- Reads 64 blocks per disk request (`BLOCKS_PER_READ = 64`)

### sort.rs — External k-way merge sort
- `RUN_BUDGET_BYTES = 20 MB` — flush sorted run to disk when buffer exceeds this
- `READER_BATCH_BLOCKS = 64` — blocks per read during merge
- In-memory sort if all rows fit in one run (no disk writes)
- External path: flush N sorted runs → BinaryHeap k-way merge via `init_heap()` + `next_row()`
- Binary sort key encoding in `encode_key_value` (sign-bit-flip for ints, IEEE-754 fix for floats, null-terminated bytes for strings — handles ASC/DESC via bitwise complement)

### hashjoin.rs — Hash join with grace/partitioned fallback
- `INMEM_BUILD_BUDGET = 6 MB` — if right (build) side fits, pure in-memory hash join
- `NUM_PARTITIONS = 64`, `FLUSH_THRESHOLD = 256 KB` per partition buffer
- `READ_BATCH = 64` blocks per partition read
- If right side exceeds budget: grace hash join (partition both sides to disk, process partition by partition)
- Key fix: `drop(hash_table)` before creating `left_bufs` to avoid OOM

### cross.rs — Cartesian product
- Spills right child to anonymous disk blocks (64-block batches)
- `RIGHT_CACHE_BYTES = 8 MB` — if right side ≤ 8 MB after spill, cache in memory
- Cached path: zero disk reads per left row; disk-fallback: reads one block at a time
- **BUG WARNING**: `spill_right` assumes all flushed blocks are contiguous (uses `right_start_block + total_blocks`). If right child does its own disk writes between flushes, blocks are non-contiguous → wrong data read back. Safe only if right child is Filter(Scan) with no disk writes.

### mod.rs — Operator tree builder
**join reordering** (`build_join_tree`): largest table (by cardinality stat or file blocks) becomes probe (left) side. Smaller tables are build (right) side. Avoids putting large tables in hash join build side.

**`sort_already_satisfied`**: Skips Sort if:
1. ALL sort specs are ascending, AND
2. ALL sort columns have `IsPhysicallyOrdered` stat in db_config, AND
3. Source is single-table (Filter/Project wrapper around a Scan, NOT a join)
→ Called on `data.underlying` (the child of Sort node in AST)

**`equi_join_keys`**: Only EQ column-to-column predicates used as hash join keys.

**Column-overlap check**: If two joined tables share a column name, falls back to FilterOperator(CrossOperator) instead of build_join_tree.

## Current Test Status
- All 63 **local** tests pass ✓
- **4 btests FAILING**: btest1, btest14, btest18, btest19
  - btest1 expected first line: `A|F|27|39890.88|0.06|0.07|` (lineitem sort, quantity=Int32)
  - btest14 expected: `3489539|89947|N|1998-12-01|4|4|`
  - btest18 expected: `9857350|106356|N|1998-12-01|2|5|`
  - btest19 expected: `Customer#000111449|11-921-359-1677|ALGERIA|   2uZwVhQvwA|`
- "error at line 1" = first output row is wrong
- Btests use DIFFERENT (larger) db_config — different column types and possibly different IsPhysicallyOrdered stats
- Previous unoptimized code passed all btests; our optimizations introduced the bug

## Bug Investigation Status (UNRESOLVED)
Analyzed all files. No concrete bug found yet. **Most likely suspects**:

1. **`sort_already_satisfied` (mod.rs)** — May incorrectly skip sorts if btest db_config marks different columns as IsPhysicallyOrdered. Targeted fix: remove the optimization (3-line deletion in `build_operator` Sort arm).

2. **Join reordering changing schema order** — Reordering changes which table is left vs right, changing combined schema column order. ProjectOperator uses names not positions so should be fine. NOT yet confirmed as bug.

3. **External sort for large data** — Triggered when data > 20 MB. k-way merge logic appears correct but only tested locally (small data). Btest1 sorts large lineitem → external sort definitely triggered.

## Key Design Constraints
- 64 MB `RLIMIT_AS` — virtual address space limit (includes all heap, stack, code)
- Block size = 4096 bytes
- Disk protocol over FD3 (read) / FD4 (write)
- Single-threaded pull-based pipeline (`next()` returns one row at a time)
- `SharedDisk = Rc<RefCell<DiskManager>>` — shared mutable disk handle

## How to Build and Test
```bash
cd /home/aprat/COL362/A3
cargo build --release 2>&1 | tail -5
# Run local tests via monitor (user does this manually)
# btests are hidden — user runs them and reports results
```

## Git History
```
d8594f3  Fix OOM + add cardinality-aware join reordering   ← our session
ffc2f59  mem limit resolved                                 ← our session
0476d3b  passed all (63 local tests)                       ← our session
366b105  till 58                                           ← pre-session baseline
```
At `366b105`: had basic join tree building + in-memory-only hash join. No external sort, no grace hash join, no sort_already_satisfied, no join reordering.

## Files to Read at Start of New Session
Always read before editing:
- `database/src/executor/mod.rs` — join tree, sort optimization
- `database/src/executor/sort.rs` — external merge sort
- `database/src/executor/hashjoin.rs` — grace hash join
- `database/src/executor/cross.rs` — cross product caching

Reference only (rarely need to edit):
- `database/src/row.rs` — encode/decode/format
- `database/src/executor/filter.rs` — filter + OwnedFilterOperator
- `database/src/executor/project.rs` — column select by name
- `database/src/executor/scan.rs` — batched table scan
- `database/src/disk.rs` — disk protocol implementation
