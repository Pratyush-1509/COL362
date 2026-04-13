use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::cell::RefCell;
use std::io::{BufRead, BufReader, Write};
use std::rc::Rc;

use crate::cli::CliOptions;
use crate::disk::DiskManager;
use crate::executor::build_operator;
use crate::io_setup::{setup_disk_io, setup_monitor_io};
use crate::row::format_row;

mod cli;
mod disk;
mod executor;
mod io_setup;
mod row;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    // Load table schema + statistics from db_config.json
    let ctx = DbContext::load_from_file(cli_options.get_config_path())
        .context("failed to load DbContext")?;

    // Wire up IO handles (FD3/4 for disk, FD5/6 for monitor)
    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();
    let mut monitor_reader = BufReader::new(monitor_in);

    // Shared disk manager — Rc<RefCell<>> lets every operator borrow it
    // mutably without lifetime headaches; safe because we are single-threaded
    // and the pull model never has two operators active simultaneously.
    let disk = Rc::new(RefCell::new(DiskManager::new(disk_in, disk_out)));

    // ── Step 1: receive query from monitor ────────────────────────────────────
    let mut query_json = String::new();
    monitor_reader
        .read_line(&mut query_json)
        .context("failed to read query from monitor")?;
    let query: Query =
        serde_json::from_str(&query_json).context("failed to parse query JSON")?;

    // ── Step 2: ask monitor for memory budget (optional optimisation hint) ───
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    let mut mem_line = String::new();
    monitor_reader
        .read_line(&mut mem_line)
        .context("failed to read memory limit from monitor")?;
    let _memory_limit_mb: u64 = mem_line.trim().parse().unwrap_or(64);
    // TODO: pass _memory_limit_mb into SortOperator / CrossOperator so they can
    //       decide how many scratch blocks to use for external sort / spill.

    // ── Step 3: build the operator tree and execute ───────────────────────────
    // `query` must outlive `op` — both live in this function scope, and Rust
    // drops locals in reverse declaration order, so `query` outlives `op`. ✓
    let mut op = build_operator(&query.root, &ctx, Rc::clone(&disk));

    // ── Step 4: stream results to monitor ────────────────────────────────────
    // Protocol: "validate\n"  then  one "col1|col2|...|colN|\n" per row  then "!\n"
    monitor_out.write_all(b"validate\n")?;

    while let Some(row) = op.next() {
        let mut line = format_row(&row);
        line.push('\n');
        monitor_out.write_all(line.as_bytes())?;
    }

    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "Database error")
}
