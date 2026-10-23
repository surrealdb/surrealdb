//! Where one commit's allocations come from: a native run of the engine's write path (a 500-row INSERT of
//! 1 KiB values into the in-memory store, as the node's smoke preloads), with a global allocator that
//! records a backtrace for each allocation while it is switched on, and a table of the sites by count and
//! bytes. A site is the first frames that are not the allocator, the standard library or this file.
//!   cargo run --release --example alloc_sites -p surrealdb-cf [rows] [frames]
//! It measures the engine, not the node's log layer (`LogTransaction`), which the wasm CPU profile covers.

use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::capabilities::Capabilities;

struct Prof;

static ON: AtomicBool = AtomicBool::new(false);
static TOTAL: AtomicU64 = AtomicU64::new(0);
static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
static SITES: Mutex<Option<HashMap<String, (u64, u64)>>> = Mutex::new(None);
static SIZES: Mutex<[u64; 5]> = Mutex::new([0; 5]);

thread_local! {
    static BUSY: Cell<bool> = const { Cell::new(false) };
}

fn site_of(bt: &str, depth: usize) -> String {
    // Frames come as `N: name` then `at path:line:col`. A site is the first `depth` frames whose file
    // is not the standard library's (`/rustc/`) or this file's: the engine's and its dependencies' own.
    let mut frames = Vec::new();
    let mut name = "";
    for line in bt.lines() {
        let l = line.trim();
        if let Some(path) = l.strip_prefix("at ") {
            if path.starts_with("/rustc/") || path.contains("alloc_sites.rs") {
                continue;
            }
            let short = path.rsplit_once("/src/").map_or(path, |(_, f)| f);
            let short = short.rsplit_once(':').map_or(short, |(a, _)| a); // drop the column
            let bare = name.rsplit_once("::h").map_or(name, |(a, _)| a);
            let bare: String = bare.chars().take(60).collect();
            frames.push(format!("{bare} @ {short}"));
            if frames.len() == depth {
                break;
            }
        } else if let Some((_, n)) = l.split_once(": ") {
            name = n;
        }
    }
    frames.join("  <  ")
}

unsafe impl GlobalAlloc for Prof {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if ON.load(Relaxed) && !BUSY.with(|b| b.replace(true)) {
            let bt = Backtrace::force_capture().to_string();
            let depth = DEPTH.load(Relaxed) as usize;
            let key = site_of(&bt, depth);
            TOTAL.fetch_add(1, Relaxed);
            TOTAL_BYTES.fetch_add(l.size() as u64, Relaxed);
            {
                let mut s = SITES.lock().unwrap();
                let e = s.get_or_insert_with(HashMap::new).entry(key).or_insert((0, 0));
                e.0 += 1;
                e.1 += l.size() as u64;
                let class = match l.size() {
                    0..256 => 0,
                    256..4096 => 1,
                    4096..65536 => 2,
                    65536..1048576 => 3,
                    _ => 4,
                };
                SIZES.lock().unwrap()[class] += 1;
            }
            BUSY.with(|b| b.set(false));
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}

static DEPTH: AtomicU64 = AtomicU64::new(4);

#[global_allocator]
static A: Prof = Prof;

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    let rows: usize = std::env::args().nth(1).and_then(|v| v.parse().ok()).unwrap_or(500);
    DEPTH.store(std::env::args().nth(2).and_then(|v| v.parse().ok()).unwrap_or(4), Relaxed);
    let ds = Datastore::builder()
        .without_maintenance_tasks()
        .with_capabilities(Capabilities::all())
        .build_with_path("memory")
        .await?;
    let sess = Session::owner().with_ns("t").with_db("t");
    ds.execute("DEFINE NS t", &Session::owner(), None).await?;
    ds.execute("DEFINE DB t", &Session::owner().with_ns("t"), None).await?;
    ds.execute("DEFINE TABLE pre SCHEMALESS", &sess, None).await?;
    let pad = "p".repeat(1000);
    let insert = |from: usize| {
        let rows: Vec<String> = (0..rows).map(|i| format!("{{id:{},p:\"{pad}\"}}", from + i)).collect();
        format!("INSERT INTO pre [{}];", rows.join(","))
    };
    for k in 0..5 {
        ds.execute(&insert(k * rows), &sess, None).await?;
    }
    let q = insert(1_000_000);
    ON.store(true, Relaxed);
    let res = ds.execute(&q, &sess, None).await?;
    ON.store(false, Relaxed);
    assert!(res.iter().all(|r| r.result.is_ok()), "the insert failed");
    let (n, bytes) = (TOTAL.load(Relaxed), TOTAL_BYTES.load(Relaxed));
    println!("{rows} rows, {} bytes of values: {n} allocations, {bytes} bytes ({:.0}x the values)", rows * 1000, bytes as f64 / (rows * 1000) as f64);
    let sz = SIZES.lock().unwrap();
    println!("by size [<256 B, <4 KiB, <64 KiB, <1 MiB, larger]: {:?}", *sz);
    let sites = SITES.lock().unwrap().take().unwrap_or_default();
    let mut v: Vec<_> = sites.into_iter().collect();
    v.sort_by_key(|(_, (c, _))| std::cmp::Reverse(*c));
    println!("\n-- top sites by count --");
    for (k, (c, b)) in v.iter().take(25) {
        println!("{c:>7} allocs {b:>10} B  {k}");
    }
    v.sort_by_key(|(_, (_, b))| std::cmp::Reverse(*b));
    println!("\n-- top sites by bytes --");
    for (k, (c, b)) in v.iter().take(15) {
        println!("{b:>10} B {c:>7} allocs  {k}");
    }
    Ok(())
}
