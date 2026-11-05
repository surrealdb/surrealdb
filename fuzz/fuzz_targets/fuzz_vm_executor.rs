//! Fuzzing of the bytecode VM's execution of SurrealQL scripts.
//!
//! Each input is decoded into a short SurrealQL script over a fixed, seeded
//! dataset. The script runs twice, each time against a fresh in-memory
//! datastore built from the same setup. The target fails when the engine
//! panics, and when any statement's value or error differs between the two
//! runs.
//!
//! The generator is deterministic by construction: it names only builtins
//! whose result is a function of their arguments (no `rand::*`, `time::now`,
//! generated record ids, `sleep`, network or hashing with a random salt), gives
//! every record it writes an explicit id, and breaks every `ORDER BY` tie on
//! `id`. A difference between the two runs is therefore nondeterminism in the
//! engine, never noise; nothing is normalised in the comparison itself.

#![no_main]
// The engine's async call graph is deeper than the default limit allows.
#![recursion_limit = "256"]

use std::fmt::Write as _;

use libfuzzer_sys::arbitrary::{Result, Unstructured};
use libfuzzer_sys::fuzz_target;

mod vm_script;

use vm_script::{generate, run};

fuzz_target!(|data: &[u8]| {
	let Ok((setup, script, who)) = generate(data) else {
		return;
	};
	let print = std::env::var_os("FUZZ_PRINT").is_some();
	if print {
		// Before either run, so a script that panics the engine is shown too.
		eprintln!("--- as {who:?}\n{script}");
	}
	let Some(first) = run(&setup, &script, who) else {
		return;
	};
	let Some(second) = run(&setup, &script, who) else {
		return;
	};
	if print {
		eprintln!("{}", first.join("\n"));
	}
	if first != second {
		let mut report = format!("runs diverge as {who:?} on:\n{script}\n");
		for (i, (a, b)) in first.iter().zip(second.iter()).enumerate() {
			if a != b {
				let _ = write!(report, "\nstatement {i}:\n  first:  {a}\n  second: {b}\n");
			}
		}
		if first.len() != second.len() {
			let _ = write!(
				report,
				"\nresult counts differ: first {} second {}\n",
				first.len(),
				second.len()
			);
		}
		panic!("{report}");
	}
});
