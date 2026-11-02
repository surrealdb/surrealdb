//! `#[derive(DynHash)]` feeds a hasher what `#[derive(Hash)]` feeds it: the same calls with the
//! same values in the same order, so a digest over a type is the same whichever derive it carries.

use std::hash::{Hash, Hasher};

/// Records every call a hasher receives.
#[derive(Default, PartialEq, Debug)]
struct Recorder(Vec<String>);

impl Hasher for Recorder {
	fn finish(&self) -> u64 {
		0
	}
	fn write(&mut self, bytes: &[u8]) {
		self.0.push(format!("bytes {bytes:?}"));
	}
	fn write_u8(&mut self, i: u8) {
		self.0.push(format!("u8 {i}"));
	}
	fn write_u32(&mut self, i: u32) {
		self.0.push(format!("u32 {i}"));
	}
	fn write_u64(&mut self, i: u64) {
		self.0.push(format!("u64 {i}"));
	}
	fn write_usize(&mut self, i: usize) {
		self.0.push(format!("usize {i}"));
	}
	fn write_isize(&mut self, i: isize) {
		self.0.push(format!("isize {i}"));
	}
}

macro_rules! twins {
	($derived:ident, $dynamic:ident, $($body:tt)*) => {
		#[derive(Hash)]
		#[allow(dead_code)]
		enum $derived $($body)*
		#[derive(surrealdb_stored_derive::DynHash)]
		#[allow(dead_code)]
		enum $dynamic $($body)*
	};
}

twins!(EnumA, EnumB, {
	Unit,
	Tuple(u32, String),
	Named { state: u64, name: Option<String>, inner: Box<[u8]> },
});

twins!(OneA, OneB, {
	Only(u8),
});

#[derive(Hash)]
struct StructA {
	a: u32,
	b: (String, i8),
}

#[derive(surrealdb_stored_derive::DynHash)]
struct StructB {
	a: u32,
	b: (String, i8),
}

fn calls(value: &impl Hash) -> Vec<String> {
	let mut recorder = Recorder::default();
	value.hash(&mut recorder);
	recorder.0
}

#[test]
fn enums_hash_as_derived() {
	assert_eq!(calls(&EnumA::Unit), calls(&EnumB::Unit));
	assert_eq!(calls(&EnumA::Tuple(7, "x".into())), calls(&EnumB::Tuple(7, "x".into())));
	assert_eq!(
		calls(&EnumA::Named {
			state: 9,
			name: Some("n".into()),
			inner: Box::new([1, 2])
		}),
		calls(&EnumB::Named {
			state: 9,
			name: Some("n".into()),
			inner: Box::new([1, 2])
		}),
	);
	assert_eq!(calls(&OneA::Only(3)), calls(&OneB::Only(3)));
}

#[test]
fn structs_hash_as_derived() {
	let a = StructA {
		a: 1,
		b: ("s".into(), -2),
	};
	let b = StructB {
		a: 1,
		b: ("s".into(), -2),
	};
	assert_eq!(calls(&a), calls(&b));
}
