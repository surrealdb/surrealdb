#![feature(test)]

extern crate test;
use test::{black_box, Bencher};

#[bench]
fn select_simple(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::parse(black_box("SELECT * FROM person;"))).unwrap();
	});
}

#[bench]
fn select_complex(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::parse(black_box(
			"SELECT name, age, country FROM person WHERE hair = \"brown\" AND is_vegetarian;",
		)))
		.unwrap();
	});
}

#[bench]
fn transaction(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::parse(black_box("BEGIN TRANSACTION; UPDATE person:finn SET squirrels = \"yes\"; SELECT * FROM person; COMMIT TRANSACTION;"))).unwrap();
	});
}

#[bench]
fn datetime(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::parse(black_box("RETURN \"2022-07-03T07:18:52.841147+02:00\";")))
			.unwrap();
	});
}

#[bench]
fn duration(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::parse(black_box("RETURN [100w, 5d, 20m, 2s];"))).unwrap();
	});
}

#[bench]
fn casting_deep(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::parse(black_box(
			"SELECT * FROM <float><float><float><float><float>1.0;",
		)))
		.unwrap();
	});
}

#[bench]
fn json_geo(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::parse(black_box(
			"RETURN { type: \"Point\", coordinates: [-0.118092, 51.509865] };",
		)))
		.unwrap();
	});
}

#[bench]
fn json_number(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::json(black_box("1.2345"))).unwrap();
	});
}

#[bench]
fn json_small_object(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::json(black_box(
			"{\"key\": true, \"number\": 42.0, \"value\": null}",
		)))
		.unwrap();
	});
}

#[bench]
fn json_small_array(b: &mut Bencher) {
	b.iter(|| {
		black_box(surrealdb::sql::json(black_box("[1, false, null, \"foo\"]"))).unwrap();
	});
}

#[bench]
fn json_large_array(b: &mut Bencher) {
	let large_array =
		format!("[{}]", (1..=100).map(|n| n.to_string()).collect::<Vec<_>>().join(", "));
	b.iter(|| {
		black_box(surrealdb::sql::json(black_box(&large_array))).unwrap();
	});
}

#[bench]
fn json_large_object(b: &mut Bencher) {
	let large_object = format!(
		"{{{}}}",
		(1..=100).map(|n| format!("\"{n}\": {n}")).collect::<Vec<_>>().join(", ")
	);
	b.iter(|| {
		black_box(surrealdb::sql::json(black_box(&large_object))).unwrap();
	});
}
