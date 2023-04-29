#![feature(test)]

extern crate test;
use test::{black_box, Bencher};

macro_rules! parser {
	($name: ident, $parser: path, $text: expr) => {
		#[bench]
		fn $name(b: &mut Bencher) {
			let text = $text;
			b.iter(|| {
				black_box($parser(black_box(text))).unwrap();
			});
		}
	};
}

parser!(select_simple, surrealdb::sql::parse, "SELECT * FROM person;");
parser!(
	select_complex,
	surrealdb::sql::parse,
	"SELECT name, age, country FROM person WHERE hair = 'brown' AND is_vegetarian;"
);
parser!(transaction, surrealdb::sql::parse, "BEGIN TRANSACTION; UPDATE person:finn SET squirrels = 'yes'; SELECT * FROM person; COMMIT TRANSACTION;");
parser!(datetime, surrealdb::sql::parse, "RETURN '2022-07-03T07:18:52.841147+02:00';");
parser!(duration, surrealdb::sql::parse, "RETURN [100w, 5d, 20m, 2s];");
parser!(casting_deep, surrealdb::sql::parse, "RETURN <float><float><float><float><float>1.0;");
parser!(
	json_geo,
	surrealdb::sql::parse,
	"RETURN { type: 'Point', coordinates: [-0.118092, 51.509865] };"
);
parser!(json_number, surrealdb::sql::json, "1.2345");
parser!(json_small_object, surrealdb::sql::json, "{'key': true, 'number': 42.0, 'value': null}");
parser!(json_small_array, surrealdb::sql::json, "[1, false, null, 'foo']");
parser!(
	json_large_array,
	surrealdb::sql::json,
	&format!("[{}]", (1..=100).map(|n| n.to_string()).collect::<Vec<_>>().join(", "))
);
parser!(
	json_large_object,
	surrealdb::sql::json,
	&format!("{{{}}}", &(1..=100).map(|n| format!("'{n}': {n}")).collect::<Vec<_>>().join(", "))
);
