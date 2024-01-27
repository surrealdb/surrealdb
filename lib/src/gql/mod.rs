pub mod query;
pub mod schema;

pub use query::parse_and_transpile;
pub use schema::get_schema;

// #[cfg(test)]
// mod test {
// 	use super::*;

// 	#[tokio::test]
// 	async fn test_schema_generation() {
// 		let ds = Datastore::new("memory").await.unwrap();
// 		ds.execute_sql(
// 			r#"USE NS test; USE DB test;
//             DEFINE TABLE person SCHEMAFUL;
//             DEFINE FIELD name ON person TYPE string;
//             DEFINE FIELD companies ON person TYPE array<record<company>>;
//             DEFINE TABLE company SCHEMAFUL;
//             DEFINE FIELD name ON company TYPE string;
//             "#,
// 			&Default::default(),
// 			None,
// 		)
// 		.await
// 		.unwrap();
// 		let schema = get_schema(&ds, "test".to_string(), "test".to_string()).await.unwrap();

// 		panic!("{}", schema);
// 	}
// }
