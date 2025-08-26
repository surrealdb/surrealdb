mod helpers;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use helpers::*;
use surrealdb::Result;
use surrealdb_core::dbs::{Session, Variables};
use surrealdb_core::err::Error;
use surrealdb_core::expr::{Ident, Idiom, Part};
use surrealdb_core::iam::{Level, Role};
use surrealdb_core::kvs::{LockType, TransactionType};
use surrealdb_core::val::Value;
use surrealdb_core::{strand, syn};
use test_log::test;
use tracing::info;

#[tokio::test]
async fn define_statement_namespace() -> Result<()> {
	let sql = "
		DEFINE NAMESPACE test;
		INFO FOR ROOT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok(), "{:?}", tmp);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: {},
			namespaces: { test: 'DEFINE NAMESPACE test' },
			nodes: {},
			system: {
				available_parallelism: 0,
				cpu_usage: 0.0f,
				load_average: [
					0.0f,
					0.0f,
					0.0f
				],
				memory_allocated: 0,
				memory_usage: 0,
				physical_cores: 0,
                threads: 0
			},
			users: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_database() -> Result<()> {
	let sql = "
		DEFINE DATABASE test;
		INFO FOR NS;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: {},
			databases: { test: 'DEFINE DATABASE test' },
			users: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

async fn define_statement_index_concurrently_building_status(
	def_index: &str,
	skip_def: usize,
	initial_size: usize,
	appended_size: usize,
) -> Result<()> {
	let session = Session::owner().with_ns("test").with_db("test");
	let ds = new_ds().await?;
	// Populate initial records
	info!("Populate: {}", initial_size);
	for i in 0..initial_size {
		let mut responses = ds
			.execute(
				&format!("CREATE user:{i} SET email = 'test{i}@surrealdb.com';"),
				&session,
				None,
			)
			.await?;
		skip_ok(&mut responses, 1)?;
	}
	// Create the index concurrently
	info!("Indexing starts");
	let mut r = ds.execute(def_index, &session, None).await?;
	assert_eq!(r.len(), skip_def);
	skip_ok(&mut r, skip_def)?;
	//
	// Loop until the index is built
	let now = SystemTime::now();
	let mut initial_count = None;
	let mut pending_count = None;
	let mut updated_count = None;
	let mut appended_count = 0;
	// While the concurrent indexing is running, we update and delete records
	info!("Loop");
	let time_out = Duration::from_secs(300);
	loop {
		if now.elapsed().map_err(|e| Error::Internal(e.to_string()))?.gt(&time_out) {
			panic!("Time-out {time_out:?}");
		}
		// Update and delete records
		if appended_count < appended_size {
			let sql = if appended_count % 2 == 0 {
				format!(
					"UPDATE user:{appended_count} SET email = 'new{appended_count}@surrealdb.com'"
				)
			} else {
				format!("DELETE user:{appended_count}")
			};
			let mut responses = ds.execute(&sql, &session, None).await?;
			skip_ok(&mut responses, 1)?;
			appended_count += 1;
		}
		// We monitor the status
		let mut r = ds.execute("INFO FOR INDEX test ON user", &session, None).await?;
		let tmp = r.remove(0).result?;
		if let Value::Object(o) = &tmp {
			if let Some(Value::Object(o)) = o.get("building") {
				if let Some(Value::Strand(s)) = o.get("status") {
					let new_initial = o.get("initial").cloned();
					let new_pending = o.get("pending").cloned();
					let new_updated = o.get("updated").cloned();
					match s.as_str() {
						"started" => {
							info!("Started");
							continue;
						}
						"cleaning" => {
							info!("Cleaning");
							continue;
						}
						"indexing" => {
							{
								if new_initial != initial_count {
									assert!(new_initial > initial_count, "{new_initial:?}");
									info!("New initial count: {:?}", new_initial);
									initial_count = new_initial;
								}
							}
							{
								if new_pending != pending_count {
									info!("New pending count: {:?}", new_pending);
									pending_count = new_pending;
								}
							}
							{
								if new_updated != updated_count {
									assert!(new_updated > updated_count, "{new_updated:?}");
									info!("New updated count: {:?}", new_updated);
									updated_count = new_updated;
								}
							}
							continue;
						}
						"ready" => {
							let initial = new_initial.unwrap().coerce_to::<i64>()? as usize;
							let pending = new_pending.unwrap().coerce_to::<i64>()?;
							let updated = new_updated.unwrap().coerce_to::<i64>()? as usize;
							assert!(initial > 0, "{initial} > 0");
							assert!(initial <= initial_size, "{initial} <= {initial_size}");
							assert_eq!(pending, 0);
							assert!(updated > 0, "{updated} > 0");
							assert!(updated <= appended_count, "{updated} <= appended_count");
							break;
						}
						_ => {}
					}
				}
			}
		}
		panic!("Invalid info: {tmp:#}");
	}
	info!("Appended: {appended_count}");
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_statement_index_concurrently_building_status_standard() -> Result<()> {
	define_statement_index_concurrently_building_status(
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
		1,
		10000,
		100,
	)
	.await
}

#[tokio::test(flavor = "multi_thread")]
async fn define_statement_index_concurrently_building_status_standard_overwrite() -> Result<()> {
	define_statement_index_concurrently_building_status(
		"DEFINE INDEX OVERWRITE test ON user FIELDS email CONCURRENTLY",
		1,
		10000,
		100,
	)
	.await
}

#[test(tokio::test)]
async fn define_statement_index_concurrently_building_status_full_text() -> Result<()> {
	define_statement_index_concurrently_building_status(
		"DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX test ON user FIELDS email SEARCH ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;",
		2,
		200,
		10,
	)
	.await
}

#[test(tokio::test)]
async fn define_statement_index_concurrently_building_status_full_text_overwrite() -> Result<()> {
	define_statement_index_concurrently_building_status(
		"DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX OVERWRITE test ON user FIELDS email SEARCH ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;",
		2,
		200, 10
	)
		.await
}

#[tokio::test]
async fn define_statement_analyzer() -> Result<()> {
	let sql = r#"
		DEFINE ANALYZER english TOKENIZERS blank,class FILTERS lowercase,snowball(english);
		DEFINE ANALYZER autocomplete FILTERS lowercase,edgengram(2,10);
        DEFINE FUNCTION fn::stripHtml($html: string) {
            RETURN string::replace($html, /<[^>]*>/, "");
        };
        DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS blank,class;
        DEFINE ANALYZER englishLemmatizer TOKENIZERS blank,class FILTERS mapper('../../tests/data/lemmatization-en.txt');
		INFO FOR DB;
	"#;
	let mut t = Test::new(sql).await?;
	t.expect_size(6)?;
	t.skip_ok(5)?;
	t.expect_val(
		r#"{
			accesses: {},
			analyzers: {
				autocomplete: 'DEFINE ANALYZER autocomplete FILTERS LOWERCASE,EDGENGRAM(2,10)',
				english: 'DEFINE ANALYZER english TOKENIZERS BLANK,CLASS FILTERS LOWERCASE,SNOWBALL(ENGLISH)',
				englishLemmatizer: 'DEFINE ANALYZER englishLemmatizer TOKENIZERS BLANK,CLASS FILTERS MAPPER(../../tests/data/lemmatization-en.txt)',
				htmlAnalyzer: 'DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS BLANK,CLASS'
			},
			apis: {},
			buckets: {},
			configs: {},
			functions: {
				stripHtml: "DEFINE FUNCTION fn::stripHtml($html: string) { RETURN string::replace($html, /<[^>]*>/, '') } PERMISSIONS FULL"
			},
			models: {},
			params: {},
			tables: {},
			sequences: {},
			users: {},
		}"#,
	)?;
	Ok(())
}

#[tokio::test]
async fn define_statement_search_index() -> Result<()> {
	let sql = r#"
		CREATE blog:1 SET title = 'Understanding SurrealQL and how it is different from PostgreSQL';
		CREATE blog:3 SET title = 'This blog is going to be deleted';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		CREATE blog:2 SET title = 'Behind the scenes of the exciting beta 9 release';
		DELETE blog:3;
		INFO FOR TABLE blog;
		ANALYZE INDEX blog_title ON blog;
	"#;

	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	//
	for i in 0..6 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok(), "{}", i);
	}

	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { blog_title: 'DEFINE INDEX blog_title ON blog FIELDS title \
			SEARCH ANALYZER simple BM25(1.2,0.75) \
			DOC_IDS_ORDER 100 DOC_LENGTHS_ORDER 100 POSTINGS_ORDER 100 TERMS_ORDER 100 \
			DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 100 POSTINGS_CACHE 100 TERMS_CACHE 100 HIGHLIGHTS' },
			lives: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);

	let tmp = res.remove(0).result?;

	check_path(&tmp, &["doc_ids", "keys_count"], |v| assert_eq!(v, Value::from(2)));
	check_path(&tmp, &["doc_ids", "max_depth"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["doc_ids", "nodes_count"], |v| assert_eq!(v, Value::from(1)));
	// TODO(emmanuel) My (Mees) changes caused some changes in these numbers but I
	// didn't have time to figure out what was going on so if you could have a look
	// after the PR merges it would be appreaciated.
	check_path(&tmp, &["doc_ids", "total_size"], |v| assert_eq!(v, Value::from(63)));

	check_path(&tmp, &["doc_lengths", "keys_count"], |v| assert_eq!(v, Value::from(2)));
	check_path(&tmp, &["doc_lengths", "max_depth"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["doc_lengths", "nodes_count"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["doc_lengths", "total_size"], |v| assert_eq!(v, Value::from(56)));

	check_path(&tmp, &["postings", "keys_count"], |v| assert_eq!(v, Value::from(17)));
	check_path(&tmp, &["postings", "max_depth"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["postings", "nodes_count"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["postings", "total_size"], |v| assert!(v > Value::from(150)));

	check_path(&tmp, &["terms", "keys_count"], |v| assert_eq!(v, Value::from(17)));
	check_path(&tmp, &["terms", "max_depth"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["terms", "nodes_count"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["terms", "total_size"], |v| assert!(v.gt(&Value::from(150))));

	Ok(())
}

#[tokio::test]
async fn define_statement_user_root() -> Result<()> {
	let sql = "
		DEFINE USER test ON ROOT PASSWORD 'test';

		INFO FOR ROOT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner();
	let res = &mut dbs.execute(sql, &ses, None).await?;

	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;

	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let define_str = tmp
		.pick(&[
			Part::Field(Ident::from_strand(strand!("users").to_owned())),
			Part::Field(Ident::from_strand(strand!("test").to_owned())),
		])
		.to_string();

	assert!(
		define_str
			.strip_prefix('\"')
			.unwrap()
			.starts_with("DEFINE USER test ON ROOT PASSHASH '$argon2id$")
	);
	Ok(())
}

#[tokio::test]
async fn define_statement_user_ns() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner();

	// Create a NS user and retrieve it.
	let sql = "
		USE NS ns;
		DEFINE USER test ON NS PASSWORD 'test';

		INFO FOR USER test;
		INFO FOR USER test ON NS;
		INFO FOR USER test ON NAMESPACE;
		INFO FOR USER test ON ROOT;
	";
	let res = dbs.execute(sql, &ses, None).await?;

	let mut res = res.into_iter();
	res.next().unwrap().result.unwrap();
	res.next().unwrap().result.unwrap();

	assert!(
		res.next()
			.unwrap()
			.result
			.as_ref()
			.unwrap()
			.to_string()
			.starts_with("\"DEFINE USER test ON NAMESPACE PASSHASH '$argon2id$")
	);
	assert!(
		res.next()
			.unwrap()
			.result
			.as_ref()
			.unwrap()
			.to_string()
			.starts_with("\"DEFINE USER test ON NAMESPACE PASSHASH '$argon2id$")
	);
	assert!(
		res.next()
			.unwrap()
			.result
			.as_ref()
			.unwrap()
			.to_string()
			.starts_with("\"DEFINE USER test ON NAMESPACE PASSHASH '$argon2id$")
	);

	assert_eq!(
		res.next().unwrap().result.as_ref().unwrap_err().to_string(),
		"The root user 'test' does not exist"
	); // User doesn't exist at the NS level

	// If it tries to create a NS user without specifying a NS, it should fail
	let sql = "
		DEFINE USER test ON NS PASSWORD 'test';
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;

	assert!(res.remove(0).result.is_err());

	Ok(())
}

#[tokio::test]
async fn define_statement_user_db() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner();

	// Create a NS user and retrieve it.
	let sql = "
		USE NS ns;
		USE DB db;
		DEFINE USER test ON DB PASSWORD 'test';

		INFO FOR USER test;
		INFO FOR USER test ON DB;
		INFO FOR USER test ON DATABASE;
		INFO FOR USER test ON NS;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;

	res[2].result.as_ref().unwrap();
	res[3].result.as_ref().unwrap();
	res[4].result.as_ref().unwrap();
	res[5].result.as_ref().unwrap();
	assert_eq!(
		res[6].result.as_ref().unwrap_err().to_string(),
		"The user 'test' does not exist in the namespace 'ns'"
	); // User doesn't exist at the NS level

	assert!(
		res[3]
			.result
			.as_ref()
			.unwrap()
			.to_string()
			.starts_with("\"DEFINE USER test ON DATABASE PASSHASH '$argon2id$")
	);
	assert!(
		res[4]
			.result
			.as_ref()
			.unwrap()
			.to_string()
			.starts_with("\"DEFINE USER test ON DATABASE PASSHASH '$argon2id$")
	);
	assert!(
		res[5]
			.result
			.as_ref()
			.unwrap()
			.to_string()
			.starts_with("\"DEFINE USER test ON DATABASE PASSHASH '$argon2id$")
	);

	// If it tries to create a NS user without specifying a NS, it should fail
	let sql = "
		DEFINE USER test ON DB PASSWORD 'test';
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;

	assert!(res.remove(0).result.is_err());

	Ok(())
}

fn check_path<F>(val: &Value, path: &[&str], check: F)
where
	F: Fn(Value),
{
	let part: Vec<Part> = path.iter().map(|p| Part::field((*p).to_owned()).unwrap()).collect();
	let res = val.walk(&part);
	for (i, v) in res {
		let mut idiom = Idiom::default();
		idiom.0.clone_from(&part);
		assert_eq!(idiom, i);
		check(v);
	}
}

//
// Permissions
//

fn level_root() -> Level {
	Level::Root
}
fn level_ns() -> Level {
	Level::Namespace("NS".to_owned())
}
fn level_db() -> Level {
	Level::Database("NS".to_owned(), "DB".to_owned())
}

#[tokio::test]
async fn permissions_checks_define_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE NAMESPACE NS"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, namespaces: {  }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{NS}}", "NS")),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.replace("{{NS}}", "NS")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let check_anonymous_success = check_success.replace("{{NS}}", "NS");
	let res = iam_check_cases_impl(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_error,
		false,
		false,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_db() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "DEFINE DATABASE DB"), ("check", "INFO FOR NS")]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success =
		"{ accesses: {  }, databases: { {{DB}}: 'DEFINE DATABASE {{DB}}' }, users: {  } }"
			.to_string();
	let check_error = "{ accesses: {  }, databases: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let check_anonymous_success = check_success.replace("{{DB}}", "DB");
	iam_check_cases_impl(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_error,
		true,
		false,
	)
	.await
	.unwrap();
}

#[tokio::test]
async fn permissions_checks_define_function() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE FUNCTION fn::greet() {RETURN \"Hello\";}"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: { greet: \"DEFINE FUNCTION fn::greet() { RETURN 'Hello' } PERMISSIONS FULL\" }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_analyzer() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ANALYZER analyzer TOKENIZERS BLANK"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, analyzers: { analyzer: 'DEFINE ANALYZER analyzer TOKENIZERS BLANK' }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, sequences: { }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {}, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_root() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ accesses: { access: "DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }"#.to_string();
	let check_error = "{ accesses: {  }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{NS}}", "NS")),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
	];

	let check_anonymous_success = check_success.replace("{{NS}}", "NS");
	let check_anonymous_error = check_error.replace("{{NS}}", "NS");
	let res = iam_check_cases(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_anonymous_error,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON NS TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: { access: \"DEFINE ACCESS access ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, databases: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, databases: { }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let check_anonymous_success = check_success.replace("{{DB}}", "DB");
	let res = iam_check_cases_impl(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_error,
		true,
		false,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_db() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON DB TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: { access: \"DEFINE ACCESS access ON DATABASE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_root() {
	let scenario = HashMap::from([
		("prepare", ""),
		(
			"test",
			"DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h",
		),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ accesses: {  }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: { user: "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h" } }"#.to_string();
	let check_error = "{ accesses: {  }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{NS}}", "NS")),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
	];

	let check_anonymous_success = check_success.replace("{{NS}}", "NS");
	let check_anonymous_error = check_error.replace("{{NS}}", "NS");
	let res = iam_check_cases_impl(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_anonymous_error,
		true,
		false,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		(
			"test",
			"DEFINE USER user ON NS PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h",
		),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ accesses: {  }, databases: { {{DB}}: 'DEFINE DATABASE {{DB}}' }, users: { user: "DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h" } }"#.to_string();
	let check_error =
		"{ accesses: {  }, databases: { {{DB}}: 'DEFINE DATABASE {{DB}}' }, users: {  } }"
			.to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Owner),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
	];

	let check_anonymous_success = check_success.replace("{{DB}}", "DB");
	let check_anonymous_error = check_error.replace("{{DB}}", "DB");
	let res = iam_check_cases(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_anonymous_error,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_db() {
	let scenario = HashMap::from([
		("prepare", ""),
		(
			"test",
			"DEFINE USER user ON DB PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h",
		),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: { user: "DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h" } }"#.to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_record() {
	let scenario = HashMap::from([
		("prepare", ""),
		(
			"test",
			"DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'secret' DURATION FOR TOKEN 15m, FOR SESSION 12h",
		),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ accesses: { account: "DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 15m, FOR SESSION 12h" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }"#.to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_param() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE PARAM $param VALUE 'foo'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: { param: "DEFINE PARAM $param VALUE 'foo' PERMISSIONS FULL" }, sequences: { }, tables: {  }, users: {  } }"#.to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_table() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "DEFINE TABLE TB"), ("check", "INFO FOR DB")]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: { TB: 'DEFINE TABLE TB TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"#.to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: { }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_event() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE EVENT event ON TABLE TB WHEN true THEN RETURN 'foo'"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ events: { event: "DEFINE EVENT event ON TB WHEN true THEN RETURN 'foo'" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"#.to_string();
	let check_error =
		"{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_field() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE FIELD field ON TABLE TB"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ events: {  }, fields: { field: 'DEFINE FIELD field ON TB PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"#.to_string();
	let check_error =
		"{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_index() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE INDEX index ON TABLE TB FIELDS field"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = r#"{ events: {  }, fields: {  }, indexes: { index: 'DEFINE INDEX index ON TB FIELDS field' }, lives: {  }, tables: {  } }"#.to_string();
	let check_error =
		"{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn cross_transaction_caching_uuids_updated() -> Result<()> {
	let ds = new_ds().await?;
	let cache = ds.get_cache();
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	let txn = ds.transaction(TransactionType::Write, LockType::Pessimistic).await?;
	let db = txn.ensure_ns_db("test", "test", false).await?;
	drop(txn);

	// Define the table, set the initial uuids
	let sql = r"DEFINE TABLE test;".to_owned();
	let res = &mut ds.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	res.remove(0).result.unwrap();
	// Obtain the initial uuids
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let initial = txn.get_tb(db.namespace_id, db.database_id, "test").await?.unwrap();
	let initial_live_query_version =
		cache.get_live_queries_version(db.namespace_id, db.database_id, "test")?;
	txn.cancel().await?;

	// Define some resources to refresh the UUIDs
	let sql = r"
		DEFINE FIELD test ON test;
		DEFINE EVENT test ON test WHEN {} THEN {};
		DEFINE TABLE view AS SELECT * FROM test;
		DEFINE INDEX test ON test FIELDS test;
		LIVE SELECT * FROM test;
	"
	.to_owned();
	let res = &mut ds.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	let lqid = res.remove(0).result?;
	assert!(matches!(lqid, Value::Uuid(_)));
	// Obtain the uuids after definitions
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let after_define = txn.get_tb(db.namespace_id, db.database_id, "test").await?.unwrap();
	let after_define_live_query_version =
		cache.get_live_queries_version(db.namespace_id, db.database_id, "test")?;
	txn.cancel().await?;
	// Compare uuids after definitions
	assert_ne!(initial.cache_indexes_ts, after_define.cache_indexes_ts);
	assert_ne!(initial.cache_tables_ts, after_define.cache_tables_ts);
	assert_ne!(initial.cache_events_ts, after_define.cache_events_ts);
	assert_ne!(initial.cache_fields_ts, after_define.cache_fields_ts);
	assert_ne!(initial_live_query_version, after_define_live_query_version);

	// Remove the defined resources to refresh the UUIDs
	let sql = r"
		REMOVE FIELD test ON test;
		REMOVE EVENT test ON test;
		REMOVE TABLE view;
		REMOVE INDEX test ON test;
		KILL $lqid;
	"
	.to_owned();
	let vars = Variables::from(map! { "lqid".to_string() => lqid });
	let res = &mut ds.execute(&sql, &ses, Some(vars)).await?;
	assert_eq!(res.len(), 5);
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	// Obtain the uuids after definitions
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let after_remove = txn.get_tb(db.namespace_id, db.database_id, "test").await?.unwrap();
	let after_remove_live_query_version =
		cache.get_live_queries_version(db.namespace_id, db.database_id, "test")?;
	drop(txn);
	// Compare uuids after definitions
	assert_ne!(after_define.cache_fields_ts, after_remove.cache_fields_ts);
	assert_ne!(after_define.cache_events_ts, after_remove.cache_events_ts);
	assert_ne!(after_define.cache_tables_ts, after_remove.cache_tables_ts);
	assert_ne!(after_define.cache_indexes_ts, after_remove.cache_indexes_ts);
	assert_ne!(after_define_live_query_version, after_remove_live_query_version);
	//
	Ok(())
}
