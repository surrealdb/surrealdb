mod helpers;
use helpers::*;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;

#[tokio::test]
async fn rebuild_index_statement() -> Result<()> {
	let sql = "
		CREATE book:1 SET title = 'Rust Web Programming', isbn = '978-1803234694', author = 'Maxwell Flitton';
		DEFINE INDEX uniq_isbn ON book FIELDS isbn UNIQUE;
		REBUILD INDEX IF EXISTS uniq_isbn ON book;
		INFO FOR TABLE book;
		REBUILD INDEX IF EXISTS idx_author ON book;
		REBUILD INDEX IF EXISTS ft_title ON book;
		DEFINE INDEX idx_author ON book FIELDS author;
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX ft_title ON book FIELDS title SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		REBUILD INDEX uniq_isbn ON book;
		REBUILD INDEX idx_author ON book;
		REBUILD INDEX ft_title ON book;
		INFO FOR TABLE book;
        SELECT * FROM book WHERE title @@ 'Rust';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 14);
	for i in 0..3 {
		let tmp = res.remove(0).result;
		tmp.unwrap_or_else(|_| panic!("{i}"));
	}
	// Check infos output
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
				events: {},
				fields: {},
				indexes: {
					uniq_isbn: 'DEFINE INDEX uniq_isbn ON book FIELDS isbn UNIQUE'
				},
				lives: {},
				tables: {}
			}",
	)
	.unwrap();
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	for _ in 0..8 {
		let tmp = res.remove(0).result;
		tmp.unwrap();
	}
	// Check infos output
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
				events: {},
				fields: {},
				indexes: {
					ft_title: 'DEFINE INDEX ft_title ON book FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) DOC_IDS_ORDER 100 DOC_LENGTHS_ORDER 100 POSTINGS_ORDER 100 TERMS_ORDER 100 DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 100 POSTINGS_CACHE 100 TERMS_CACHE 100 HIGHLIGHTS',
					idx_author: 'DEFINE INDEX idx_author ON book FIELDS author',
					uniq_isbn: 'DEFINE INDEX uniq_isbn ON book FIELDS isbn UNIQUE'
				},
				lives: {},
				tables: {}
			}",
	).unwrap();
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	// Check record is found
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					author: 'Maxwell Flitton',
					id: book:1,
					isbn: '978-1803234694',
					title: 'Rust Web Programming'
				}
			]",
	)
	.unwrap();
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	Ok(())
}
