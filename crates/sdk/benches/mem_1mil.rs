use divan::AllocProfiler;
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use tokio::runtime::Runtime;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
	divan::main();
}

#[divan::bench(sample_count = 1, sample_size = 1)]
fn runner() {
	let rt = Runtime::new().unwrap();
	rt.block_on(mem_bench());
}

async fn mem_bench() {
	let dbs = Datastore::new("memory").await.unwrap();
	let ses = Session::owner().with_ns("bench").with_db("bench");
	let sql = r"
    USE NS test DB test;

    DEFINE FUNCTION OVERWRITE fn::ingest() {
        CREATE |person:1000000| CONTENT {
            name: rand::string(7, 15),
            details: [
                rand::string(10),
                rand::string(10),
            ],
            colours: [
                rand::string(10),
                rand::string(10),
            ],
        };
    };

    fn::ingest();
    "
	.to_owned();
	let _res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
}
