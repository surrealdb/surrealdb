use arbitrary::{Arbitrary, Unstructured};
use std::fs::File;
use std::io::Read;
use std::env;
use surrealdb::{dbs::Session, kvs::Datastore, sql::Query};
use surrealdb::engine::remote::ws::Ws;
use surrealdb::Surreal;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <test_case_path>", args[0]);
        std::process::exit(1);
    }

    // TODO: Take flag to indicate if test case is a query string or query object.
    let is_parsed = true;

    let test_case_path = &args[1];
    let mut test_case = File::open(test_case_path).unwrap();

    let query = if is_parsed { 
        let mut buffer = Vec::new();
        test_case.read_to_end(&mut buffer).unwrap();
        let raw_data: &[u8] = &buffer;
        let unstructured: &mut Unstructured = &mut Unstructured::new(raw_data);
        <Query as Arbitrary>::arbitrary(unstructured).unwrap()
    } else {
        let mut query_string = String::new();
        test_case.read_to_string(&mut query_string).unwrap();
        surrealdb::sql::parse(&query_string).unwrap()
    };

    let query_string = format!("{}", query);
    println!("Original query string: {}", query_string);
    
    // Connect to server to reproduce remotely
    let db = Surreal::new::<Ws>("127.0.0.1:8000").await.unwrap();
    db.use_ns("test").use_db("test").await.unwrap();

    // Attempt to crash the parser
    println!("Attempting to remotely parse query string...");
    match db.query(&query_string).await {
        Err(err) => println!("Failed to remotely parse query string: {}", err),
        _ => (),
    };
    println!("Attempting to locally parse query string...");
    match surrealdb::sql::parse(&query_string) {
        Ok(ast) => println!("Parsed query string: {}", ast),
        Err(err) => println!("Failed to locally parse query string: {}", err),
    };
    
    // Attempt to crash the executor 
    println!("Attempting to remotely execute query object...");
    match db.query(query.clone()).await {
        Err(err) => println!("Failed to remotely execute query object: {}", err),
        _ => (),
    };
    println!("Attempting to locally execute query object...");
    let ds = Datastore::new("memory").await.unwrap();
    let ses = Session::owner().with_ns("test").with_db("test");
    match ds.process(query, &ses, None).await {
        Err(err) => println!("Failed to locally execute query object: {}", err),
        _ => (),
    };
}
