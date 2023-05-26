#[cfg(feature = "kv-mem")]
pub(crate) mod table {
	use crate::err::Error;
	use crate::key::tb;
	use crate::key::tb::Tb;
	use crate::kvs::Datastore;
	use crate::sql::statements::DefineTableStatement;

	struct TestContext {
		db: Datastore,
	}

	async fn init() -> Result<TestContext, Error> {
		let db = Datastore::new("memory").await?;
		return Ok(TestContext {
			db,
		});
	}

	#[tokio::test]
    #[rustfmt::skip]
    async fn expired_nodes_are_garbage_collected() {
        let test = match init().await {
            Ok(test) => test,
            Err(e) => panic!("{}", e),
        };
        
        test.db.cluster_init()
    }
}
