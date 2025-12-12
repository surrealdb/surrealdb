// SDK2 integration tests

use sdk2::{auth::AccessRecordAuth, Surreal};
use surrealdb_core::embedded::EmbeddedSurrealEngine;
use surrealdb_types::{object, RecordId, SurrealValue};

async fn setup<T: Into<String>>(setup: T) -> Surreal {
    let surreal = Surreal::new().attach_engine::<EmbeddedSurrealEngine>();
    surreal.connect("memory://").await.unwrap();
    surreal.use_ns("test").use_db("test").await.unwrap();
    surreal.query(setup).await.unwrap();
    surreal
}

#[tokio::test]
async fn test_simple_auth() {
    #[derive(Debug, SurrealValue)]
    struct User {
        id: RecordId,
        email: String,
        pass: String,
    }

	let surreal = setup(r"
        DEFINE ACCESS user ON DATABASE TYPE RECORD
            SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
            SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
            DURATION FOR SESSION 60s, FOR TOKEN 1d;
    ").await;

    let sess = surreal.fork_session().await.unwrap();
    let tokens = sess.signup(AccessRecordAuth {
        namespace: "test".to_string(),
        database: "test".to_string(),
        access: "user".to_string(),
        params: object! {
            email: "test@test.com",
            pass: "test",
        }.into(),
    }).await.unwrap();

    let user: User = surreal.query("SELECT * FROM ONLY user LIMIT 1").await.unwrap().first().unwrap().clone().into_t().unwrap();
    assert_eq!(user.email, "test@test.com");
    assert_eq!(user.id.table.to_string(), "user");
}

