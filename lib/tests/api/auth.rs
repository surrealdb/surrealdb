// Auth tests
// Supported by both HTTP and WS protocols

#[tokio::test]
async fn invalidate() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	db.invalidate().await.unwrap();
	let error = db.create::<Option<RecordId>>(("user", "john")).await.unwrap_err();
	assert!(error.to_string().contains("You don't have permission to perform this query type"));
}

#[tokio::test]
async fn signup_scope() {
	let db = new_db().await;
	let database = Ulid::new().to_string();
	db.use_ns(NS).use_db(&database).await.unwrap();
	let scope = Ulid::new().to_string();
	let sql = format!(
		"
        DEFINE SCOPE {scope} SESSION 1s
        SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
        SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
    "
	);
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	db.signup(Scope {
		namespace: NS,
		database: &database,
		scope: &scope,
		params: AuthParams {
			email: "john.doe@example.com",
			pass: "password123",
		},
	})
	.await
	.unwrap();
}

#[tokio::test]
async fn signin_ns() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let user = Ulid::new().to_string();
	let pass = "password123";
	let sql = format!("DEFINE LOGIN {user} ON NAMESPACE PASSWORD '{pass}'");
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	db.signin(Namespace {
		namespace: NS,
		username: &user,
		password: pass,
	})
	.await
	.unwrap();
}

#[tokio::test]
async fn signin_db() {
	let db = new_db().await;
	let database = Ulid::new().to_string();
	db.use_ns(NS).use_db(&database).await.unwrap();
	let user = Ulid::new().to_string();
	let pass = "password123";
	let sql = format!("DEFINE LOGIN {user} ON DATABASE PASSWORD '{pass}'");
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	db.signin(Database {
		namespace: NS,
		database: &database,
		username: &user,
		password: pass,
	})
	.await
	.unwrap();
}

#[tokio::test]
async fn signin_scope() {
	let db = new_db().await;
	let database = Ulid::new().to_string();
	db.use_ns(NS).use_db(&database).await.unwrap();
	let scope = Ulid::new().to_string();
	let email = format!("{scope}@example.com");
	let pass = "password123";
	let sql = format!(
		"
        DEFINE SCOPE {scope} SESSION 1s
        SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
        SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
    "
	);
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	db.signup(Scope {
		namespace: NS,
		database: &database,
		scope: &scope,
		params: AuthParams {
			pass,
			email: &email,
		},
	})
	.await
	.unwrap();
	db.signin(Scope {
		namespace: NS,
		database: &database,
		scope: &scope,
		params: AuthParams {
			pass,
			email: &email,
		},
	})
	.await
	.unwrap();
}

#[tokio::test]
async fn authenticate() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let user = Ulid::new().to_string();
	let pass = "password123";
	let sql = format!("DEFINE LOGIN {user} ON NAMESPACE PASSWORD '{pass}'");
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	let token = db
		.signin(Namespace {
			namespace: NS,
			username: &user,
			password: pass,
		})
		.await
		.unwrap();
	db.authenticate(token).await.unwrap();
}
