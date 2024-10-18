use js::{CatchResultExt, CaughtError};

macro_rules! create_test_context{
	($ctx:ident => { $($t:tt)* }) => {
		let rt = js::AsyncRuntime::new().unwrap();
		let ctx = js::AsyncContext::full(&rt).await.unwrap();

		js::async_with!(ctx => |$ctx|{
			super::register(&$ctx).unwrap();
			$($t)*
		}).await;
	};
}

#[tokio::test]
async fn fetch() {
	create_test_context!(ctx => {
		let res = ctx.eval::<(),_>("fetch()").catch(&ctx);
		match res{
			Ok(_) => panic!("didn't return an error when it should"),
			Err(CaughtError::Exception(e)) => {
				let message = e.message().expect("exception didn't have a message");
				assert_eq!(message,"The 'fetch' function is not available in this build of SurrealDB. In order to use 'fetch', enable the 'http' feature.");
			}
			Err(_) => panic!("didn't return an exception"),
		}
	});
}

#[tokio::test]
async fn request() {
	create_test_context!(ctx => {
		let res = ctx.eval::<(),_>("new Request('http://a',{ body: 'test' })").catch(&ctx);
		match res{
			Ok(_) => panic!("didn't return an error when it should"),
			Err(CaughtError::Exception(e)) => {
				let message = e.message().expect("exception didn't have a message");
				assert_eq!(message,"The 'Request' class is not available in this build of SurrealDB. In order to use 'Request', enable the 'http' feature.");
			}
			Err(_) => panic!("didn't return an exception"),
		}
	});
}

#[tokio::test]
async fn response() {
	create_test_context!(ctx => {
		let res = ctx.eval::<(),_>("new Response('test')").catch(&ctx);
		match res{
			Ok(_) => panic!("didn't return an error when it should"),
			Err(CaughtError::Exception(e)) => {
				let message = e.message().expect("exception didn't have a message");
				assert_eq!(message,"The 'Response' class is not available in this build of SurrealDB. In order to use 'Response', enable the 'http' feature.");
			}
			Err(_) => panic!("didn't return an exception"),
		}
	});
}

#[tokio::test]
async fn headers() {
	create_test_context!(ctx => {
		let res = ctx.eval::<(),_>("new Headers({ foo: 'bar'})").catch(&ctx);
		match res{
			Ok(_) => panic!("didn't return an error when it should"),
			Err(CaughtError::Exception(e)) => {
				let message = e.message().expect("exception didn't have a message");
				assert_eq!(message,"The 'Headers' class is not available in this build of SurrealDB. In order to use 'Headers', enable the 'http' feature.");
			}
			Err(_) => panic!("didn't return an exception"),
		}
	});
}

#[tokio::test]
async fn blob() {
	create_test_context!(ctx => {
		let res = ctx.eval::<(),_>("new Blob()").catch(&ctx);
		match res{
			Ok(_) => panic!("didn't return an error when it should"),
			Err(CaughtError::Exception(e)) => {
				let message = e.message().expect("exception didn't have a message");
				assert_eq!(message,"The 'Blob' class is not available in this build of SurrealDB. In order to use 'Blob', enable the 'http' feature.");
			}
			Err(_) => panic!("didn't return an exception"),
		}
	});
}

#[tokio::test]
async fn form_data() {
	create_test_context!(ctx => {
		let res = ctx.eval::<(),_>("new FormData()").catch(&ctx);
		match res{
			Ok(_) => panic!("didn't return an error when it should"),
			Err(CaughtError::Exception(e)) => {
				let message = e.message().expect("exception didn't have a message");
				assert_eq!(message,"The 'FormData' class is not available in this build of SurrealDB. In order to use 'FormData', enable the 'http' feature.");
			}
			Err(_) => panic!("didn't return an exception"),
		}
	});
}
