/// Converts some text into a new line byte string
macro_rules! bytes {
	($expression:expr) => {
		format!("{}\n", $expression).into_bytes()
	};
}

/// Creates a new b-tree map of key-value pairs
macro_rules! map {
    ($($k:expr $(, if let $grant:pat = $check:expr)? $(, if $guard:expr)? => $v:expr),* $(,)? $( => $x:expr )?) => {{
        let mut m = ::std::collections::BTreeMap::new();
    	$(m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)?
		$( $(if let $grant = $check)? $(if $guard)? { m.insert($k, $v); };)+
        m
    }};
}

/// Extends a b-tree map of key-value pairs
macro_rules! mrg {
	($($m:expr, $x:expr)+) => {{
		$($m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)+
		$($m)+
	}};
}

/// Matches on a specific config environment
macro_rules! get_cfg {
	($i:ident : $($s:expr),+) => (
		let $i = || { $( if cfg!($i=$s) { return $s; } );+ "unknown"};
	)
}

/// Runs a method on a transaction, ensuring that the transaction
/// is cancelled and rolled back if the initial function fails.
/// This can be used to ensure that the use of the `?` operator to
/// fail fast and return an error from a function does not leave
/// a transaction in an uncommitted state without rolling back.
macro_rules! catch {
	($txn:ident, $default:expr) => {
		match $default.await {
			Err(e) => {
				let _ = $txn.cancel().await;
				return Err(e);
			}
			Ok(v) => v,
		}
	};
}

/// Runs a method on a transaction, ensuring that the transaction
/// is cancelled and rolled back if the initial function fails, or
/// committed successfully if the initial function succeeds. This
/// can be used to ensure that the use of the `?` operator to fail
/// fast and return an error from a function does not leave a
/// transaction in an uncommitted state without rolling back.
macro_rules! run {
	($txn:ident, $default:expr) => {
		match $default.await {
			Err(e) => {
				let _ = $txn.cancel().await;
				Err(e)
			}
			Ok(v) => match $txn.commit().await {
				Err(e) => {
					let _ = $txn.cancel().await;
					Err(e)
				}
				Ok(_) => Ok(v),
			},
		}
	};
}

/// A macro that allows lazily parsing a value from the environment variable,
/// with a fallback default value if the variable is not set or parsing fails.
///
/// # Parameters
///
/// - `$key`: An expression representing the name of the environment variable.
/// - `$t`: The type of the value to be parsed.
/// - `$default`: The default value to fall back to if the environment variable
///   is not set or parsing fails.
///
/// # Return Value
///
/// A lazy static variable of type `once_cell::sync::Lazy`, which holds the parsed value
/// from the environment variable or the default value.
#[macro_export]
macro_rules! lazy_env_parse {
	($key:expr, $t:ty, $default:expr) => {
		once_cell::sync::Lazy::new(|| {
			std::env::var($key)
				.and_then(|s| Ok(s.parse::<$t>().unwrap_or($default)))
				.unwrap_or($default)
		})
	};
}

/// Lazily parses an environment variable into a specified type. If the environment variable is not set or the parsing fails,
/// it returns a default value.
///
/// # Parameters
///
/// - `$key`: A string literal representing the name of the environment variable.
/// - `$t`: The type to parse the environment variable into.
/// - `$default`: A fallback function or constant value to be returned if the environment variable is not set or the parsing fails.
///
/// # Returns
///
/// A `Lazy` static variable that stores the parsed value or the default value.
#[macro_export]
macro_rules! lazy_env_parse_or_else {
	($key:expr, $t:ty, $default:expr) => {
		once_cell::sync::Lazy::new(|| {
			std::env::var($key)
				.and_then(|s| Ok(s.parse::<$t>().unwrap_or_else($default)))
				.unwrap_or_else($default)
		})
	};
}

#[cfg(test)]
macro_rules! async_defer{
	(let $bind:ident = ($capture:expr) defer { $($d:tt)* } after { $($t:tt)* }) => {
		async {
			async_defer!(@captured);
			async_defer!(@catch_unwind);

			#[allow(unused_mut)]
			let mut v = Some($capture);
			#[allow(unused_mut)]
			let mut $bind = Captured(&mut v);
			let res = CatchUnwindFuture(async { $($t)* }).await;
			#[allow(unused_variables,unused_mut)]
			if let Some(mut $bind) = v.take(){
				async { $($d)* }.await;
			}
			match res{
				Ok(x) => x,
				Err(e) => ::std::panic::resume_unwind(e)
			}

		}
	};

	(defer { $($d:tt)* } after { $($t:tt)* }) => {
		async {
			async_defer!(@catch_unwind);

			let res = CatchUnwindFuture(async { $($t)* }).await;
			#[allow(unused_variables)]
			async { $($d)* }.await;
			match res{
				Ok(x) => x,
				Err(e) => ::std::panic::resume_unwind(e)
			}

		}
	};

	(@captured) => {
		// unwraps are save cause the value can only be taken by consuming captured.
		pub struct Captured<'a,T>(&'a mut Option<T>);
		impl<T> ::std::ops::Deref for Captured<'_,T>{
			type Target = T;

			fn deref(&self) -> &T{
				self.0.as_ref().unwrap()
			}
		}
		impl<T> ::std::ops::DerefMut for Captured<'_,T>{
			fn deref_mut(&mut self) -> &mut T{
				self.0.as_mut().unwrap()
			}
		}
		impl<T> Captured<'_,T>{
			#[allow(dead_code)]
			pub fn take(self) -> T{
				self.0.take().unwrap()
			}
		}
	};

	(@catch_unwind) => {
		struct CatchUnwindFuture<F>(F);
		impl<F,R> ::std::future::Future for CatchUnwindFuture<F>
			where F: ::std::future::Future<Output = R>,
		{
			type Output = ::std::thread::Result<R>;

			fn poll(self: ::std::pin::Pin<&mut Self>, cx: &mut ::std::task::Context) -> ::std::task::Poll<Self::Output>{
				let pin = unsafe{ self.map_unchecked_mut(|x| &mut x.0) };
				match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(||{
					pin.poll(cx)
				})) {
					Ok(x) => x.map(Ok),
					Err(e) => ::std::task::Poll::Ready(Err(e))
				}
			}
		}
	};
}

#[cfg(test)]
mod test {
	#[tokio::test]
	async fn async_defer_basic() {
		let mut counter = 0;

		async_defer!(defer {
			assert_eq!(counter,1);
		} after {
			assert_eq!(counter,0);
			counter += 1;
		})
		.await;

		async_defer!(let t = (()) defer {
			panic!("shouldn't be called");
		} after {
			assert_eq!(counter,1);
			counter += 1;
			t.take();
		})
		.await;
	}

	#[tokio::test]
	#[should_panic(expected = "this is should be the message of the panic")]
	async fn async_defer_panic() {
		let mut counter = 0;

		async_defer!(defer {
			// This should still execute
			assert_eq!(counter,1);
			panic!("this is should be the message of the panic")
		} after {
			assert_eq!(counter,0);
			counter += 1;
			panic!("this panic should be caught")
		})
		.await;
	}
}
