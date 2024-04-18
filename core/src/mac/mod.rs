/// Converts some text into a new line byte string
#[macro_export]
#[doc(hidden)]
macro_rules! bytes {
	($expression:expr) => {
		format!("{}\n", $expression).into_bytes()
	};
}

/// Creates a new b-tree map of key-value pairs
#[macro_export]
#[doc(hidden)]
macro_rules! map {
    ($($k:expr => $v:expr),* $(,)? $( => $x:expr )?) => {{
        let mut m = ::std::collections::BTreeMap::new();
        $(m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)?
        $(m.insert($k, $v);)+
        m
    }};
}

/// Matches on a specific config environment
#[macro_export]
#[doc(hidden)]
macro_rules! get_cfg {
	($i:ident : $($s:expr),+) => (
		let $i = || { $( if cfg!($i=$s) { return $s; } );+ "unknown"};
	)
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
#[macro_export]
macro_rules! async_defer{
	$(let $bind:ident = ($capture:expr) defer { $($d:tt)* } after { $($t:tt)* }) => {
		async {

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
				pub fn take(self) -> T{
					self.0.take().unwrap()
				}
			}

			struct CatchUnwindFuture<F>(F);
			impl<F,R> ::std::future::Future for <F>
				where F: ::std::future::Future<Output = R>,
			{
				type Output = ::std::thread::Result<R>;

				fn poll(self: ::std::pin::Pin<&mut Self>, cx: &mut ::std::task::Context<'_>) -> ::std::task::Poll<Self::Output>{
					let pin = unsafe{ self.map_unchecked_mut(|x| &mut x.0) };
					match ::std::panic::catch_unwind(||{
						pin.poll(cx)
					}) {
						Ok(x) => x.map(Ok),
						Err(e) => Poll::Ready(Err(e))
					}
				}
			}

			let mut v = Some($capture);
			let mut $bind = Captured(&mut v);
			let res = CatchUnwindFuture(async { $($t)* }).await;
			if let Some($bind) = v.take(){
				async { $($d)* }.await;
			}
			match res{
				Ok(x) => x,
				Err(e) => ::std::panic::resume_unwind(e)
			}

		}
	};
}
