/// Creates a new [`VecMap`] of key-value pairs.
///
/// Clones items from the optional secondary map first, then applies conditional
/// entries. Duplicate keys keep the last value (same as repeated [`VecMap::insert`]).
/// Uses batch construction so key order in the source does not trigger quadratic cost.
#[macro_export]
macro_rules! map {
    ($($k:expr_2021 $(, if let $grant:pat = $check:expr_2021)? $(, if $guard:expr_2021)? => $v:expr_2021),* $(,)? $( => $x:expr_2021 )?) => {{
        let mut pairs: ::std::vec::Vec<_> = ::std::vec::Vec::new();
    	$(pairs.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)?
		$( $(if let $grant = $check)? $(if $guard)? { pairs.push(($k, $v)); };)+
        pairs.into_iter().collect::<$crate::VecMap<_, _>>()
    }};
}

/// Report a broken invariant and return from the enclosing function.
///
/// Expands to an [`EngineError::unreachable`](crate::err::EngineError::unreachable)
/// carrying the call site, so the message points at the code that noticed
/// rather than at this macro.
macro_rules! fail {
	($($arg:tt)+) => {
		return ::core::result::Result::Err(
			::core::convert::Into::into(
				$crate::err::EngineError::unreachable(::core::format_args!($($arg)*))
			)
		)
	};
}

/// Maps an optional value to a new value if the optional value is some, otherwise returns none.
/// Useful when the computation is async
macro_rules! map_opt {
	($x:ident as $opt:expr => $exp:expr) => {
		match $opt {
			Some($x) => Some($exp),
			None => None,
		}
	};
}

/// Merges two sorted maps into one [`VecMap`]. Values from the second map win when keys overlap.
#[macro_export]
macro_rules! mrg {
	($m:expr_2021, $x:expr_2021) => {{
		let mut pairs: ::std::vec::Vec<_> =
			$m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
		pairs.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));
		pairs.into_iter().collect::<$crate::VecMap<_, _>>()
	}};
}

/// Converts some text into a new line byte string
macro_rules! bytes {
	($expression:expr_2021) => {
		format!("{}\n", $expression).into_bytes()
	};
}

/// Pauses and yields execution to the tokio runtime
macro_rules! yield_now {
	() => {
		if tokio::runtime::Handle::try_current().is_ok() {
			tokio::task::consume_budget().await;
		}
	};
}

/// Runs a method on a transaction, ensuring that the transaction
/// is cancelled and rolled back if the initial function fails.
/// This can be used to ensure that the use of the `?` operator to
/// fail fast and return an error from a function does not leave
/// a transaction in an uncommitted state without rolling back.
macro_rules! catch {
	($txn:ident, $default:expr_2021) => {
		match $default {
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
	($txn:ident, $default:expr_2021) => {
		match $default {
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

#[cfg(test)]
mod test {
	use crate::err::EngineError;

	#[track_caller]
	fn fail_func() -> Result<(), anyhow::Error> {
		fail!("Reached unreachable code");
	}

	#[track_caller]
	fn fail_func_args() -> Result<(), anyhow::Error> {
		fail!("Found {} but expected {}", "test", "other");
	}

	#[test]
	fn fail_literal() {
		let line = line!();
		let Ok(EngineError::Unreachable(msg)) = fail_func().unwrap_err().downcast() else {
			panic!()
		};
		assert_eq!(
			format!("surrealdb/core/src/mac/mod.rs:{}: Reached unreachable code", line + 1),
			msg
		);
	}

	#[test]
	fn fail_call() {
		let line = line!();
		let EngineError::Unreachable(msg) = EngineError::unreachable("Reached unreachable code")
		else {
			panic!()
		};
		assert_eq!(
			format!("surrealdb/core/src/mac/mod.rs:{}: Reached unreachable code", line + 1),
			msg
		);
	}

	#[test]
	fn fail_arguments() {
		let line = line!();
		let Ok(EngineError::Unreachable(msg)) = fail_func_args().unwrap_err().downcast() else {
			panic!()
		};
		assert_eq!(
			format!("surrealdb/core/src/mac/mod.rs:{}: Found test but expected other", line + 1),
			msg
		);
	}
}
