#[allow(unused_macros)]
macro_rules! assert_empty_val {
	($tx:expr, $key:expr) => {{
		let r = $tx.get($key).await?;
		assert!(r.is_none());
	}};
}

#[allow(unused_macros)]
macro_rules! assert_empty_prefix {
	($tx:expr, $rng:expr) => {{
		let r = $tx.getp($rng, 1).await?;
		assert!(r.is_empty());
	}};
}

#[allow(unused_macros)]
macro_rules! assert_empty_range {
	($tx:expr, $rng:expr) => {{
		let r = $tx.getr($rng, 1).await?;
		assert!(r.is_empty());
	}};
}
