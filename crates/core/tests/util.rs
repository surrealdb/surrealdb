#[expect(unused_macros)]
macro_rules! assert_empty_val {
	($tx:expr, $key:expr) => {{
		let r = $tx.get($key).await?;
		assert!(r.is_none());
	}};
}

#[expect(unused_macros)]
macro_rules! assert_empty_prefix {
	($tx:expr, $rng:expr) => {{
		let r = $tx.getp($rng).await?;
		assert!(r.is_empty());
	}};
}

#[expect(unused_macros)]
macro_rules! assert_empty_range {
	($tx:expr, $rng:expr) => {{
		let r = $tx.getr($rng).await?;
		assert!(r.is_empty());
	}};
}
