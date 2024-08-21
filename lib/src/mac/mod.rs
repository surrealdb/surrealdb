/*
/// Creates a new b-tree map of key-value pairs
macro_rules! map {
	($($k:expr $(, if let $grant:pat = $check:expr)? $(, if $guard:expr)? => $v:expr),* $(,)? $( => $x:expr )?) => {{
		let mut m = ::std::collections::BTreeMap::new();
		$(m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)?
		$( $(if let $grant = $check)? $(if $guard)? { m.insert($k, $v); };)+
		m
	}};
}*/
