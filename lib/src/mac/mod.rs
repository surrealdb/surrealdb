macro_rules! output {
	($expression:expr) => {
		bytes::Bytes::from(format!("{}\n", $expression))
	};
}

macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        let mut m = ::std::collections::BTreeMap::new();
        $(m.insert($k, $v);)+
        m
    }};
}
