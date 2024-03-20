macro_rules! map {
    ($($k:expr => $v:expr),* $(,)? $( => $x:expr )?) => {{
        let mut m = ::std::collections::BTreeMap::new();
        $(m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)?
        $(m.insert($k, $v);)+
        m
    }};
}
