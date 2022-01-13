macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        let mut m = ::std::collections::BTreeMap::new();
        $(m.insert($k, $v);)+
        m
     }};
}

macro_rules! hmap {
    ($($k:expr => $v:expr),* $(,)?) => {{
        let mut m = ::std::collections::HashMap::new();
        $(m.insert($k, $v);)+
        m
     }};
}

macro_rules! bmap {
    ($($k:expr => $v:expr),* $(,)?) => {{
        let mut m = ::std::collections::BTreeMap::new();
        $(m.insert($k, $v);)+
        m
     }};
}
