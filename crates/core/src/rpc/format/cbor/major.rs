use super::{simple::Simple, tags::Tag};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Major {
    Positive(i64),
    Negative(i64),
    Bytes(u8),
    Text(u8),
    Array(u8),
    Map(u8),
    Tagged(Tag),
    Simple(Simple),
}

impl Major {
    pub fn eq_major(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl From<Major> for u8 {
    fn from(value: Major) -> Self {
        u8::from(&value)
    }
}

impl From<&Major> for u8 {
    fn from(value: &Major) -> Self {
        match value {
            Major::Positive(_) => 0,
            Major::Negative(_) => 1,
            Major::Bytes(_) => 2,
            Major::Text(_) => 3,
            Major::Array(_) => 4,
            Major::Map(_) => 5,
            Major::Tagged(_) => 6,
            Major::Simple(_) => 7,
        }
    }
}

impl std::fmt::Display for Major {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u8::from(self))
    }
}