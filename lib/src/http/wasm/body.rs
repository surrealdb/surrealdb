use bytes::Bytes;

enum Kind {
	Bytes(Bytes),
}

pub struct Body {
	kind: Kind,
}

impl Body {
	pub fn empty() -> Self {
		Self {
			kind: Kind::Bytes(Bytes::new()),
		}
	}
}

impl<B> From<B> for Body
where
	Bytes: From<B>,
{
	fn from(b: B) -> Self {
		Body {
			kind: Kind::Bytes(Bytes::from(b)),
		}
	}
}
