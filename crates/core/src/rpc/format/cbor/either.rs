use super::{
	decode::{Decoder, TryDecode},
	err::Error,
	major::Major,
	types::TypeName,
};

pub enum Either<A, B>
where
	A: TryDecode + TypeName,
	B: TryDecode + TypeName,
{
	A(A),
	B(B),
}

impl<A, B> TryDecode for Either<A, B>
where
	A: TryDecode + TypeName,
	B: TryDecode + TypeName,
{
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error>
	where
		Self: Sized,
	{
		if let Some(x) = A::try_decode(dec, major)? {
			return Ok(Some(Self::A(x)));
		}

		if let Some(x) = B::try_decode(dec, major)? {
			return Ok(Some(Self::B(x)));
		}

		Ok(None)
	}
}

impl<A, B> TypeName for Either<A, B>
where
	A: TryDecode + TypeName,
	B: TryDecode + TypeName,
{
	fn type_name() -> String {
		format!("{} or {}", A::type_name(), B::type_name())
	}
}

pub enum Either6<A, B, C, D, E, F>
where
	A: TryDecode + TypeName,
	B: TryDecode + TypeName,
	C: TryDecode + TypeName,
	D: TryDecode + TypeName,
	E: TryDecode + TypeName,
	F: TryDecode + TypeName,
{
	A(A),
	B(B),
	C(C),
	D(D),
	E(E),
	F(F),
}

impl<A, B, C, D, E, F> TryDecode for Either6<A, B, C, D, E, F>
where
	A: TryDecode + TypeName,
	B: TryDecode + TypeName,
	C: TryDecode + TypeName,
	D: TryDecode + TypeName,
	E: TryDecode + TypeName,
	F: TryDecode + TypeName,
{
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error>
	where
		Self: Sized,
	{
		if let Some(x) = A::try_decode(dec, major)? {
			return Ok(Some(Self::A(x)));
		}

		if let Some(x) = B::try_decode(dec, major)? {
			return Ok(Some(Self::B(x)));
		}

		if let Some(x) = C::try_decode(dec, major)? {
			return Ok(Some(Self::C(x)));
		}

		if let Some(x) = D::try_decode(dec, major)? {
			return Ok(Some(Self::D(x)));
		}

		if let Some(x) = E::try_decode(dec, major)? {
			return Ok(Some(Self::E(x)));
		}

		if let Some(x) = F::try_decode(dec, major)? {
			return Ok(Some(Self::F(x)));
		}

		Ok(None)
	}
}

impl<A, B, C, D, E, F> TypeName for Either6<A, B, C, D, E, F>
where
	A: TryDecode + TypeName,
	B: TryDecode + TypeName,
	C: TryDecode + TypeName,
	D: TryDecode + TypeName,
	E: TryDecode + TypeName,
	F: TryDecode + TypeName,
{
	fn type_name() -> String {
		format!(
			"{}, {}, {}, {}, {} or {}",
			A::type_name(),
			B::type_name(),
			C::type_name(),
			D::type_name(),
			E::type_name(),
			F::type_name()
		)
	}
}
