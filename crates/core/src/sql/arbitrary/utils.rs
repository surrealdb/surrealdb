use arbitrary::{Arbitrary, Unstructured};

pub fn atleast_one<'a, T: Arbitrary<'a>>(
	u: &mut arbitrary::Unstructured<'a>,
) -> arbitrary::Result<Vec<T>> {
	arb_vec1(u, Arbitrary::arbitrary)
}

/// Generates an arbitrary vector with atleast one element generated from the given closure.
pub fn arb_vec1<'a, R, F>(
	u: &mut arbitrary::Unstructured<'a>,
	mut f: F,
) -> arbitrary::Result<Vec<R>>
where
	R: Arbitrary<'a>,
	F: FnMut(&mut Unstructured<'a>) -> arbitrary::Result<R>,
{
	let mut res = vec![f(u)?];
	res.reserve_exact(u.arbitrary_len::<R>()?);
	for _ in 1..res.capacity() {
		res.push(f(u)?);
	}
	Ok(res)
}

/// Generates an arbitrary vector with atleast one element generated from the given closure.
pub fn arb_vec2<'a, R, F>(
	u: &mut arbitrary::Unstructured<'a>,
	mut f: F,
) -> arbitrary::Result<Vec<R>>
where
	R: Arbitrary<'a>,
	F: FnMut(&mut Unstructured<'a>) -> arbitrary::Result<R>,
{
	let mut res = vec![f(u)?, f(u)?];
	res.reserve_exact(u.arbitrary_len::<R>()?);
	for _ in 2..res.capacity() {
		res.push(f(u)?);
	}
	Ok(res)
}

pub fn arb_opt<'a, R, F>(u: &mut arbitrary::Unstructured<'a>, f: F) -> arbitrary::Result<Option<R>>
where
	R: Arbitrary<'a>,
	F: FnOnce(&mut Unstructured<'a>) -> arbitrary::Result<R>,
{
	if u.arbitrary()? {
		Ok(Some(f(u)?))
	} else {
		Ok(None)
	}
}
