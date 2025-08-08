use crate::val::Value;

pub trait Extractor: Sized {
	fn take<I>(iterator: &mut I) -> Option<Self>
	where
		I: Iterator<Item = Value>;
}

impl Extractor for Value {
	fn take<I>(iterator: &mut I) -> Option<Self>
	where
		I: Iterator<Item = Value>,
	{
		iterator.next()
	}
}

impl Extractor for Option<Value> {
	fn take<I>(iterator: &mut I) -> Option<Self>
	where
		I: Iterator<Item = Value>,
	{
		Some(iterator.next())
	}
}

macro_rules! impl_tuple{
	($($I:ident),*$(,)?) => {
		impl<$($I: Extractor),*> Extractor for ($($I,)*){
			#[allow(non_snake_case)]
			fn take<I>(iterator: &mut I) -> Option<Self>
			where
				I: Iterator<Item = Value>,
			{
				$(
					let $I = $I::take(iterator)?;
				)*

				if iterator.next().is_some(){
					return None
				}

				Some(($($I,)*))
			}
		}
	}
}

impl_tuple!();
impl_tuple!(A);
impl_tuple!(A, B);
impl_tuple!(A, B, C);
impl_tuple!(A, B, C, D);
impl_tuple!(A, B, C, D, E);
impl_tuple!(A, B, C, D, E, F);

pub fn extract_args<E>(args: Vec<Value>) -> Option<E>
where
	E: Extractor,
{
	let mut iter = args.into_iter();
	E::take(&mut iter)
}
