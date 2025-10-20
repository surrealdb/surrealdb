use anyhow::Result;
use surrealdb_types::SurrealValue;

pub trait Args: Sized {
	fn to_values(self) -> Vec<surrealdb_types::Value>;
	fn from_values(values: Vec<surrealdb_types::Value>) -> Result<Self>;
	fn kinds() -> Vec<surrealdb_types::Kind>;
}

macro_rules! impl_args {
    ($($len:literal => ($($name:ident),+)),+ $(,)?) => {
        $(
            impl<$($name),+> Args for ($($name,)+)
            where
                $($name: SurrealValue),+
            {
                fn to_values(self) -> Vec<surrealdb_types::Value> {
                    #[allow(non_snake_case)]
                    let ($($name,)+) = self;
                    vec![
                        $($name.into_value(),)+
                    ]
                }

                fn from_values(values: Vec<surrealdb_types::Value>) -> Result<Self> {
                    if values.len() != $len {
                        return Err(anyhow::anyhow!("Expected ({}), found other arguments", Self::kinds().iter().map(|k| k.to_string()).collect::<Vec<String>>().join(", ")));
                    }

                    let mut values = values;

                    $(#[allow(non_snake_case)] let $name = values.remove(0);)+

                    Ok(($($name::from_value($name)?,)+))
                }

                fn kinds() -> Vec<surrealdb_types::Kind> {
                    vec![
                        $($name::kind_of(),)+
                    ]
                }
            }
        )+
    };
}

impl_args! {
	1 => (A),
	2 => (A, B),
	3 => (A, B, C),
	4 => (A, B, C, D),
	5 => (A, B, C, D, E),
	6 => (A, B, C, D, E, F),
	7 => (A, B, C, D, E, F, G),
	8 => (A, Bq, C, D, E, F, G, H),
	9 => (A, B, C, D, E, F, G, H, I),
	10 => (A, B, C, D, E, F, G, H, I, J),
}

// Empty impl
impl Args for () {
	fn to_values(self) -> Vec<surrealdb_types::Value> {
		Vec::new()
	}

	fn from_values(values: Vec<surrealdb_types::Value>) -> Result<Self> {
		if !values.is_empty() {
			return Err(anyhow::anyhow!(
				"Expected ({}), found other arguments",
				Self::kinds().iter().map(|k| k.to_string()).collect::<Vec<String>>().join(", ")
			));
		}

		Ok(())
	}

	fn kinds() -> Vec<surrealdb_types::Kind> {
		Vec::new()
	}
}

impl<T> Args for Vec<T>
where
	T: SurrealValue,
{
	fn to_values(self) -> Vec<surrealdb_types::Value> {
		self.into_iter().map(|x| x.into_value()).collect()
	}

	fn from_values(values: Vec<surrealdb_types::Value>) -> Result<Self> {
		Ok(values.into_iter().map(|x| T::from_value(x)).collect::<Result<Vec<T>>>()?.into())
	}

	// This implementation is only used to dynamically transfer arguments, not to annotate them
	fn kinds() -> Vec<surrealdb_types::Kind> {
		vec![T::kind_of()]
	}
}
