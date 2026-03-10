use crate::{Error, Kind, SurrealValue, Value, union_conversion_error};

// Either of 2, 3, 4, 5, 6 or 7
macro_rules! impl_arg_either {
    ($($enum:ident => $len:literal => ($($name:ident),+)),+ $(,)?) => {
        $(
            /// Create an either of the given types
            #[derive(Debug, Clone, PartialEq, Eq)]
            pub enum $enum<$($name: SurrealValue,)+> {
                $(
                    /// A value of the given type
                    $name($name),
                )+
            }

            impl<$($name: SurrealValue),+> SurrealValue for $enum<$($name,)+>
            {
                fn is_value(value: &Value) -> bool {
                    $($name::is_value(value) ||)+ false
                }

                fn from_value(value: Value) -> Result<Self, Error> {
                    $(if $name::is_value(&value) {
                        return Ok($enum::$name($name::from_value(value)?));
                    })+

                    Err(union_conversion_error(Self::kind_of(), value))
                }

                fn into_value(self) -> Value {
                    match self {
                        $($enum::$name(val) => val.into_value(),)+
                    }
                }

                fn kind_of() -> Kind {
                    Kind::Either(vec![
                        $($name::kind_of(),)+
                    ])
                }
            }
        )+
    };
}

impl_arg_either! {
	Either2 => 2 => (A, B),
	Either3 => 2 => (A, B, C),
	Either4 => 2 => (A, B, C, D),
	Either5 => 2 => (A, B, C, D, E),
	Either6 => 2 => (A, B, C, D, E, F),
	Either7 => 2 => (A, B, C, D, E, F, G),
	Either8 => 2 => (A, B, C, D, E, F, G, H),
	Either9 => 2 => (A, B, C, D, E, F, G, H, I),
	Either10 => 2 => (A, B, C, D, E, F, G, H, I, J),
}
