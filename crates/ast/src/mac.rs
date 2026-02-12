macro_rules! ast_type {
	(
        $(#[$m:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_m:meta])*
                pub $field:ident: $ty:ty
            ),*$(,)?
        }


    ) => {

        $(#[$m])*
		#[derive(Debug)]
		$vis struct $name {
            $(
                $(#[$field_m])*
                pub $field: $ty,
            )*
				pub span: Span
		}

		impl crate::types::Node for $name {}

		impl crate::types::AstSpan for $name{
			fn ast_span<L: crate::types::NodeLibrary>(&self, _: &crate::types::Ast<L>) -> Span {
				self.span
			}
		}

		#[cfg(feature = "visualize")]
		impl<L, W> crate::vis::AstVis<L, W> for $name
		where
			L: crate::types::NodeLibrary,
			W: std::fmt::Write,
		{
			fn fmt(&self, ast: &crate::types::Ast<L>, fmt: &mut crate::vis::AstFormatter<W>) -> std::fmt::Result {
				fmt.fmt_struct(ast,stringify!($name),|ast,fmt|{
					fmt
						$(
							.field(ast,stringify!($field),&self.$field)?
						)*
					;
					Ok(())
				})
			}
		}

	};
	(
        $(#[$m:meta])*
        $vis:vis enum $name:ident {
            $(
                $variant:ident($ty:ty)
            ),*$(,)?
        }


    ) => {

        $(#[$m])*
		#[derive(Debug)]
		$vis enum $name {
            $(
                $variant($ty),
            )*
		}

		impl crate::types::Node for $name {}

		impl crate::types::AstSpan for $name{
			fn ast_span<L: crate::types::NodeLibrary>(&self, ast: &crate::types::Ast<L>) -> Span {
				match self{
					$(
						Self::$variant(x) => x.ast_span(ast),
					)*
				}
			}
		}

		#[cfg(feature = "visualize")]
		impl<L, W> crate::vis::AstVis<L, W> for $name
		where
			L: crate::types::NodeLibrary,
			W: std::fmt::Write,
		{
			fn fmt(&self, ast: &crate::types::Ast<L>, fmt: &mut crate::vis::AstFormatter<W>) -> std::fmt::Result {
				fmt.fmt_enum(ast,stringify!($name),|ast,fmt|{
					match self{
						$(
							Self::$variant(x) => fmt.variant(ast,stringify!($variant), |ast,fmt|{
								fmt.tuple(ast,x)?.finish()
							}),
						)*
					}
				})
			}
		}
	};

}
pub(crate) use ast_type;

macro_rules! impl_vis_debug {
	($t:ty) => {
		#[cfg(feature = "visualize")]
		impl<L, W> AstVis<L, W> for $t
		where
			L: $crate::types::NodeLibrary,
			W: ::std::fmt::Write,
		{
			fn fmt(
				&self,
				_: &$crate::types::Ast<L>,
				fmt: &mut $crate::vis::AstFormatter<W>,
			) -> std::fmt::Result {
				fmt.fmt_debug(self)
			}
		}
	};
}
pub(crate) use impl_vis_debug;
