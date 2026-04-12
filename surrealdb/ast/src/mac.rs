/// Macro implementing a bunch of traits for AST nodes
///
/// This macro has some limitation on what kind of type it can match.
/// If the type is an enum it can only have tuple variants with one field.
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
		impl<L> crate::vis::AstVis<L> for $name
		where
			L: crate::types::NodeLibrary,
		{
			fn fmt<W>(&self, ast: &crate::types::Ast<L>, fmt: &mut crate::vis::AstFormatter<W>) -> std::fmt::Result
				where W: std::fmt::Write,
			{
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

		crate::mac::impl_vis_type!{
			$(#[$m])*
			#[derive(Debug)]
			$vis enum $name {
				$(
					$variant($ty),
				)*
			}
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
	};

}
pub(crate) use ast_type;

/// Macro implementing [`crate::vis::AstVist`] for a type if the "visualize" feature is enabled.
///
/// This macro has some limitation on what kind of type it can match.
/// If the type is an enum it can only have:  unit variants i.e. variants without a field, tuple
/// variants with one field, or a struct variant.
macro_rules! impl_vis_type{
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
		$vis struct $name {
            $(
                $(#[$field_m])*
                pub $field: $ty,
            )*
		}

		#[cfg(feature = "visualize")]
		impl<L> crate::vis::AstVis<L> for $name
		where
			L: crate::types::NodeLibrary,
		{
			fn fmt<W>(&self, ast: &crate::types::Ast<L>, fmt: &mut crate::vis::AstFormatter<W>) -> std::fmt::Result
				where W: std::fmt::Write,
			{
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
			$($tt:tt)*
        }
    ) => {

        $(#[$m])*
        $vis enum $name{
			$($tt)*
        }

		#[cfg(feature = "visualize")]
		#[allow(unused_variables)]
		#[allow(irrefutable_let_patterns)]
		impl<L> crate::vis::AstVis<L> for $name
		where
			L: crate::types::NodeLibrary,
		{
			fn fmt<W>(&self, ast: &crate::types::Ast<L>, fmt: &mut crate::vis::AstFormatter<W>) -> std::fmt::Result
				where W: std::fmt::Write,
			{
				fmt.fmt_enum(ast,stringify!($name),|ast,fmt|{
					impl_vis_type!{@variant self,fmt,ast => { $($tt)* }}

				})
			}
		}
	};

	(@variant $this:expr, $fmt:expr, $ast:expr => { }) => {  Ok(())  };
	(@variant $this:expr, $fmt:expr, $ast:expr => { $(#[$fm:meta])* $variant:ident($ty:ty), $($rest:tt)* }) => {
		if let Self::$variant(x) = $this{
			$fmt.variant($ast,stringify!($variant), |ast,fmt|{
				fmt.tuple(ast,x)?.finish()
			})
		}else
		{
			impl_vis_type!(@variant $this,$fmt,$ast => { $($rest)* })
		}
	};
	(@variant $this:expr, $fmt:expr, $ast:expr => { $(#[$fm:meta])* $variant:ident{ $($field:ident: $ty:ty),* $(,)?}, $($rest:tt)* }) => {
		if let Self::$variant{$($field),*} = $this{
			$fmt.variant($ast,stringify!($variant), |ast,fmt|{
				fmt
					$(.field(ast,stringify!($field),$field)?)*
					.finish()
			})
		}else{
		impl_vis_type!(@variant $this,$fmt,$ast => { $($rest)* })
		}
	};
	(@variant $this:expr, $fmt:expr, $ast:expr => { $(#[$fm:meta])* $variant:ident, $($rest:tt)* }) => {
		if let Self::$variant = $this{
			$fmt.unit_variant(stringify!($variant))
		}else{
			impl_vis_type!(@variant $this,$fmt,$ast => { $($rest)* })
		}
	};
}
pub(crate) use impl_vis_type;

macro_rules! impl_vis_debug {
	($t:ty) => {
		#[cfg(feature = "visualize")]
		impl<L> crate::vis::AstVis<L> for $t
		where
			L: $crate::types::NodeLibrary,
		{
			fn fmt<W>(
				&self,
				_ast: &crate::types::Ast<L>,
				fmt: &mut crate::vis::AstFormatter<W>,
			) -> std::fmt::Result
			where
				W: std::fmt::Write,
			{
				fmt.fmt_debug(self)
			}
		}
	};
}
pub(crate) use impl_vis_debug;

/// Debug macro that can pretty format ast types.
#[macro_export]
macro_rules! ast_dbg {
	($ast:expr, $expr:expr) => {{
		let expr = $expr;
		println!(
			"[{}:{}]{} = {}",
			file!(),
			line!(),
			stringify!($expr),
			$crate::vis::AstVis::to_ast_string(&expr, $ast)
		);
		expr
	}};
}
