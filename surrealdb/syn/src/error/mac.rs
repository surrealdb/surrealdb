/// Macro to create an parser error.
///
/// This creates an error with a message first and then a number of spans,
/// possibly with a label.
///
/// # Example
///
/// ```ignore
/// let text = "As we all know 1+1 is 3.";
/// if text.contains("3"){
///     let span = Span::empty(); // just imagine there is an actual span here.
///     error!("1 + 1 should be {}",1+1, @span => "your wrong here!");
///     // This will return the following error when rendered:
///     // Error: 1 + 1 should be 2
///     //   |
///     // 1 | As we all know 1+1 is 3.
///     //   |                       ^ your wrong here!
/// }
/// ```
#[macro_export]
macro_rules! syntax_error {
	($format:literal $(, $expr:expr_2021)*
		$(, @ $span:expr_2021 $(=> $label_format:literal $(, $label_expr:expr_2021)* $(,)? )? )*
	) => {{
		let __error: $crate::error::SyntaxError = $crate::error::SyntaxError::new(format_args!($format $(, $expr)*));
		$(
			$crate::error::syntax_error!(#label __error, $span $(=> $label_format$(, $label_expr)*  )?);
		)*
		__error
	}};

	(#label $name:ident, $span:expr_2021 => $label_format:literal $(, $label_expr:expr_2021)* ) => {
		let $name = $name.with_labeled_span($span,$crate::error::MessageKind::Error, format_args!($label_format $(, $label_expr)*));
	};

	(#label $name:ident, $span:expr_2021 ) => {
	    let $name = $crate::error::SyntaxError::with_span($name,$span, $crate::error::MessageKind::Error);
	};
}

/// Similar to [`syntax_error`] but immediately returns the error.
///
/// Macro to create and return a parser error.
///
/// This creates an error with a message first and then a number of spans,
/// possibly with a label.
///
/// # Example
///
/// ```ignore
/// let text = "As we all know 1+1 is 3.";
/// if text.contains("3"){
///     let span = Span::empty(); // just imagine there is an actual span here.
///     bail!("1 + 1 should be {}",1+1, @span => "your wrong here!");
///     // This will return the following error when rendered:
///     // Error: 1 + 1 should be 2
///     //   |
///     // 1 | As we all know 1+1 is 3.
///     //   |                       ^ your wrong here!
/// }
/// ```
#[macro_export]
macro_rules! bail {
	($($t:tt)*) => {{
		let __error = $crate::error::syntax_error!($($t)*);
		return Err(__error)
	}};
}

pub use crate::{bail, syntax_error};
