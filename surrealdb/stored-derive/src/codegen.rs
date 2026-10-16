//! Code generation: from a checked [`Model`] to the `StoredField` and `StoredType` impls.
//!
//! Everything the expansion names lives under `::surrealdb_expr::val::stored::schema`, so the
//! generated code reads and writes through the runtime's helpers and carries no logic of its own
//! beyond the order and numbers the attribute table declares.

use std::cmp::Reverse;

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{Ident, Index, Member, Path, Type};

use crate::model::{Body, FIRST, Field, FieldSet, Model, Payload, Presence, Rel, Variant};

fn schema() -> TokenStream {
	quote!(::surrealdb_expr::val::stored::schema)
}

fn bind(i: usize) -> Ident {
	format_ident!("__f{i}")
}

fn out(i: usize) -> Ident {
	format_ident!("__v{i}")
}

fn rel(r: Rel) -> TokenStream {
	let s = schema();
	let (major, minor) = (r.0, r.1);
	quote!(#s::Release::new(#major, #minor))
}

/// A fixed-size list of numbers as a pattern: `2 | 4 | 7`.
fn number_pattern(numbers: &[u32]) -> TokenStream {
	if numbers.is_empty() {
		quote!(_ if false)
	} else {
		quote!(#(#numbers)|*)
	}
}

pub(crate) fn expand(model: &Model) -> TokenStream {
	let s = schema();
	let ident = &model.ident;
	let name = ident.to_string();
	let since = rel(model.attrs.since);
	let Parts {
		encode,
		decode,
		floor,
		unrev,
		vm_into,
		vm_from,
		type_name,
		collect,
		shape,
	} = match &model.body {
		Body::Struct(set) => struct_parts(model, set),
		Body::Transparent(ty) => transparent_parts(model, ty),
		Body::Enum {
			variants,
			..
		} => enum_parts(model, variants),
	};
	// Every constructor of the type ends in its validator, so no decode or VM conversion yields a
	// value the type's invariants refuse.
	let decode = validated(model.attrs.validate.as_ref(), decode);
	let vm_from = validated(model.attrs.validate.as_ref(), vm_from);
	let floor_fixed = model.attrs.floor_fixed;
	let embedded = model.attrs.revision_embedded;
	let reserved = match &model.body {
		Body::Struct(set) => set.reserved.clone(),
		Body::Transparent(_) => Vec::new(),
		Body::Enum {
			reserved,
			..
		} => reserved.clone(),
	};
	quote! {
		const _: () = {
			#[automatically_derived]
			#[allow(clippy::absolute_paths)]
			impl #s::StoredField for #ident {
				fn encode_item(
					&self,
					enc: &mut ::surrealdb_expr::val::stored::Encoder,
					prior: ::core::option::Option<::surrealdb_expr::val::stored::Item<'_>>,
				) -> ::core::result::Result<(), #s::SchemaError> {
					#encode
				}

				fn decode_item(
					item: ::surrealdb_expr::val::stored::Item<'_>,
					depth: u32,
				) -> ::core::result::Result<Self, #s::SchemaError> {
					#decode
				}

				fn floor(
					&self,
					acc: &mut #s::Floor,
				) -> ::core::result::Result<(), #s::SchemaError> {
					#floor
				}

				fn unrevisioned_floor(&self) -> ::core::option::Option<#s::Release> {
					#unrev
				}

				fn into_vm_value(
					self,
				) -> ::core::result::Result<::surrealdb_expr::val::Value, #s::SchemaError> {
					#vm_into
				}

				fn from_vm_value(
					value: ::surrealdb_expr::val::Value,
				) -> ::core::result::Result<Self, #s::SchemaError> {
					#vm_from
				}

				fn write_type_name(out: &mut ::std::string::String) {
					#type_name
				}

				fn collect_schemas(
					out: &mut ::std::vec::Vec<&'static #s::TypeSchema>,
				) {
					#collect
				}
			}

			#[automatically_derived]
			#[allow(clippy::absolute_paths)]
			impl #s::StoredType for #ident {
				const SCHEMA: #s::TypeSchema = #s::TypeSchema {
					name: #name,
					since: #since,
					reserved: &[#(#reserved),*],
					embedded_in_revision: #embedded,
					floor_fixed: #floor_fixed,
					shape: #shape,
				};
			}
		};
	}
}

/// Wraps a constructor, an expression of type `Result<Self, SchemaError>`, so the value it builds
/// passes the type's validator before it is returned. The constructed value is bound with an
/// explicit type so the `?` on it has a known error type.
fn validated(validate: Option<&Path>, construct: TokenStream) -> TokenStream {
	let Some(path) = validate else {
		return construct;
	};
	let s = schema();
	quote! {
		let __built: ::core::result::Result<Self, #s::SchemaError> = { #construct };
		let __checked = __built?;
		#path(&__checked)?;
		::core::result::Result::Ok(__checked)
	}
}

/// The method bodies and the shape of one derived impl.
struct Parts {
	encode: TokenStream,
	decode: TokenStream,
	floor: TokenStream,
	unrev: TokenStream,
	vm_into: TokenStream,
	vm_from: TokenStream,
	type_name: TokenStream,
	collect: TokenStream,
	shape: TokenStream,
}

/// Every field type of a set, for `collect_schemas`.
fn collect_fields(types: impl Iterator<Item = Type>) -> TokenStream {
	let s = schema();
	let types: Vec<Type> = types.collect();
	quote! {
		#( <#types as #s::StoredField>::collect_schemas(out); )*
	}
}

/// Registers the type's own schema once, then those of what it holds.
fn collect_with(model: &Model, held: &TokenStream) -> TokenStream {
	let s = schema();
	let ident = &model.ident;
	let name = ident.to_string();
	quote! {
		let this: &'static #s::TypeSchema = &<#ident as #s::StoredType>::SCHEMA;
		if out.iter().any(|schema| schema.name == #name) {
			return;
		}
		out.push(this);
		#held
	}
}

fn struct_parts(model: &Model, set: &FieldSet) -> Parts {
	let s = schema();
	let ty = model.ident.to_string();
	let members: Vec<&Member> = set.fields.iter().map(|f| &f.member).collect();
	let binds: Vec<Ident> = (0..set.fields.len()).map(bind).collect();
	let pattern = quote!(Self { #(#members: #binds),* });
	let since = model.attrs.since;

	let writer = encode_set(&ty, set, &quote!(prior), &quote!(enc));
	let encode = quote! {
		let #pattern = self;
		#writer
		::core::result::Result::Ok(())
	};
	let decode = {
		let build = |vals: &[Ident]| quote!(Self { #(#members: #vals),* });
		let body = decode_set(&ty, set, &quote!(item), &quote!(depth), &build);
		quote!(::core::result::Result::Ok(#body))
	};
	let raise = (since > FIRST).then(|| {
		let r = rel(since);
		quote!(acc.raise(#r);)
	});
	let floor_body = floor_set(set, since);
	let floor = quote! {
		#raise
		let #pattern = self;
		#floor_body
		::core::result::Result::Ok(())
	};
	let unrev_body = unrev_set(set);
	let unrev = quote! {
		let #pattern = self;
		#unrev_body
	};
	let vm_into = {
		let vm = vm_into_set(set);
		quote! {
			let #pattern = self;
			::core::result::Result::Ok(#vm)
		}
	};
	let vm_from = {
		let build = |vals: &[Ident]| quote!(Self { #(#members: #vals),* });
		let body = vm_from_set(&ty, set, &quote!(value), &build);
		quote!(::core::result::Result::Ok(#body))
	};
	let type_name = quote!(out.push_str(#ty););
	let collect = collect_with(model, &collect_fields(set.fields.iter().map(|f| f.ty.clone())));
	let fields = field_schemas(set);
	let shape = quote!(#s::Shape::Struct(&[#(#fields),*]));
	Parts {
		encode,
		decode,
		floor,
		unrev,
		vm_into,
		vm_from,
		type_name,
		collect,
		shape,
	}
}

fn transparent_parts(model: &Model, inner: &Type) -> Parts {
	let s = schema();
	let encode = quote! {
		<#inner as #s::StoredField>::encode_item(&self.0, enc, prior)
	};
	let decode = quote! {
		<#inner as #s::StoredField>::decode_item(item, depth).map(Self)
	};
	let floor = quote! {
		<#inner as #s::StoredField>::floor(&self.0, acc)
	};
	let unrev = quote! {
		<#inner as #s::StoredField>::unrevisioned_floor(&self.0)
	};
	let vm_into = quote! {
		<#inner as #s::StoredField>::into_vm_value(self.0)
	};
	let vm_from = quote! {
		<#inner as #s::StoredField>::from_vm_value(value).map(Self)
	};
	let type_name = quote!(<#inner as #s::StoredField>::write_type_name(out););
	let collect = collect_with(model, &quote!(<#inner as #s::StoredField>::collect_schemas(out);));
	let shape = quote!(#s::Shape::Transparent(<#inner as #s::StoredField>::write_type_name));
	Parts {
		encode,
		decode,
		floor,
		unrev,
		vm_into,
		vm_from,
		type_name,
		collect,
		shape,
	}
}

// ---------------------------------------------------------------------------------------------
// Field sets: a struct's fields, a struct variant's fields, a positional variant's elements.
// ---------------------------------------------------------------------------------------------

/// Whether the encoder writes the field, as an expression over the bound reference `v`.
fn written(field: &Field, v: &Ident) -> TokenStream {
	let ty = &field.ty;
	match &field.presence {
		Presence::Required => quote!(true),
		Presence::Opt(_) => {
			let opt = opt_ref(field, v);
			quote!(#opt.is_some())
		}
		Presence::Default(path) => {
			quote!({
				let default: #ty = (#path)();
				*#v != default
			})
		}
	}
}

/// The `Option` a field of the `Opt` kind holds, as a reference.
fn opt_ref(field: &Field, v: &Ident) -> TokenStream {
	if field.unrevisioned {
		quote!((&#v.0))
	} else {
		quote!(#v)
	}
}

/// A positional variant's elements as a set of required critical fields numbered 2, 4, 6, …
fn positional_set(types: &[Type], since: Rel) -> FieldSet {
	let fields = types
		.iter()
		.enumerate()
		.map(|(i, ty)| Field {
			member: Member::Unnamed(Index::from(i)),
			name: i.to_string(),
			ty: ty.clone(),
			n: 2 * (i as u32 + 1),
			since,
			ignorable: false,
			independent: None,
			presence: Presence::Required,
			unrevisioned: false,
			supersedes: None,
			span: Span::call_site(),
		})
		.collect();
	FieldSet {
		fields,
		reserved: Vec::new(),
	}
}

/// Statements that write the set as a MAP into `enc`, keeping the unknown ignorable entries of
/// `prior`. Fields are bound as references `__f0, __f1, …`.
fn encode_set(ty: &str, set: &FieldSet, prior: &TokenStream, enc: &TokenStream) -> TokenStream {
	let s = schema();
	let ours = {
		let mut numbers = set.known();
		numbers.extend(set.reserved.iter().copied());
		numbers.sort_unstable();
		numbers.dedup();
		let pat = number_pattern(&numbers);
		quote!(|n: u32| matches!(n, #pat))
	};

	// A superseding field decides between its two numbers once, before the walk.
	let mut prepare = Vec::new();
	for (i, f) in set.fields.iter().enumerate() {
		if let Some(sup) = &f.supersedes {
			let v = bind(i);
			let w = format_ident!("__w{i}");
			let narrow = format_ident!("__s{i}");
			let cond = written(f, &v);
			let path = &sup.narrow;
			prepare.push(quote! {
				let #w: bool = #cond;
				let #narrow = if #w { (#path)(#v) } else { ::core::option::Option::None };
			});
		}
	}

	struct Slot<'f> {
		n: u32,
		index: usize,
		field: &'f Field,
		old: bool,
	}
	let mut slots: Vec<Slot<'_>> = Vec::new();
	for (index, field) in set.fields.iter().enumerate() {
		slots.push(Slot {
			n: field.n,
			index,
			field,
			old: false,
		});
		if let Some(sup) = &field.supersedes {
			slots.push(Slot {
				n: sup.old,
				index,
				field,
				old: true,
			});
		}
	}
	slots.sort_by_key(|slot| Reverse(slot.n));

	let steps = slots.iter().map(|slot| {
		let n = slot.n;
		let v = bind(slot.index);
		let field = slot.field;
		if field.supersedes.is_some() {
			let w = format_ident!("__w{}", slot.index);
			let narrow = format_ident!("__s{}", slot.index);
			if slot.old {
				quote! {
					match &#narrow {
						::core::option::Option::Some(old) => __w.put(old, #n)?,
						::core::option::Option::None => __w.skip(#n)?,
					}
				}
			} else {
				quote! {
					if #w && #narrow.is_none() {
						__w.put(#v, #n)?;
					} else {
						__w.skip(#n)?;
					}
				}
			}
		} else {
			match &field.presence {
				Presence::Required => quote!(__w.put(#v, #n)?;),
				Presence::Opt(_) => {
					let opt = opt_ref(field, &v);
					quote! {
						match #opt {
							::core::option::Option::Some(inner) => __w.put(inner, #n)?,
							::core::option::Option::None => __w.skip(#n)?,
						}
					}
				}
				Presence::Default(_) => {
					let cond = written(field, &v);
					quote! {
						if #cond {
							__w.put(#v, #n)?;
						} else {
							__w.skip(#n)?;
						}
					}
				}
			}
		}
	});
	quote! {
		{
			let __ours: fn(u32) -> bool = #ours;
			#(#prepare)*
			let mut __w = #s::rt::MapWriter::new(#enc, #prior, #ty, __ours)?;
			#(#steps)*
			__w.finish()?;
		}
	}
}

/// A block that reads the set from the MAP `item` at schema depth `depth` and builds the value
/// with `build`, which receives the field values in declaration order.
fn decode_set(
	ty: &str,
	set: &FieldSet,
	item: &TokenStream,
	depth: &TokenStream,
	build: &dyn Fn(&[Ident]) -> TokenStream,
) -> TokenStream {
	let s = schema();
	let reserved = &set.reserved;
	let decls = set.fields.iter().enumerate().map(|(i, f)| {
		let v = out(i);
		let inner = decode_type(f);
		let mut decl =
			quote!(let mut #v: ::core::option::Option<#inner> = ::core::option::Option::None;);
		if f.supersedes.is_some() {
			let o = format_ident!("__o{i}");
			decl.extend(quote!(let mut #o = ::core::option::Option::None;));
		}
		decl
	});
	let arms = set.fields.iter().enumerate().flat_map(|(i, f)| {
		let v = out(i);
		let n = f.n;
		let inner = decode_type(f);
		let mut arms = vec![quote! {
			#n => #v = ::core::option::Option::Some(
				#s::rt::field::<#inner>(__it, __depth, #ty, #n)?
			),
		}];
		if let Some(sup) = &f.supersedes {
			let o = format_ident!("__o{i}");
			let old = sup.old;
			let narrow = &sup.narrow;
			arms.push(quote! {
				#old => #o = ::core::option::Option::Some(
					#s::rt::field_via(__it, __depth, #ty, #old, #narrow)?
				),
			});
		}
		arms
	});
	let finish = set.fields.iter().enumerate().map(|(i, f)| {
		let v = out(i);
		let r = format_ident!("__r{i}");
		let n = f.n;
		let merged = if f.supersedes.is_some() {
			let o = format_ident!("__o{i}");
			quote! {
				match (#v, #o) {
					(::core::option::Option::Some(a), ::core::option::Option::None) => {
						::core::option::Option::Some(a)
					}
					(::core::option::Option::None, ::core::option::Option::Some(old)) => {
						::core::option::Option::Some(::core::convert::From::from(old))
					}
					(::core::option::Option::None, ::core::option::Option::None) => {
						::core::option::Option::None
					}
					_ => return ::core::result::Result::Err(#s::rt::order(#ty)),
				}
			}
		} else {
			quote!(#v)
		};
		match &f.presence {
			Presence::Required => quote! {
				let #r = match #merged {
					::core::option::Option::Some(v) => v,
					::core::option::Option::None => {
						return ::core::result::Result::Err(#s::rt::missing(#ty, #n));
					}
				};
			},
			Presence::Default(path) => quote! {
				let #r = match #merged {
					::core::option::Option::Some(v) => v,
					::core::option::Option::None => (#path)(),
				};
			},
			Presence::Opt(_) => {
				if f.unrevisioned {
					quote!(let #r = #s::Unrevisioned(#merged);)
				} else {
					quote!(let #r = #merged;)
				}
			}
		}
	});
	let results: Vec<Ident> = (0..set.fields.len()).map(|i| format_ident!("__r{i}")).collect();
	let value = build(&results);
	quote! {
		{
			let __depth: u32 = #depth;
			let __map = #s::rt::map(#item, __depth, #ty)?;
			let mut __prev = 0u32;
			#(#decls)*
			for __i in 0..__map.len() {
				let (__n, __it) = #s::rt::entry(&__map, __i, &mut __prev, #ty)?;
				match __n {
					#(#arms)*
					_ => #s::rt::unknown(__n, #ty, &[#(#reserved),*])?,
				}
			}
			#(#finish)*
			#value
		}
	}
}

/// The type a field's item decodes to: the type inside an `Option`, else the field type.
fn decode_type(f: &Field) -> Type {
	match &f.presence {
		Presence::Opt(inner) => inner.clone(),
		_ => f.ty.clone(),
	}
}

/// Statements that fold the written fields' floors into `acc`.
fn floor_set(set: &FieldSet, type_since: Rel) -> TokenStream {
	let steps = set.fields.iter().enumerate().map(|(i, f)| {
		let v = bind(i);
		let raise = |since: Rel| {
			(f.critical() && since > type_since).then(|| {
				let r = rel(since);
				quote!(acc.raise(#r);)
			})
		};
		let s = schema();
		let fl = |expr: TokenStream| quote!(#s::StoredField::floor(#expr, acc)?;);
		if let Some(sup) = &f.supersedes {
			let cond = written(f, &v);
			let narrow = &sup.narrow;
			let new = raise(f.since).unwrap_or_default();
			let old = raise(type_since).unwrap_or_default();
			let inner = fl(quote!(#v));
			quote! {
				if #cond {
					if (#narrow)(#v).is_some() { #old } else { #new }
					#inner
				}
			}
		} else {
			let up = raise(f.since).unwrap_or_default();
			match &f.presence {
				Presence::Required => {
					let inner = fl(quote!(#v));
					quote! { #up #inner }
				}
				Presence::Opt(_) => {
					let opt = opt_ref(f, &v);
					let inner = fl(quote!(inner));
					quote! {
						if let ::core::option::Option::Some(inner) = #opt {
							#up
							#inner
						}
					}
				}
				Presence::Default(_) => {
					let cond = written(f, &v);
					let inner = fl(quote!(#v));
					quote! {
						if #cond {
							#up
							#inner
						}
					}
				}
			}
		}
	});
	quote!(#(#steps)*)
}

/// An expression for the newest `Unrevisioned` release among the set's written fields.
fn unrev_set(set: &FieldSet) -> TokenStream {
	let s = schema();
	let steps = set.fields.iter().enumerate().map(|(i, f)| {
		let v = bind(i);
		let own = f.unrevisioned.then(|| {
			let r = rel(f.since);
			let cond = written(f, &v);
			quote! {
				if #cond {
					__r = #s::rt::max_release(__r, ::core::option::Option::Some(#r));
				}
			}
		});
		let nested = match &f.presence {
			Presence::Opt(_) => {
				let opt = opt_ref(f, &v);
				quote! {
					if let ::core::option::Option::Some(inner) = #opt {
						__r = #s::rt::max_release(
							__r,
							#s::StoredField::unrevisioned_floor(inner),
						);
					}
				}
			}
			_ => quote! {
				__r = #s::rt::max_release(__r, #s::StoredField::unrevisioned_floor(#v));
			},
		};
		quote! { #own #nested }
	});
	quote! {
		let mut __r: ::core::option::Option<#s::Release> = ::core::option::Option::None;
		#(#steps)*
		__r
	}
}

/// An expression building the VM object of a set, from owned bindings `__f0, __f1, …`.
fn vm_into_set(set: &FieldSet) -> TokenStream {
	let s = schema();
	let entries = set.fields.iter().enumerate().map(|(i, f)| {
		let v = bind(i);
		let name = &f.name;
		let ty = &f.ty;
		quote!(__o.push(#name, <#ty as #s::StoredField>::into_vm_value(#v)?);)
	});
	let count = set.fields.len();
	if count == 0 {
		return quote!(#s::vm::ObjectBuilder::new(0).finish());
	}
	quote! {
		{
			let mut __o = #s::vm::ObjectBuilder::new(#count);
			#(#entries)*
			__o.finish()
		}
	}
}

/// A block building the value from the VM object `value`; `build` receives each field's value.
fn vm_from_set(
	ty: &str,
	set: &FieldSet,
	value: &TokenStream,
	build: &dyn Fn(&[Ident]) -> TokenStream,
) -> TokenStream {
	let s = schema();
	let reads = set.fields.iter().enumerate().map(|(i, f)| {
		let r = format_ident!("__r{i}");
		let name = &f.name;
		let fty = &f.ty;
		match &f.presence {
			Presence::Default(path) => {
				quote!(let #r: #fty = #s::vm::field_or(&mut __object, #name, #path)?;)
			}
			_ => quote!(let #r: #fty = #s::vm::field(&mut __object, #name)?;),
		}
	});
	let results: Vec<Ident> = (0..set.fields.len()).map(|i| format_ident!("__r{i}")).collect();
	let built = build(&results);
	quote! {
		{
			let mut __object = #s::vm::object_of(#value, #ty)?;
			#(#reads)*
			#built
		}
	}
}

fn field_schemas(set: &FieldSet) -> Vec<TokenStream> {
	let s = schema();
	let mut out = Vec::new();
	for f in &set.fields {
		let n = f.n;
		let name = &f.name;
		let ty = &f.ty;
		let class = if f.ignorable {
			quote!(#s::Class::Ignorable)
		} else {
			quote!(#s::Class::Critical)
		};
		let since = rel(f.since);
		let absent = match &f.presence {
			Presence::Required => quote!(#s::Absent::Required),
			Presence::Opt(_) => quote!(#s::Absent::None),
			Presence::Default(path) => {
				// An `Unrevisioned` field's default is spelled the way every other schema
				// spells it, whatever path the expansion calls.
				let text = if f.unrevisioned {
					"Default::default".to_owned()
				} else {
					quote!(#path).to_string().replace(' ', "")
				};
				quote!(#s::Absent::Default(#text))
			}
		};
		let supersedes = match &f.supersedes {
			Some(sup) => {
				let old = sup.old;
				quote!(::core::option::Option::Some(#old))
			}
			None => quote!(::core::option::Option::None),
		};
		let independent = match &f.independent {
			Some(reason) => quote!(::core::option::Option::Some(#reason)),
			None => quote!(::core::option::Option::None),
		};
		let unrevisioned = f.unrevisioned;
		out.push(quote! {
			#s::FieldSchema {
				n: #n,
				name: #name,
				ty: <#ty as #s::StoredField>::write_type_name,
				class: #class,
				since: #since,
				absent: #absent,
				supersedes: #supersedes,
				independent: #independent,
				unrevisioned: #unrevisioned,
			}
		});
		if let Some(sup) = &f.supersedes {
			let old = sup.old;
			let narrow = &sup.narrow;
			let class = if old % 2 == 0 {
				quote!(#s::Class::Critical)
			} else {
				quote!(#s::Class::Ignorable)
			};
			// The narrow form was written from the type's first release.
			let since = rel(crate::model::FIRST);
			out.push(quote! {
				#s::FieldSchema {
					n: #old,
					name: #name,
					ty: #s::rt::narrow_type_name(#narrow),
					class: #class,
					since: #since,
					absent: #absent,
					supersedes: ::core::option::Option::None,
					independent: ::core::option::Option::None,
					unrevisioned: false,
				}
			});
		}
	}
	out
}

// ---------------------------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------------------------

fn enum_parts(model: &Model, variants: &[Variant]) -> Parts {
	let s = schema();
	let ty = model.ident.to_string();
	let legacy = model.attrs.legacy.as_ref();
	let has_legacy = variants.iter().any(|v| v.n.is_none());

	// `self` after the legacy conversion, as a reference.
	let convert_ref = |has: bool| {
		if let (true, Some(hook)) = (has, legacy) {
			let is_legacy = legacy_pattern(variants);
			quote! {
				let __converted;
				let __this: &Self = if matches!(self, #is_legacy) {
					__converted = (#hook)(self);
					&__converted
				} else {
					self
				};
			}
		} else {
			quote!(let __this: &Self = self;)
		}
	};
	// ---- encode
	let encode_arms = variants.iter().map(|v| {
		let ident = &v.ident;
		let Some(n) = v.n else {
			return quote! {
				Self::#ident { .. } => {
					return ::core::result::Result::Err(#s::rt::legacy(#ty));
				}
			};
		};
		match &v.payload {
			Payload::Unit => quote! {
				Self::#ident => {
					enc.none();
					enc.end_entry(&mut __open, #n)?;
				}
			},
			Payload::Single(t) => quote! {
				Self::#ident(__f0) => {
					let __p = #s::rt::variant_prior(prior, #n);
					<#t as #s::StoredField>::encode_item(__f0, enc, __p)
						.map_err(|e| e.at(#ty, #n))?;
					enc.end_entry(&mut __open, #n)?;
				}
			},
			Payload::Positional(types) => {
				let set = positional_set(types, v.since);
				let binds: Vec<Ident> = (0..types.len()).map(bind).collect();
				let writer = encode_set(&ty, &set, &quote!(__p), &quote!(enc));
				quote! {
					Self::#ident(#(#binds),*) => {
						let __p = #s::rt::variant_prior(prior, #n);
						#writer
						enc.end_entry(&mut __open, #n)?;
					}
				}
			}
			Payload::Fields(set) => {
				let members: Vec<&Member> = set.fields.iter().map(|f| &f.member).collect();
				let binds: Vec<Ident> = (0..set.fields.len()).map(bind).collect();
				let writer = encode_set(&ty, set, &quote!(__p), &quote!(enc));
				quote! {
					Self::#ident { #(#members: #binds),* } => {
						let __p = #s::rt::variant_prior(prior, #n);
						#writer
						enc.end_entry(&mut __open, #n)?;
					}
				}
			}
		}
	});
	let convert = convert_ref(has_legacy);
	let encode = quote! {
		#convert
		let mut __open = enc.open()?;
		match __this {
			#(#encode_arms)*
		}
		enc.close_map(__open)?;
		::core::result::Result::Ok(())
	};

	// ---- decode
	let decode_arms = variants.iter().filter_map(|v| {
		let n = v.n?;
		let ident = &v.ident;
		let arm = match &v.payload {
			Payload::Unit => quote! {
				#s::rt::unit(__payload, #ty, #n)?;
				Self::#ident
			},
			Payload::Single(t) => quote! {
				Self::#ident(#s::rt::field::<#t>(__payload, depth, #ty, #n)?)
			},
			Payload::Positional(types) => {
				let set = positional_set(types, v.since);
				let build = |vals: &[Ident]| quote!(Self::#ident(#(#vals),*));
				decode_set(&ty, &set, &quote!(__payload), &quote!(depth.saturating_add(1)), &build)
			}
			Payload::Fields(set) => {
				let members: Vec<&Member> = set.fields.iter().map(|f| &f.member).collect();
				let build = |vals: &[Ident]| quote!(Self::#ident { #(#members: #vals),* });
				decode_set(&ty, set, &quote!(__payload), &quote!(depth.saturating_add(1)), &build)
			}
		};
		Some(quote!(#n => ::core::result::Result::Ok({ #arm }),))
	});
	let decode = quote! {
		let (__n, __payload) = #s::rt::one_variant(item, depth, #ty)?;
		match __n {
			#(#decode_arms)*
			_ => ::core::result::Result::Err(#s::SchemaError::UnknownVariant { ty: #ty, n: __n }),
		}
	};

	// ---- floor and unrevisioned floor
	let floor_arms = variants.iter().map(|v| {
		let ident = &v.ident;
		let Some(_) = v.n else {
			return quote! {
				Self::#ident { .. } => {
					return ::core::result::Result::Err(#s::rt::legacy(#ty));
				}
			};
		};
		let raise = (v.since > FIRST).then(|| {
			let r = rel(v.since);
			quote!(acc.raise(#r);)
		});
		match &v.payload {
			Payload::Unit => quote!(Self::#ident => { #raise }),
			Payload::Single(t) => quote! {
				Self::#ident(__f0) => {
					#raise
					<#t as #s::StoredField>::floor(__f0, acc)?;
				}
			},
			Payload::Positional(types) => {
				let set = positional_set(types, v.since);
				let binds: Vec<Ident> = (0..types.len()).map(bind).collect();
				let body = floor_set(&set, v.since);
				quote! {
					Self::#ident(#(#binds),*) => {
						#raise
						#body
					}
				}
			}
			Payload::Fields(set) => {
				let members: Vec<&Member> = set.fields.iter().map(|f| &f.member).collect();
				let binds: Vec<Ident> = (0..set.fields.len()).map(bind).collect();
				let body = floor_set(set, v.since);
				quote! {
					Self::#ident { #(#members: #binds),* } => {
						#raise
						#body
					}
				}
			}
		}
	});
	let convert = convert_ref(has_legacy);
	let floor = quote! {
		#convert
		match __this {
			#(#floor_arms)*
		}
		::core::result::Result::Ok(())
	};
	let unrev_arms = variants.iter().map(|v| {
		let ident = &v.ident;
		if v.n.is_none() {
			return quote!(Self::#ident { .. } => ::core::option::Option::None,);
		}
		match &v.payload {
			Payload::Unit => quote!(Self::#ident => ::core::option::Option::None,),
			Payload::Single(t) => quote! {
				Self::#ident(__f0) => <#t as #s::StoredField>::unrevisioned_floor(__f0),
			},
			Payload::Positional(types) => {
				let set = positional_set(types, v.since);
				let binds: Vec<Ident> = (0..types.len()).map(bind).collect();
				let body = unrev_set(&set);
				quote!(Self::#ident(#(#binds),*) => { #body },)
			}
			Payload::Fields(set) => {
				let members: Vec<&Member> = set.fields.iter().map(|f| &f.member).collect();
				let binds: Vec<Ident> = (0..set.fields.len()).map(bind).collect();
				let body = unrev_set(set);
				quote!(Self::#ident { #(#members: #binds),* } => { #body },)
			}
		}
	});
	let convert = convert_ref(has_legacy);
	let unrev = quote! {
		#convert
		match __this {
			#(#unrev_arms)*
		}
	};

	// ---- VM
	let convert_owned = if let (true, Some(hook)) = (has_legacy, legacy) {
		let is_legacy = legacy_pattern(variants);
		quote! {
			let __this: Self = if matches!(self, #is_legacy) { (#hook)(&self) } else { self };
		}
	} else {
		quote!(let __this: Self = self;)
	};
	let vm_into_arms = variants.iter().map(|v| {
		let ident = &v.ident;
		let name = &v.name;
		if v.n.is_none() {
			return quote! {
				Self::#ident { .. } => {
					return ::core::result::Result::Err(#s::rt::legacy(#ty));
				}
			};
		}
		match &v.payload {
			Payload::Unit => quote! {
				Self::#ident => #s::vm::variant(#name, ::surrealdb_expr::val::Value::None),
			},
			Payload::Single(t) => quote! {
				Self::#ident(__f0) => #s::vm::variant(
					#name,
					<#t as #s::StoredField>::into_vm_value(__f0)?,
				),
			},
			Payload::Positional(types) => {
				let binds: Vec<Ident> = (0..types.len()).map(bind).collect();
				quote! {
					Self::#ident(#(#binds),*) => #s::vm::variant(
						#name,
						#s::vm::tuple([
							#( <#types as #s::StoredField>::into_vm_value(#binds)? ),*
						]),
					),
				}
			}
			Payload::Fields(set) => {
				let members: Vec<&Member> = set.fields.iter().map(|f| &f.member).collect();
				let binds: Vec<Ident> = (0..set.fields.len()).map(bind).collect();
				let body = vm_into_set(set);
				quote! {
					Self::#ident { #(#members: #binds),* } => #s::vm::variant(#name, #body),
				}
			}
		}
	});
	let vm_into = quote! {
		#convert_owned
		::core::result::Result::Ok(match __this {
			#(#vm_into_arms)*
		})
	};
	let vm_from_arms = variants.iter().filter(|v| v.n.is_some()).map(|v| {
		let ident = &v.ident;
		let name = &v.name;
		let arm = match &v.payload {
			Payload::Unit => quote!(Self::#ident),
			Payload::Single(t) => quote! {
				Self::#ident(<#t as #s::StoredField>::from_vm_value(__body)?)
			},
			Payload::Positional(types) => {
				let count = types.len();
				let vals: Vec<Ident> = (0..count).map(out).collect();
				quote! {
					{
						let mut __items = #s::vm::tuple_of(__body, #count, #ty)?;
						#( let #vals = <#types as #s::StoredField>::from_vm_value(
							#s::vm::next(&mut __items, #ty)?
						)?; )*
						Self::#ident(#(#vals),*)
					}
				}
			}
			Payload::Fields(set) => {
				let members: Vec<&Member> = set.fields.iter().map(|f| &f.member).collect();
				let build = |vals: &[Ident]| quote!(Self::#ident { #(#members: #vals),* });
				vm_from_set(&ty, set, &quote!(__body), &build)
			}
		};
		quote!(#name => #arm,)
	});
	let vm_from = quote! {
		let (__name, __body) = #s::vm::split_variant(value, #ty)?;
		::core::result::Result::Ok(match __name.as_str() {
			#(#vm_from_arms)*
			_ => {
				return ::core::result::Result::Err(
					#s::SchemaError::VmShape { ty: #ty, what: "an unknown variant" },
				);
			}
		})
	};

	let type_name = quote!(out.push_str(#ty););
	let held = collect_fields(variants.iter().filter(|v| v.n.is_some()).flat_map(|v| {
		let types: Vec<Type> = match &v.payload {
			Payload::Unit => Vec::new(),
			Payload::Single(t) => vec![t.clone()],
			Payload::Positional(types) => types.clone(),
			Payload::Fields(set) => set.fields.iter().map(|f| f.ty.clone()).collect(),
		};
		types
	}));
	let collect = collect_with(model, &held);

	let variant_schemas = variants.iter().map(|v| {
		let name = &v.name;
		let since = rel(v.since);
		let n = match v.n {
			Some(n) => quote!(::core::option::Option::Some(#n)),
			None => quote!(::core::option::Option::None),
		};
		let payload = if v.n.is_none() {
			quote!(#s::Payload::Unit)
		} else {
			match &v.payload {
				Payload::Unit => quote!(#s::Payload::Unit),
				Payload::Single(t) => {
					quote!(#s::Payload::Item(<#t as #s::StoredField>::write_type_name))
				}
				Payload::Positional(types) => quote! {
					#s::Payload::Positional(&[
						#( <#types as #s::StoredField>::write_type_name ),*
					])
				},
				Payload::Fields(set) => {
					let fields = field_schemas(set);
					quote!(#s::Payload::Fields(&[#(#fields),*]))
				}
			}
		};
		quote! {
			#s::VariantSchema { n: #n, name: #name, since: #since, payload: #payload }
		}
	});
	let shape = quote!(#s::Shape::Enum(&[#(#variant_schemas),*]));
	Parts {
		encode,
		decode,
		floor,
		unrev,
		vm_into,
		vm_from,
		type_name,
		collect,
		shape,
	}
}

/// `Self::A { .. } | Self::B { .. }` over the legacy variants.
fn legacy_pattern(variants: &[Variant]) -> TokenStream {
	let pats = variants.iter().filter(|v| v.n.is_none()).map(|v| {
		let ident = &v.ident;
		quote!(Self::#ident { .. })
	});
	quote!(#(#pats)|*)
}
