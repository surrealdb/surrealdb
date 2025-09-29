use proc_macro::TokenStream;
use syn::{Generics, Ident, WhereClause};
use quote::quote;

use crate::{UnitAttributes, UnitValue};

/// Generate implementation for a struct with named fields
pub fn impl_struct_unit(
	name: &Ident,
	generics: &Generics,
	where_clause: &Option<WhereClause>,
	attrs: UnitAttributes,
) -> TokenStream {
	let name_str = name.to_string();
	
    if let Some(UnitValue { value, is_value, kind_of }) = attrs.value {
        quote! {
            impl #generics SurrealValue for #name #generics #where_clause {
                fn into_value(self) -> surrealdb_types::Value {
                    surrealdb_types::Value::from_t(#value)
                }
    
                fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
                    if #is_value {
                        Ok(Self)
                    } else {
                        Err(surrealdb_types::anyhow::anyhow!("Expected value of type {}", #name_str))
                    }
                }
    
                fn is_value(value: &surrealdb_types::Value) -> bool {
                    #is_value
                }
    
                fn kind_of() -> surrealdb_types::Kind {
                    #kind_of
                }
            }
        }.into()
    } else {
        quote! {
            impl #generics SurrealValue for #name #generics #where_clause {
                fn into_value(self) -> surrealdb_types::Value {
                    surrealdb_types::Value::Object(surrealdb_types::Object::new())
                }
    
                fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
                    if let surrealdb_types::Value::Object(obj) = value {
                        if obj.is_empty() {
                            return Ok(Self)
                        }    
                    }

                    Err(surrealdb_types::anyhow::anyhow!("Expected object value"))
                }
    
                fn is_value(value: &surrealdb_types::Value) -> bool {
                    if let surrealdb_types::Value::Object(obj) = value {
                        if obj.is_empty() {
                            return true
                        }    
                    }

                    false
                }
    
                fn kind_of() -> surrealdb_types::Kind {
                    surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(std::collections::BTreeMap::new()))
                }
            }
        }.into()
    }
}
