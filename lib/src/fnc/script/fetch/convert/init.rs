use rquickjs::{
    ArrayBuffer, Class, Ctx, Error, FromJs, Persistent, String, Type, TypedArray, Value,
};

/// The type of the init body argument for a response.
#[derive(Clone)]
pub enum BodyInit {
    Blob(Persistent<Class<'static, BlobClass>>),
    ArrayI8(Persistent<TypedArray<'static, i8>>),
    ArrayU8(Persistent<TypedArray<'static, u8>>),
    ArrayI16(Persistent<TypedArray<'static, i16>>),
    ArrayU16(Persistent<TypedArray<'static, u16>>),
    ArrayI32(Persistent<TypedArray<'static, i32>>),
    ArrayU32(Persistent<TypedArray<'static, u32>>),
    ArrayI64(Persistent<TypedArray<'static, i64>>),
    ArrayU64(Persistent<TypedArray<'static, u64>>),
    ArrayBuffer(Persistent<ArrayBuffer<'static>>),
    /// TODO
    FormData(()),
    /// TODO
    URLSearchParams(()),
    String(Persistent<String<'static>>),
    Stream(Rc<RefCell<Option<reqwest::Response>>>),
}

impl HasRefs for BodyInit {
    fn mark_refs(&self, marker: &RefsMarker) {
        match *self {
            BodyInit::Blob(ref x) => x.mark_refs(marker),
            BodyInit::ArrayI8(ref x) => x.mark_refs(marker),
            BodyInit::ArrayU8(ref x) => x.mark_refs(marker),
            BodyInit::ArrayI16(ref x) => x.mark_refs(marker),
            BodyInit::ArrayU16(ref x) => x.mark_refs(marker),
            BodyInit::ArrayI32(ref x) => x.mark_refs(marker),
            BodyInit::ArrayU32(ref x) => x.mark_refs(marker),
            BodyInit::ArrayI64(ref x) => x.mark_refs(marker),
            BodyInit::ArrayU64(ref x) => x.mark_refs(marker),
            BodyInit::ArrayBuffer(ref x) => x.mark_refs(marker),
            BodyInit::String(ref x) => x.mark_refs(marker),
            BodyInit::FormData(_) | BodyInit::URLSearchParams(_) | BodyInit::Stream(_) => {}
        }
    }
}

impl<'js> FromJs<'js> for BodyInit {
    fn from_js(ctx: Ctx<'js>, value: Value<'js>) -> Result<Self> {
        let object = match value.type_of() {
            Type::String => {
                let s = Persistent::save(ctx, value.into_string().unwrap());
                return Ok(BodyInit::String(s));
            }
            Type::Object => value.as_object().unwrap(),
            x => {
                return Err(Error::FromJs {
                    from: x.as_str(),
                    to: "Blob, TypedArray, FormData, URLSearchParams, or String",
                    message: None,
                })
            }
        };
        if let Ok(x) = Class::<Blob>::from_object(object.clone()) {
            return Ok(BodyInit::Blob(Persistent::save(ctx, x)));
        }
        if let Ok(x) = TypedArray::<i8>::from_object(object.clone()) {
            return Ok(BodyInit::ArrayI8(Persistent::save(ctx, x)));
        }
        if let Ok(x) = TypedArray::<u8>::from_object(object.clone()) {
            return Ok(BodyInit::ArrayU8(Persistent::save(ctx, x)));
        }
        if let Ok(x) = TypedArray::<i16>::from_object(object.clone()) {
            return Ok(BodyInit::ArrayI16(Persistent::save(ctx, x)));
        }
        if let Ok(x) = TypedArray::<u16>::from_object(object.clone()) {
            return Ok(BodyInit::ArrayU16(Persistent::save(ctx, x)));
        }
        if let Ok(x) = TypedArray::<i32>::from_object(object.clone()) {
            return Ok(BodyInit::ArrayI32(Persistent::save(ctx, x)));
        }
        if let Ok(x) = TypedArray::<u32>::from_object(object.clone()) {
            return Ok(BodyInit::ArrayU32(Persistent::save(ctx, x)));
        }
        if let Ok(x) = TypedArray::<i64>::from_object(object.clone()) {
            return Ok(BodyInit::ArrayI64(Persistent::save(ctx, x)));
        }
        if let Ok(x) = TypedArray::<u64>::from_object(object.clone()) {
            return Ok(BodyInit::ArrayU64(Persistent::save(ctx, x)));
        }
        if let Ok(x) = ArrayBuffer::from_object(object.clone()) {
            return Ok(BodyInit::ArrayBuffer(Persistent::save(ctx, x)));
        }

        Err(Error::FromJs {
            from: "object",
            to: "Blob, TypedArray, FormData, URLSearchParams, or String",
            message: None,
        })
    }
}

#[derive(Clone)]
pub struct ResponseInit {
    // u16 instead of reqwest::StatusCode since javascript allows non valid status codes in some
    // circumstances.
    pub status: u16,
    pub status_text: String,
    pub headers: Persistent<Class<'static, Headers>>,
}

impl HasRefs for ResponseInit {
    fn mark_refs(&self, marker: &RefsMarker) {
        self.headers.mark_refs(marker);
    }
}

impl ResponseInit {
    /// Returns a ResponseInit object with all values as the default value.
    pub fn default(ctx: Ctx<'_>) -> Result<ResponseInit> {
        let headers = Class::instance(ctx, Headers::new_empty())?;
        let headers = Persistent::save(ctx, headers);
        Ok(ResponseInit {
            status: 200,
            status_text: String::new(),
            headers,
        })
    }
}

impl<'js> FromJs<'js> for ResponseInit {
    fn from_js(ctx: Ctx<'js>, value: Value<'js>) -> Result<Self> {
        let object = Object::from_js(ctx, value)?;

        let status =
            if let Some(Coerced(status)) = object.get::<_, Option<Coerced<i32>>>("status")? {
                if !(200..=599).contains(&status) {
                    return Err(Exception::throw_range(
                        ctx,
                        "response status code outside range",
                    ));
                }
                status as u16
            } else {
                200u16
            };

        let status_text = if let Some(Coerced(string)) =
            object.get::<_, Option<Coerced<String>>>("statusText")?
        {
            if !is_reason_phrase(string.as_str()) {
                return Err(Exception::throw_type(
                    ctx,
                    "statusText was not a valid reason phrase",
                ));
            }
            string
        } else {
            String::new()
        };

        let headers = if let Some(headers) = object.get::<_, Option<Value>>("headers")? {
            let headers = Headers::new_inner(ctx, headers)?;
            Class::instance(ctx, headers)?
        } else {
            Class::instance(ctx, Headers::new_empty())?
        };
        let headers = Persistent::save(ctx, headers);

        Ok(ResponseInit {
            status,
            status_text,
            headers,
        })
    }
}
