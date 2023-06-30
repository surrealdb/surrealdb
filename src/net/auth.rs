use futures_util::future::BoxFuture;
use http::{StatusCode, request::Parts};
use hyper::{Request, Response};
use axum::{body::{boxed, Body, BoxBody}, RequestPartsExt, headers::{Authorization, authorization::{Basic, Bearer}, Origin}, TypedHeader, Extension};
use surrealdb::{dbs::Session, iam::{verify::token}};
use tower_http::auth::AsyncAuthorizeRequest;

use crate::{iam::verify::basic, err::Error, dbs::DB};

use super::{client_ip::ExtractClientIP, AppState};

///
/// SurrealAuth is a tower layer that implements the AsyncAuthorizeRequest trait.
/// It is used to authorize requests to SurrealDB using Basic or Token authentication.
/// 
/// It has to be used in conjunction with the tower_http::auth::RequireAuthorizationLayer layer:
/// 
/// ```rust
/// use tower_http::auth::RequireAuthorizationLayer;
/// use surrealdb::net::SurrealAuth;
/// use axum::Router;
///
/// let auth = RequireAuthorizationLayer::new(SurrealAuth);
/// 
/// let app = Router::new()
///   .route("/version", get(|| async { "0.1.0" }))
///   .layer(auth);
/// ```
#[derive(Clone, Copy)]
pub(super) struct SurrealAuth;

impl<B> AsyncAuthorizeRequest<B> for SurrealAuth
where
    B: Send + Sync + 'static,
{
    type RequestBody = B;
    type ResponseBody = BoxBody;
    type Future = BoxFuture<'static, Result<Request<B>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, request: Request<B>) -> Self::Future {
        Box::pin(async {
            let (mut parts, body) = request.into_parts();
            match check_auth(&mut parts).await {
                Ok(sess) => {
                    parts.extensions.insert(sess);
                    Ok(Request::from_parts(parts, body))
                }
                Err(err) => {
                    let unauthorized_response = Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(boxed(Body::from(err.to_string())))
                        .unwrap();
                    Err(unauthorized_response)
                }
            }
        })
    }
}

async fn check_auth(parts: &mut Parts) -> Result<Session, Error> {
    let kvs = DB.get().unwrap();

    let or: Option<String> = parts.extract::<TypedHeader<Origin>>().await.and_then(|or| {
        if !or.is_null() {
            Ok(Some(or.to_string()))
        } else {
            Ok(None)
        }
    }).unwrap_or(None);

    let id = parts.headers.get("id").map(|v| v.to_str().unwrap().to_string()); // TODO: Use a TypedHeader
    let ns = parts.headers.get("ns").map(|v| v.to_str().unwrap().to_string()); // TODO: Use a TypedHeader
    let db = parts.headers.get("db").map(|v| v.to_str().unwrap().to_string()); // TODO: Use a TypedHeader

    let Extension(state) = parts.extract::<Extension<AppState>>().await.or_else(|err| {
        tracing::error!("Error extracting the app state: {:?}", err);
        Err(Error::InvalidAuth)
    })?;
    let ExtractClientIP(ip) = parts.extract_with_state(&state).await.unwrap_or(ExtractClientIP(None));

    // Create session
    #[rustfmt::skip]
    let mut session = Session { ip, or, id, ns, db, ..Default::default() };
    
    // If Basic authentication data was supplied
    if let Ok(au) = parts.extract::<TypedHeader<Authorization<Basic>>>().await {
        debug!("Basic auth: {:?}", au);
        basic(&mut session, au.username(), au.password()).await?;
    } else if let Ok(au) = parts.extract::<TypedHeader<Authorization<Bearer>>>().await {
        debug!("Bearer auth: {:?}", au);
        token(kvs, &mut session, au.token().into()).await?;
    };

    debug!("Session: {:?}", session);
    Ok(session)
}
