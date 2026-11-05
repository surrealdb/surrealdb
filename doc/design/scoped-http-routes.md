# Scoped HTTP routes

**Status:** Namespace, database and access-method scopes implemented; `.well-known` documents planned ·
**Scope:** `surrealdb/server/src/ntw/`, `surrealdb/core/src/dbs/session.rs`

The HTTP server takes the namespace, database and access method from the URL
path, as an alternative to the `surreal-ns` and `surreal-db` headers and to the
`NS`, `DB` and `AC` fields of signin and signup credentials. The existing
routes, headers and fields stay unchanged.

## 1. Grammar

```
/{method}/{...rest}
/ns/{ns}/{method}/{...rest}
/ns/{ns}/db/{db}/{method}/{...rest}
/ac/{ac}/{method}/{...rest}
/ns/{ns}/ac/{ac}/{method}/{...rest}
/ns/{ns}/db/{db}/ac/{ac}/{method}/{...rest}
```

`ac` can follow every level, because `DEFINE ACCESS` exists on root, namespace
and database. Examples:

```
POST /ns/foo/db/bar/sql
GET  /ns/foo/db/bar/rpc                      (WebSocket upgrade)
ANY  /ns/foo/db/bar/api/orders/42
GET  /ns/foo/db/bar/key/person/tobie
POST /ns/foo/db/bar/signin                   (database user)
POST /ns/foo/db/bar/ac/users/signup          (record access method)
POST /ac/admins/signin                       (root access method)
GET  /ns/foo/db/bar/bucket/{bucket}/{path}  (a database's bucket)
GET  /ns/foo/bucket/{bucket}/{path}         (a namespace's bucket)
GET  /bucket/{bucket}/{path}                (a root bucket)
```

`ns`, `db` and `ac` are reserved. No top-level method can use these names.

Segments are percent-decoded once. Namespace and database names can contain
`/`, spaces and Unicode, so a client sends `/ns/a%2Fb/...` for the namespace
`a/b`. An empty or malformed segment matches no route, and the server returns
404.

### Scopable methods

| Method | `ns`, `db` | `ac` |
|---|---|---|
| `sql`, `rpc`, `import`, `export`, `key`, `ml`, `gql`, `graphql`, `mcp`, `api`, `bucket` | yes | no, 404 |
| `signin`, `signup` (and their `/cookie` forms) | yes | yes |
| `signout`, `refresh` | no, 404 | no, 404 |
| `.well-known` | planned, see section 5 | planned |
| `health`, `version`, `status`, `ready`, `metrics`, `kvexport`, `sync` | no, 404 | no, 404 |

A route outside its set returns 404 under a scope. It does not ignore the scope.

`signout` and `refresh` act on a credential the request already carries, which
names its own namespace, database and access method.

### Scoped signin and signup

`signin` and `signup` read the namespace, database and access method from the
request body, not from the session. Under a scope, the body is completed from
the path before the credentials are checked:

```
POST /ns/foo/db/bar/ac/users/signin
{ "email": "a@b.c" }
→ signin with { "NS": "foo", "DB": "bar", "AC": "users", "email": "a@b.c" }
```

The path fixes the level the credentials act at. A body field that repeats the
path is accepted; when the body names a level under both aliases (`NS` and
`ns`), both must repeat it, because a `SIGNIN` or `SIGNUP` clause can read
either. A body field that names something else returns 400, and so does a
namespace or database the path leaves out: `/ac/admins/signin` is a root access
method and `/ns/foo/signin` a namespace signin, whatever the body adds. Without
an `ac` in the path, the body may name an access method at the path's level.

### `/api` keeps both forms

```
/api/{ns}/{db}/{*path}            existing form, unchanged
/ns/{ns}/db/{db}/api/{*path}      scoped form
```

Both forms reach the same handler: the scoped form is rewritten to the existing
form (`/api/foo/bar/{*path}`). A plain prefix strip to `/api/{*path}` would
match `/api/{ns}/{db}/{*path}` and read the first two segments of the API path
as the namespace and database. A scoped `/api` path without a database returns
404.

### `/bucket` has one form per level

A bucket is defined on the root, a namespace or a database, and the URL names
which:

```
/bucket/{name}/{*path}                    a bucket on the root
/ns/{ns}/bucket/{name}/{*path}            a bucket on a namespace
/ns/{ns}/db/{db}/bucket/{name}/{*path}    a bucket on a database
```

The scoped forms are rewritten to `/bucket/{name}/{*path}` like any other
session route, and the bucket route takes the level from the `PathScope` alone:
no scope is the root, and the `surreal-ns` and `surreal-db` headers never move a
request to another level. There is no `/bucket/{ns}/{db}/…` form;
`/bucket/a/b/c/d` is object `b/c/d` in root bucket `a`.

A session reaches a bucket at its own level or below: a root principal any
bucket, a namespace principal its namespace's and its databases', a database or
record principal its database's. A guest is judged by the guest rules, which
admit it to no bucket above a database.

`file::presign_get` and `file::presign_put` mint the form for the bucket's
level, and the signature covers that path. The signing key derives from the
level's ids, so a URL moved to another level fails to verify even where a
bucket of the same name exists there.

A bucket request that presents a signature (an `exp` or `sig` query parameter)
is authenticated without its `Authorization` header or session cookie. The
signature authorises it; an ambient credential (a browser's cookie for another
tenant, an expired token) would otherwise refuse a URL that needs none.

## 2. Resolution rules

| Path | Other source | Result |
|---|---|---|
| `ns=foo` | header `surreal-ns: foo` | accept |
| `ns=foo` | header `surreal-ns: other` | 400 |
| `ns=foo` | no header | `ns=foo` |
| `ns=foo` | no `db` in path, header `surreal-db: bar` | `ns=foo`, `db=bar` |
| `ns=foo`, `db=bar` | token for a user on `foo`/`bar` | accept |
| `ns=foo`, `db=bar` | token for a user on `foo`/`other` | 401 |
| `ns=foo` | root token | `ns=foo` |
| `ns=foo` | Basic auth as a user on namespace `other` | 401 |

The path never silently overrides a header, and a header never silently
overrides the path.

The authenticated principal must belong to the scope: a namespace principal to
its namespace, a database or record principal also to its database when the
path names one. Basic authentication against `surreal-auth-ns` and
`surreal-auth-db` changes the principal but not the selection, so the principal
is checked on its own.

Namespace- and database-level credentials set the session's namespace and
database themselves. With headers, the credentials win silently. With a path
scope, the request is refused when the credentials select a different
namespace or database than the path names, because the path states where the
request starts.

### The scope is the starting selection

The namespace and database in the path are the session's starting selection,
as the `surreal-ns` and `surreal-db` headers are. They are checked once, when
the request or connection is authenticated (`PathScope::admits`). They do not
confine the session: `USE`, the RPC `use` method and the authentication
methods change the selection as they do on an unscoped connection.

```
connect /ns/foo/db/bar/rpc
query RETURN session::db()   bar
use ["other", "other"]       ok
USE NS foo DB baz;           ok
reset                        ns=none, db=none
attach <id>                  the new session starts with no selection
```

The scope is not stored on the session. It is not serialized and not part of
the durable session (`DurableSession`), so it adds nothing to the persisted
session format.

## 3. Implementation

### Rewrite before routing, not `nest`

With axum 0.8, `nest("/ns/{ns}/db/{db}", …)` and wildcard routes add their
path parameters to the ones the inner route records. Tuple extractors then
fail on the parameter count:

```rust
// key.rs: under /ns/{ns}/db/{db}/key/{table}/{key} this receives 4 params
Path((table, id)): Path<(String, String)>,
```

`ml.rs` has the same pattern.

`scope::mount` instead takes the finished router (all middleware and state
applied) and adds `nest_service("/ns", …)` and `nest_service("/ac", …)` to it.
`nest_service` strips the prefix and records no path parameters. The nested
service:

1. Parses and decodes the `ns`, `db` and `ac` segments.
2. Inserts `PathScope { ns, db, ac }` into the request extensions.
3. Rewrites the URI to the unscoped route (`/ns/foo/db/bar/sql` → `/sql`), or
   to the existing `/api/{ns}/{db}/…` form for `api`. Only the path and query
   change; the scheme and authority are kept.
4. Sends the request to a clone of the router taken before the prefixes were
   added. A rewritten request therefore runs through the same middleware as an
   unscoped one, and cannot be scoped twice.

A path that names no scoped route keeps its original URI, so the router answers
404 through its own middleware. axum records the URI the client sent as
`OriginalUri` before the rewrite.

The session builder in `auth.rs` reads `PathScope` next to the
`SurrealNamespace` and `SurrealDatabase` headers, applies the rules in section
2. The `signin` and `signup` handlers complete their
credentials from `PathScope` (`scope::credentials`).

### Layers that match on the path

Every layer that reads `request.uri().path()` sees the rewritten path, because
the rewrite runs before the router:

| Location | Reads the path for |
|---|---|
| `rate_limit.rs` `auth_endpoint` | matches `/signin`, `/signup`, … |
| `auth.rs` `is_credential_exchange`, `is_signout` | skips the cookie on credential routes |
| `ready.rs` gate | `startup_routes()` allowlist |
| `tracer.rs` | `url.path` span field, which records `OriginalUri` instead |

The rate limiter is a security property: scoped and unscoped signin attempts
from one client spend the same budget (`tests/scoped_rate_limit.rs`).

### Security review

Custom headers force a CORS preflight, and path segments do not. Cookie
credentials do not depend on the preflight: `browser_origin_allowed` in
`auth.rs` enforces the origin on the server for simple requests and WebSocket
upgrades. Bearer tokens are not ambient.

## 4. SDKs

An SDK can put the scope in the endpoint and send no headers and no initial
`use`:

```
wss://host/ns/foo/db/bar/rpc
https://host/ns/foo/db/bar
```

## 5. `.well-known` documents

Clients build discovery URLs from the issuer or resource URL. The server must
serve the URLs that the specifications define. A URL such as
`/.well-known/ns/foo/db/bar/ac/baz/openid-configuration` matches no
specification, and clients never request it.

For the issuer `https://host/ns/foo/db/bar/ac/baz`:

```
# OIDC Discovery 1.0 §4: suffix appended to the issuer
/ns/foo/db/bar/ac/baz/.well-known/openid-configuration

# RFC 8414 §3.1: well-known segment inserted after the host
/.well-known/oauth-authorization-server/ns/foo/db/bar/ac/baz
```

For the resource `https://host/ns/foo/db/bar/mcp`:

```
# RFC 9728 §3: used by the MCP authorization specification
/.well-known/oauth-protected-resource/ns/foo/db/bar/mcp
```

The router must accept `.well-known` in both positions: as a method after the
scope, and as the first segment with the scope after it.

### What each document needs

The catalog stores this for a JWT access method
(`surrealdb/catalog/src/schema/access.rs`):

```rust
pub struct JwtAccess {
    pub verify: JwtAccessVerify,          // Key { alg, key } | Jwks { url }
    pub issue: Option<JwtAccessIssue>,    // { alg, key }: the signing key
    pub audience: Option<Vec<String>>,
}
```

| Document | Can ship | Needs |
|---|---|---|
| `jwks.json` | after `kid` | asymmetric `issue` algorithm; a `kid` on issued tokens |
| `oauth-protected-resource` | after `ISSUER` | an issuer identifier on the access method |
| `openid-configuration` | no | authorization and token endpoints |
| `oauth-authorization-server` | no | authorization and token endpoints |

#### `jwks.json`

```
/ns/foo/db/bar/ac/baz/.well-known/jwks.json
```

Publishes the public half of `verify.key` for an access method that issues
tokens with an asymmetric algorithm (RS\*, ES\*, PS\*, EdDSA). Other services
can then verify tokens that SurrealDB issues. `INFO ... STRUCTURE` already
shows this key without redaction.

- Never publish a key for a symmetric algorithm (HS\*).
- Issued tokens must carry a `kid`, and the JWKS entry must carry the same
  `kid`. The JWKS verifier in `iam/verify.rs` rejects a token without a `kid`,
  so one SurrealDB instance cannot verify another without it. Derive the `kid`
  from a hash of the public key, so that it is stable and needs no storage.
- `INFO` requires authentication, and a well-known URL does not. An
  unauthenticated caller can probe which `ns/db/ac` combinations exist. Decide
  whether publication is opt-in per access method or a server capability.
- `jwks.json` is a convention, not a registered well-known suffix. Its full
  value comes when an `openid-configuration` references it through `jwks_uri`.

#### `oauth-protected-resource`

RFC 9728 requires only `resource`. MCP clients request this document after a
401 to find `authorization_servers`. A document without an authorization
server makes the client start an OAuth flow that cannot complete. Do not serve
it before an issuer is known.

`JwtAccess` has a JWKS URL but no issuer identifier. The `ISSUER` verify form
from the auth design supplies it:

```sql
DEFINE ACCESS auth0 ON DATABASE TYPE JWT
    URL "https://tenant.auth0.com/.well-known/jwks.json"
    ISSUER "https://tenant.auth0.com/";
```

The server then serves:

```json
GET /.well-known/oauth-protected-resource/ns/foo/db/bar/mcp
{
  "resource": "https://host/ns/foo/db/bar/mcp",
  "authorization_servers": ["https://tenant.auth0.com/"]
}
```

and returns this on an unauthenticated MCP request:

```
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer resource_metadata="https://host/.well-known/oauth-protected-resource/ns/foo/db/bar/mcp"
```

The MCP client runs the OAuth flow against the external provider. SurrealDB
only verifies the resulting token, which the JWKS verifier already does.
SurrealDB does not act as an authorization server for this.

- `authorization_servers` lists every JWT access method with an `ISSUER` at the
  scope of the URL. With `/ac/{ac}` in the URL, it lists that method only.
- The `resource` value is the audience that RFC 8707 clients request. It
  identifies one database only when the scope is in the URL.
- `AUDIENCE` stays optional. An access method without it accepts a token
  minted for a different resource. Report this as a warning in
  `INFO ... STRUCTURE`, not as an error.

#### `openid-configuration` and `oauth-authorization-server`

Both documents require endpoints that the server does not have:

- RFC 8414: `authorization_endpoint`, `token_endpoint`, `response_types_supported`.
- OIDC Discovery adds `jwks_uri`, `subject_types_supported`,
  `id_token_signing_alg_values_supported`.

Clients trust these documents. Serve them only when SurrealDB brokers OAuth
flows.

#### MCP server discovery

No well-known document for MCP server discovery is standardized. Do not build
on the current proposals.

## 6. Delivery order

1. **Scoped routes.** Implemented: the rewrite, `PathScope`, the resolution
   rules, namespace, database and access-method scopes, scoped `signin` and
   `signup`.
2. **`kid` on issued tokens.** Derived from the public key hash. Required
   before step 3.
3. **`jwks.json`.** Behind the publication policy from section 5.
4. **`oauth-protected-resource` for `/mcp`.** After the `ISSUER` verify form
   exists, with the `WWW-Authenticate` challenge on `/mcp`.
5. **`openid-configuration` and `oauth-authorization-server`.** With the
   broker tier of the auth design.
