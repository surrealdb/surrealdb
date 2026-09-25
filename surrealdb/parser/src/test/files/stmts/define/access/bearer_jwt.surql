DEFINE ACCESS acss ON DB TYPE BEARER FOR USER WITH JWT ALGORITHM HS512 KEY "secret";
DEFINE ACCESS acss ON DB TYPE BEARER FOR RECORD WITH JWT ALGORITHM HS512 KEY "secret" WITH ISSUER KEY "issuer";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineAccess
                DefineAccess
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "acss"
                            .parts: None
                    .base: Database
                    .comment: None
                    .duration_session: None
                    .duration_token: None
                    .duration_grant: None
                    .authenticate: None
                    .context: None
                    .ty: Some
                    AccessType::Bearer
                        BearerAccess
                            .subject: BearerAccessSubject::User
                            .jwt: Some
                            Jwt
                                .verify: JwtVerify::Key
                                    .algorithm: Algorithm::Hs512
                                    .key: Expr::String
                                        StringLit
                                            .text: "secret"
                                .issue: None
                                .audience: None
        -TopLevelExpr::Expr
            Expr::DefineAccess
                DefineAccess
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "acss"
                            .parts: None
                    .base: Database
                    .comment: None
                    .duration_session: None
                    .duration_token: None
                    .duration_grant: None
                    .authenticate: None
                    .context: None
                    .ty: Some
                    AccessType::Bearer
                        BearerAccess
                            .subject: BearerAccessSubject::Record
                            .jwt: Some
                            Jwt
                                .verify: JwtVerify::Key
                                    .algorithm: Algorithm::Hs512
                                    .key: Expr::String
                                        StringLit
                                            .text: "secret"
                                .issue: Some
                                JwtIssue
                                    .algorithm: None
                                    .key: Some
                                    Expr::String
                                        StringLit
                                            .text: "issuer"
                                .audience: None
*/