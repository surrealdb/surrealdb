DEFINE ACCESS acss ON DB TYPE JWT ALGORITHM HS512 KEY 1 + 1 AUDIENCE "aud1";
DEFINE ACCESS acss ON DB TYPE JWT URL 1 + 1 AUDIENCE "aud1", "aud2" WITH ISSUER ALGORITHM HS512 KEY 1 + 2;
DEFINE ACCESS acss ON DB TYPE RECORD WITH REFRESH WITH JWT ALGORITHM HS512 KEY 1 + 1 AUDIENCE "aud1";
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
                    AccessType::Jwt
                        Jwt
                            .verify: JwtVerify::Key
                                .algorithm: Algorithm::Hs512
                                .key: Expr::Binary
                                    BinaryExpr
                                        .left: Expr::Integer
                                            +1
                                        .op: BinaryOperator::Add
                                        .right: Expr::Integer
                                            +1
                            .issue: None
                            .audience: Some
                                -Expr::String
                                    StringLit
                                        .text: "aud1"
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
                    AccessType::Jwt
                        Jwt
                            .verify: JwtVerify::Jwks
                                .url: Expr::Binary
                                    BinaryExpr
                                        .left: Expr::Integer
                                            +1
                                        .op: BinaryOperator::Add
                                        .right: Expr::Integer
                                            +1
                            .issue: Some
                            JwtIssue
                                .algorithm: Some
                                Algorithm::Hs512
                                .key: Some
                                Expr::Binary
                                    BinaryExpr
                                        .left: Expr::Integer
                                            +1
                                        .op: BinaryOperator::Add
                                        .right: Expr::Integer
                                            +2
                            .audience: Some
                                -Expr::String
                                    StringLit
                                        .text: "aud1"
                                -Expr::String
                                    StringLit
                                        .text: "aud2"
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
                    AccessType::Record
                        RecordAccess
                            .signup: None
                            .signin: None
                            .jwt: Some
                            Jwt
                                .verify: JwtVerify::Key
                                    .algorithm: Algorithm::Hs512
                                    .key: Expr::Binary
                                        BinaryExpr
                                            .left: Expr::Integer
                                                +1
                                            .op: BinaryOperator::Add
                                            .right: Expr::Integer
                                                +1
                                .issue: None
                                .audience: Some
                                    -Expr::String
                                        StringLit
                                            .text: "aud1"
                            .refresh: true
*/