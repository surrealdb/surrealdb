DEFINE ACCESS acss ON ROOT;
DEFINE ACCESS acss ON NAMESPACE;
DEFINE ACCESS acss ON DATABASE;
DEFINE ACCESS acss ON DB COMMENT "foo";
DEFINE ACCESS acss ON DB AUTHENTICATE 1 + 1;
DEFINE ACCESS acss ON DB DURATION FOR SESSION 1s FOR TOKEN 1s DURATION FOR GRANT 1s;
DEFINE ACCESS acss ON DB TYPE JWT ALGORITHM EDDSA KEY 1 + 1 WITH ISSUER ALGORITHM EDDSA KEY 1 + 2;
DEFINE ACCESS acss ON DB TYPE JWT URL 1 + 1;
DEFINE ACCESS acss ON DB TYPE RECORD SIGNIN 1 + 1 SIGNUP 1 + 2 WITH REFRESH WITH JWT URL 1 + 3;
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
                    .base: Root
                    .comment: None
                    .duration_session: None
                    .duration_token: None
                    .duration_grant: None
                    .authenticate: None
                    .ty: None
        -TopLevelExpr::Expr
            Expr::DefineAccess
                DefineAccess
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "acss"
                            .parts: None
                    .base: Namespace
                    .comment: None
                    .duration_session: None
                    .duration_token: None
                    .duration_grant: None
                    .authenticate: None
                    .ty: None
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
                    .ty: None
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
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "foo"
                    .duration_session: None
                    .duration_token: None
                    .duration_grant: None
                    .authenticate: None
                    .ty: None
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
                    .authenticate: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +1
                    .ty: None
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
                    .duration_session: Some
                    Expr::Duration
                        1s
                    .duration_token: Some
                    Expr::Duration
                        1s
                    .duration_grant: Some
                    Expr::Duration
                        1s
                    .authenticate: None
                    .ty: None
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
                    .ty: Some
                    AccessType::Jwt
                        Jwt
                            .verify: JwtVerify::Key
                                .algorithm: Algorithm::EdDSA
                                .key: Expr::Binary
                                    BinaryExpr
                                        .left: Expr::Integer
                                            +1
                                        .op: Add
                                        .right: Expr::Integer
                                            +1
                            .issue: Some
                            JwtIssue
                                .algorithm: Some
                                Algorithm::EdDSA
                                .key: Some
                                Expr::Binary
                                    BinaryExpr
                                        .left: Expr::Integer
                                            +1
                                        .op: Add
                                        .right: Expr::Integer
                                            +2
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
                    .ty: Some
                    AccessType::Jwt
                        Jwt
                            .verify: JwtVerify::Jwks
                                .url: Expr::Binary
                                    BinaryExpr
                                        .left: Expr::Integer
                                            +1
                                        .op: Add
                                        .right: Expr::Integer
                                            +1
                            .issue: None
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
                    .ty: Some
                    AccessType::Record
                        RecordAccess
                            .signup: Some
                            Expr::Binary
                                BinaryExpr
                                    .left: Expr::Integer
                                        +1
                                    .op: Add
                                    .right: Expr::Integer
                                        +2
                            .signin: Some
                            Expr::Binary
                                BinaryExpr
                                    .left: Expr::Integer
                                        +1
                                    .op: Add
                                    .right: Expr::Integer
                                        +1
                            .jwt: Some
                            Jwt
                                .verify: JwtVerify::Jwks
                                    .url: Expr::Binary
                                        BinaryExpr
                                            .left: Expr::Integer
                                                +1
                                            .op: Add
                                            .right: Expr::Integer
                                                +3
                                .issue: None
                            .refresh: true
*/