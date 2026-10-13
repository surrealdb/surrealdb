DEFINE ACCESS account ON DATABASE TYPE RECORD AUTHENTICATE { RETURN $auth } CONTEXT { RETURN { org: 1 } } DURATION FOR TOKEN 18m, FOR SESSION 12h;
DEFINE ACCESS account ON DATABASE TYPE RECORD CONTEXT { RETURN { org: 1 } } DURATION FOR TOKEN 18m;
DEFINE ACCESS account ON DATABASE TYPE RECORD DURATION FOR TOKEN 18m CONTEXT { RETURN { org: 1 } };
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
                                .text: "account"
                            .parts: None
                    .base: Database
                    .comment: None
                    .duration_session: Some
                    Expr::Duration
                        43200s
                    .duration_token: Some
                    Expr::Duration
                        1080s
                    .duration_grant: None
                    .authenticate: Some
                    Expr::Block
                        Block
                            .exprs: Some
                                -Expr::Return
                                    Return
                                        .expr: Expr::Param
                                            Param
                                                .text: "auth"
                                        .fetch: None
                    .context: Some
                    Expr::Block
                        Block
                            .exprs: Some
                                -Expr::Return
                                    Return
                                        .expr: Expr::Object
                                            Object
                                                .entries: Some
                                                    -ObjectEntry
                                                        .key: "org"
                                                        .value: Expr::Integer
                                                            +1
                                        .fetch: None
                    .ty: Some
                    AccessType::Record
                        RecordAccess
                            .signup: None
                            .signin: None
                            .jwt: None
                            .refresh: false
        -TopLevelExpr::Expr
            Expr::DefineAccess
                DefineAccess
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "account"
                            .parts: None
                    .base: Database
                    .comment: None
                    .duration_session: None
                    .duration_token: Some
                    Expr::Duration
                        1080s
                    .duration_grant: None
                    .authenticate: None
                    .context: Some
                    Expr::Block
                        Block
                            .exprs: Some
                                -Expr::Return
                                    Return
                                        .expr: Expr::Object
                                            Object
                                                .entries: Some
                                                    -ObjectEntry
                                                        .key: "org"
                                                        .value: Expr::Integer
                                                            +1
                                        .fetch: None
                    .ty: Some
                    AccessType::Record
                        RecordAccess
                            .signup: None
                            .signin: None
                            .jwt: None
                            .refresh: false
        -TopLevelExpr::Expr
            Expr::DefineAccess
                DefineAccess
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "account"
                            .parts: None
                    .base: Database
                    .comment: None
                    .duration_session: None
                    .duration_token: Some
                    Expr::Duration
                        1080s
                    .duration_grant: None
                    .authenticate: None
                    .context: Some
                    Expr::Block
                        Block
                            .exprs: Some
                                -Expr::Return
                                    Return
                                        .expr: Expr::Object
                                            Object
                                                .entries: Some
                                                    -ObjectEntry
                                                        .key: "org"
                                                        .value: Expr::Integer
                                                            +1
                                        .fetch: None
                    .ty: Some
                    AccessType::Record
                        RecordAccess
                            .signup: None
                            .signin: None
                            .jwt: None
                            .refresh: false
*/