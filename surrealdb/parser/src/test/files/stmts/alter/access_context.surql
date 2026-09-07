ALTER ACCESS account ON DATABASE CONTEXT { RETURN { org: 1 } };
ALTER ACCESS account ON DATABASE DROP CONTEXT;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::AlterAccess
                AlterAccess
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "account"
                            .parts: None
                    .base: Database
                    .authenticate: None
                    .context: Some
                    AlterKind::Set
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
                    .grant_duration: None
                    .token_duration: None
                    .session_duration: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::AlterAccess
                AlterAccess
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "account"
                            .parts: None
                    .base: Database
                    .authenticate: None
                    .context: Some
                    AlterKind::Drop
                    .grant_duration: None
                    .token_duration: None
                    .session_duration: None
                    .comment: None
*/