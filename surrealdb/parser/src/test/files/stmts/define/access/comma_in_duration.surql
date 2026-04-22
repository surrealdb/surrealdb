DEFINE ACCESS account ON DATABASE TYPE RECORD DURATION FOR TOKEN 18m, FOR SESSION 12h;
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
                    .authenticate: None
                    .ty: Some
                    AccessType::Record
                        RecordAccess
                            .signup: None
                            .signin: None
                            .jwt: None
                            .refresh: false
*/