SHOW CHANGES FOR TABLE foo SINCE 1;
SHOW CHANGES FOR TABLE foo SINCE 1 LIMIT 1 + 1;
SHOW CHANGES FOR DATABASE SINCE 1 LIMIT 1 + 1;
SHOW CHANGES FOR DATABASE SINCE d"0000-01-01T00:00:00Z" LIMIT 1 + 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Show
            Show
                .target: ShowTarget::Table
                    Ident
                        .text: "foo"
                .since: ShowSince::VersionStamp
                    +1
                .limit: None
        -TopLevelExpr::Show
            Show
                .target: ShowTarget::Table
                    Ident
                        .text: "foo"
                .since: ShowSince::VersionStamp
                    +1
                .limit: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +1
        -TopLevelExpr::Show
            Show
                .target: ShowTarget::Database
                .since: ShowSince::VersionStamp
                    +1
                .limit: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +1
        -TopLevelExpr::Show
            Show
                .target: ShowTarget::Database
                .since: ShowSince::Timestamp
                    0000-01-01T00:00:00Z
                .limit: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +1
*/