ALTER SYSTEM COMPACT;
ALTER SYSTEM QUERY_TIMEOUT 1s;
ALTER SYSTEM DROP QUERY_TIMEOUT;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::AlterSystem
                AlterSystem
                    .query_timeout: None
                    .compact: true
        -TopLevelExpr::Expr
            Expr::AlterSystem
                AlterSystem
                    .query_timeout: Some
                    AlterKind::Set
                        Expr::Duration
                            1s
                    .compact: false
        -TopLevelExpr::Expr
            Expr::AlterSystem
                AlterSystem
                    .query_timeout: Some
                    AlterKind::Drop
                    .compact: false
*/