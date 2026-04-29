ALTER DATABASE a COMPACT;
ALTER DATABASE 1 + 1;
ALTER DATABASE IF EXISTS 1 + 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::AlterDatabase
                AlterDatabase
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .compact: true
        -TopLevelExpr::Expr
            Expr::AlterDatabase
                AlterDatabase
                    .if_exists: false
                    .name: Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +1
                    .compact: false
        -TopLevelExpr::Expr
            Expr::AlterDatabase
                AlterDatabase
                    .if_exists: true
                    .name: Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +1
                    .compact: false
*/