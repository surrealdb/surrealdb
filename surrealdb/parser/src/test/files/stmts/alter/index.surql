ALTER INDEX IF EXISTS a ON b COMMENT 1;
ALTER INDEX a ON b COMMENT 1;
ALTER INDEX a ON b PREPARE REMOVE;
ALTER INDEX a ON b DROP COMMENT;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::AlterIndex
                AlterIndex
                    .if_exists: true
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .comment: Some
                    AlterKind::Set
                        Expr::Integer
                            +1
                    .prepare_remove: false
        -TopLevelExpr::Expr
            Expr::AlterIndex
                AlterIndex
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .comment: Some
                    AlterKind::Set
                        Expr::Integer
                            +1
                    .prepare_remove: false
        -TopLevelExpr::Expr
            Expr::AlterIndex
                AlterIndex
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .comment: None
                    .prepare_remove: true
        -TopLevelExpr::Expr
            Expr::AlterIndex
                AlterIndex
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .comment: Some
                    AlterKind::Drop
                    .prepare_remove: false
*/