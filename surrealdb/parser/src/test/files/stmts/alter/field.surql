ALTER FIELD score ON likes TYPE number;
ALTER FIELD IF EXISTS score ON likes READONLY INLINE;
ALTER FIELD score ON likes DROP INLINE;
ALTER FIELD score ON likes DROP READONLY COMMENT "c";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::AlterField
                AlterField
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "score"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "likes"
                            .parts: None
                    .ty: Some
                    AlterKind::Set
                        Type::Prime
                                -PrimeType::Number
                    .flexible: None
                    .readonly: None
                    .inline: None
                    .value: None
                    .assert: None
                    .default: None
                    .on_delete: None
                    .comment: None
                    .permissions: None
        -TopLevelExpr::Expr
            Expr::AlterField
                AlterField
                    .if_exists: true
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "score"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "likes"
                            .parts: None
                    .ty: None
                    .flexible: None
                    .readonly: Some
                    AlterKind::Set
                        ()
                    .inline: Some
                    AlterKind::Set
                        ()
                    .value: None
                    .assert: None
                    .default: None
                    .on_delete: None
                    .comment: None
                    .permissions: None
        -TopLevelExpr::Expr
            Expr::AlterField
                AlterField
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "score"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "likes"
                            .parts: None
                    .ty: None
                    .flexible: None
                    .readonly: None
                    .inline: Some
                    AlterKind::Drop
                    .value: None
                    .assert: None
                    .default: None
                    .on_delete: None
                    .comment: None
                    .permissions: None
        -TopLevelExpr::Expr
            Expr::AlterField
                AlterField
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "score"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "likes"
                            .parts: None
                    .ty: None
                    .flexible: None
                    .readonly: Some
                    AlterKind::Drop
                    .inline: None
                    .value: None
                    .assert: None
                    .default: None
                    .on_delete: None
                    .comment: Some
                    AlterKind::Set
                        Expr::String
                            StringLit
                                .text: "c"
                    .permissions: None
*/