DEFINE BUCKET OVERWRITE name;
DEFINE BUCKET name COMMENT "comment";
DEFINE BUCKET name PERMISSIONS FULL COMMENT "comment";
DEFINE BUCKET name READONLY PERMISSIONS FULL;
DEFINE BUCKET name BACKEND 1 + 2 READONLY;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineBucket
                DefineBucket
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "name"
                            .parts: None
                    .backend: None
                    .permission: None
                    .comment: None
                    .readonly: false
        -TopLevelExpr::Expr
            Expr::DefineBucket
                DefineBucket
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "name"
                            .parts: None
                    .backend: None
                    .permission: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "comment"
                    .readonly: false
        -TopLevelExpr::Expr
            Expr::DefineBucket
                DefineBucket
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "name"
                            .parts: None
                    .backend: None
                    .permission: Some
                    Permission::Full
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "comment"
                    .readonly: false
        -TopLevelExpr::Expr
            Expr::DefineBucket
                DefineBucket
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "name"
                            .parts: None
                    .backend: None
                    .permission: Some
                    Permission::Full
                    .comment: None
                    .readonly: true
        -TopLevelExpr::Expr
            Expr::DefineBucket
                DefineBucket
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "name"
                            .parts: None
                    .backend: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: Add
                            .right: Expr::Integer
                                +2
                    .permission: None
                    .comment: None
                    .readonly: true
*/