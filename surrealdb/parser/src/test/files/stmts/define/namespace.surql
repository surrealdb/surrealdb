DEFINE NAMESPACE ns COMMENT "comment";
DEFINE NAMESPACE IF NOT EXISTS 1;
DEFINE NAMESPACE OVERWRITE "n" + "s";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineNamespace
                DefineNamespace
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "ns"
                            .parts: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "comment"
        -TopLevelExpr::Expr
            Expr::DefineNamespace
                DefineNamespace
                    .kind: IfNotExists
                    .name: Expr::Integer
                        +1
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineNamespace
                DefineNamespace
                    .kind: Overwrite
                    .name: Expr::Binary
                        BinaryExpr
                            .left: Expr::String
                                StringLit
                                    .text: "n"
                            .op: Add
                            .right: Expr::String
                                StringLit
                                    .text: "s"
                    .comment: None
*/