DEFINE FUNCTION fn::a() { RETURN 1 } GRAPHQL_ALIAS "a1";
DEFINE FUNCTION fn::a() { RETURN 1 } GRAPHQL_DEPRECATED "old" GRAPHQL_ALIAS "a2";
DEFINE FUNCTION fn::a() { RETURN 1 } COMMENT "c" PERMISSIONS FULL GRAPHQL_ALIAS "a3";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                    .parameters: None
                    .return_ty: None
                    .body: Block
                        .exprs: Some
                            -Expr::Return
                                Return
                                    .expr: Expr::Integer
                                        +1
                                    .fetch: None
                    .comment: None
                    .permission: None
                    .graphql_alias: Some
                    StringLit
                        .text: "a1"
                    .graphql_deprecated: None
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                    .parameters: None
                    .return_ty: None
                    .body: Block
                        .exprs: Some
                            -Expr::Return
                                Return
                                    .expr: Expr::Integer
                                        +1
                                    .fetch: None
                    .comment: None
                    .permission: None
                    .graphql_alias: Some
                    StringLit
                        .text: "a2"
                    .graphql_deprecated: Some
                    StringLit
                        .text: "old"
        -TopLevelExpr::Expr
            Expr::DefineFunction
                DefineFunction
                    .kind: Create
                    .name: Path
                        .start: Ident
                            .text: "fn"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "a"
                    .parameters: None
                    .return_ty: None
                    .body: Block
                        .exprs: Some
                            -Expr::Return
                                Return
                                    .expr: Expr::Integer
                                        +1
                                    .fetch: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "c"
                    .permission: Some
                    Permission::Full
                    .graphql_alias: Some
                    StringLit
                        .text: "a3"
                    .graphql_deprecated: None
*/