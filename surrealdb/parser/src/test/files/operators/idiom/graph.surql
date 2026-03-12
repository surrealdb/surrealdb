->?->foo->bar;
foo<->?<-foo->bar;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Idiom
                                IdiomExpr
                                    .left: Expr::Document
                                    .op: IdiomOperator::GraphAny
                                        Out
                            .op: IdiomOperator::GraphTable
                                .direction: Out
                                .table: Ident
                                    .text: "foo"
                    .op: IdiomOperator::GraphTable
                        .direction: Out
                        .table: Ident
                            .text: "bar"
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Idiom
                                IdiomExpr
                                    .left: Expr::Path
                                        Path
                                            .start: Ident
                                                .text: "foo"
                                            .parts: None
                                    .op: IdiomOperator::GraphAny
                                        Both
                            .op: IdiomOperator::GraphTable
                                .direction: In
                                .table: Ident
                                    .text: "foo"
                    .op: IdiomOperator::GraphTable
                        .direction: Out
                        .table: Ident
                            .text: "bar"
*/