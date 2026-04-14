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
                                    .op: IdiomOperator::Graph
                                        .direction: Out
                                        .lookup: Lookup::Any
                            .op: IdiomOperator::Graph
                                .direction: Out
                                .lookup: Lookup::Subject
                                    LookupSubject::Table
                                        LookupSubjectTable
                                            .table: Ident
                                                .text: "foo"
                                            .field: None
                    .op: IdiomOperator::Graph
                        .direction: Out
                        .lookup: Lookup::Subject
                            LookupSubject::Table
                                LookupSubjectTable
                                    .table: Ident
                                        .text: "bar"
                                    .field: None
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
                                    .op: IdiomOperator::Graph
                                        .direction: Both
                                        .lookup: Lookup::Any
                            .op: IdiomOperator::Graph
                                .direction: In
                                .lookup: Lookup::Subject
                                    LookupSubject::Table
                                        LookupSubjectTable
                                            .table: Ident
                                                .text: "foo"
                                            .field: None
                    .op: IdiomOperator::Graph
                        .direction: Out
                        .lookup: Lookup::Subject
                            LookupSubject::Table
                                LookupSubjectTable
                                    .table: Ident
                                        .text: "bar"
                                    .field: None
*/