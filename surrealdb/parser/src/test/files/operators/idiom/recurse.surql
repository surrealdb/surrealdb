a.{1..3}(->knows->person)
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .op: IdiomOperator::Recurse
                        Recurse
                            .start: Bound::Included
                                +1
                            .end: Bound::Excluded
                                +3
                            .kind: None
                            .expr: Expr::Idiom
                                IdiomExpr
                                    .left: Expr::Idiom
                                        IdiomExpr
                                            .left: Expr::Document
                                            .op: IdiomOperator::Graph
                                                .direction: Out
                                                .lookup: Lookup::Subject
                                                    LookupSubject::Table
                                                        LookupSubjectTable
                                                            .table: Ident
                                                                .text: "knows"
                                                            .field: None
                                    .op: IdiomOperator::Graph
                                        .direction: Out
                                        .lookup: Lookup::Subject
                                            LookupSubject::Table
                                                LookupSubjectTable
                                                    .table: Ident
                                                        .text: "person"
                                                    .field: None
*/