SELECT ->knows:rand()..->person FROM person:1;
SELECT ->knows:ulid()..5->person FROM person:1;
SELECT ->knows:uuid()..=5->person FROM person:1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::Selector
                                Selector
                                    .expr: Expr::Idiom
                                        IdiomExpr
                                            .left: Expr::Idiom
                                                IdiomExpr
                                                    .left: Expr::Document
                                                    .op: IdiomOperator::Graph
                                                        .direction: Out
                                                        .lookup: Lookup::Subject
                                                            LookupSubject::Range
                                                                LookupSubjectRange
                                                                    .table: Ident
                                                                        .text: "knows"
                                                                    .range: RecordIdKeyRange
                                                                        .start: Bound::Included
                                                                            RecordIdKey::Generate
                                                                                Rand
                                                                        .end: Bound::Unbounded
                                                                    .field: None
                                            .op: IdiomOperator::Graph
                                                .direction: Out
                                                .lookup: Lookup::Subject
                                                    LookupSubject::Table
                                                        LookupSubjectTable
                                                            .table: Ident
                                                                .text: "person"
                                                            .field: None
                                    .alias: None
                    .omit: None
                    .only: false
                    .from: 
                        -Expr::RecordId
                            RecordId
                                .name: Ident
                                    .text: "person"
                                .key: RecordIdKey::Number
                                    1
                    .with_index: None
                    .condition: None
                    .split: None
                    .group: None
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .for_update: false
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::Selector
                                Selector
                                    .expr: Expr::Idiom
                                        IdiomExpr
                                            .left: Expr::Idiom
                                                IdiomExpr
                                                    .left: Expr::Document
                                                    .op: IdiomOperator::Graph
                                                        .direction: Out
                                                        .lookup: Lookup::Subject
                                                            LookupSubject::Range
                                                                LookupSubjectRange
                                                                    .table: Ident
                                                                        .text: "knows"
                                                                    .range: RecordIdKeyRange
                                                                        .start: Bound::Included
                                                                            RecordIdKey::Generate
                                                                                Ulid
                                                                        .end: Bound::Excluded
                                                                            RecordIdKey::Number
                                                                                5
                                                                    .field: None
                                            .op: IdiomOperator::Graph
                                                .direction: Out
                                                .lookup: Lookup::Subject
                                                    LookupSubject::Table
                                                        LookupSubjectTable
                                                            .table: Ident
                                                                .text: "person"
                                                            .field: None
                                    .alias: None
                    .omit: None
                    .only: false
                    .from: 
                        -Expr::RecordId
                            RecordId
                                .name: Ident
                                    .text: "person"
                                .key: RecordIdKey::Number
                                    1
                    .with_index: None
                    .condition: None
                    .split: None
                    .group: None
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .for_update: false
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::Selector
                                Selector
                                    .expr: Expr::Idiom
                                        IdiomExpr
                                            .left: Expr::Idiom
                                                IdiomExpr
                                                    .left: Expr::Document
                                                    .op: IdiomOperator::Graph
                                                        .direction: Out
                                                        .lookup: Lookup::Subject
                                                            LookupSubject::Range
                                                                LookupSubjectRange
                                                                    .table: Ident
                                                                        .text: "knows"
                                                                    .range: RecordIdKeyRange
                                                                        .start: Bound::Included
                                                                            RecordIdKey::Generate
                                                                                Uuid
                                                                        .end: Bound::Included
                                                                            RecordIdKey::Number
                                                                                5
                                                                    .field: None
                                            .op: IdiomOperator::Graph
                                                .direction: Out
                                                .lookup: Lookup::Subject
                                                    LookupSubject::Table
                                                        LookupSubjectTable
                                                            .table: Ident
                                                                .text: "person"
                                                            .field: None
                                    .alias: None
                    .omit: None
                    .only: false
                    .from: 
                        -Expr::RecordId
                            RecordId
                                .name: Ident
                                    .text: "person"
                                .key: RecordIdKey::Number
                                    1
                    .with_index: None
                    .condition: None
                    .split: None
                    .group: None
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .for_update: false
                    .tempfiles: false
                    .explain: None
*/