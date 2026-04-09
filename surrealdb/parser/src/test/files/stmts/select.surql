SELECT * FROM foo;
SELECT a as b, c from foo,bar;
SELECT * FROM ONLY foo;
SELECT * FROM ONLY foo WITH INDEX a,b;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 SPLIT ON a,b;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1 TIMEOUT 1s;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1 TIMEOUT 1s TEMPFILES;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1 TIMEOUT 1s EXPLAIN;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a START 1 LIMIT 1 VERSION 1 TIMEOUT 1s EXPLAIN;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1 FETCH a.b, c.d;
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1 FETCH type::fields(1,2);
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1 FETCH type::field(1);
SELECT * FROM ONLY foo WITH INDEX a,b WHERE 1==2 GROUP BY a,b ORDER BY a LIMIT 1 START 1 FETCH $p;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: false
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
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
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::Selector
                                Selector
                                    .expr: Expr::Path
                                        Path
                                            .start: Ident
                                                .text: "a"
                                            .parts: None
                                    .alias: Some
                                    Place::Field
                                        Ident
                                            .text: "b"
                            -ListSelector::Selector
                                Selector
                                    .expr: Expr::Path
                                        Path
                                            .start: Ident
                                                .text: "c"
                                            .parts: None
                                    .alias: None
                    .omit: None
                    .only: false
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "bar"
                                .parts: None
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
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
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
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: None
                    .split: None
                    .group: None
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: None
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: Some
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .group: None
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: None
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: None
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: None
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: None
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: Some
                    Expr::Integer
                        +1
                    .timeout: None
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: Some
                    Expr::Integer
                        +1
                    .timeout: Some
                    Expr::Duration
                        1s
                    .fetch: None
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: Some
                    Expr::Integer
                        +1
                    .timeout: Some
                    Expr::Duration
                        1s
                    .fetch: None
                    .tempfiles: true
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: Some
                    Expr::Integer
                        +1
                    .timeout: Some
                    Expr::Duration
                        1s
                    .fetch: None
                    .tempfiles: false
                    .explain: Some
                    ExplainClause::Base
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: Some
                    Expr::Integer
                        +1
                    .timeout: Some
                    Expr::Duration
                        1s
                    .fetch: None
                    .tempfiles: false
                    .explain: Some
                    ExplainClause::Base
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: None
                    .timeout: None
                    .fetch: Some
                        -Expr::Idiom
                            IdiomExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .op: IdiomOperator::Field
                                    Ident
                                        .text: "b"
                        -Expr::Idiom
                            IdiomExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "c"
                                        .parts: None
                                .op: IdiomOperator::Field
                                    Ident
                                        .text: "d"
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: None
                    .timeout: None
                    .fetch: Some
                        -Expr::Idiom
                            IdiomExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "type"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "fields"
                                .op: IdiomOperator::Call
                                    Some
                                        -Expr::Integer
                                            +1
                                        -Expr::Integer
                                            +2
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: None
                    .timeout: None
                    .fetch: Some
                        -Expr::Idiom
                            IdiomExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "type"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "field"
                                .op: IdiomOperator::Call
                                    Some
                                        -Expr::Integer
                                            +1
                    .tempfiles: false
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Select
                Select
                    .fields: Fields::List
                            -ListSelector::All
                    .omit: None
                    .only: true
                    .from: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +1
                            .op: ExactEqual
                            .right: Expr::Integer
                                +2
                    .split: None
                    .group: Some
                    Group::Fields
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                    .order: Some
                    OrderBy::List
                            -Order
                                .expr: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "a"
                                        .parts: None
                                .collate: false
                                .numeric: false
                                .direction: None
                    .start: Some
                    Expr::Integer
                        +1
                    .limit: Some
                    Expr::Integer
                        +1
                    .version: None
                    .timeout: None
                    .fetch: Some
                        -Expr::Param
                            Param
                                .text: "p"
                    .tempfiles: false
                    .explain: None
*/