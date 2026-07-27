// None of these fields are created.
DEFINE FIELD OVERWRITE a.? ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE ->a ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a... ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a[WHERE false] ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a[? TRUE] ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a.{b} ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a.{b: 1} ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a->b ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a->b->c ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a<~b ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a<~? ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a<~(select foo from bar as baz) ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a.@ ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a.* ON bar DEFAULT 1;
DEFINE FIELD OVERWRITE a.? ON bar DEFAULT 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Option
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Document
                            .op: IdiomOperator::Graph
                                .direction: Out
                                .lookup: Lookup::Subject
                                    LookupSubject::Table
                                        LookupSubjectTable
                                            .table: Ident
                                                .text: "a"
                                            .field: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Flatten
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Where
                                Expr::Builtin
                                    Builtin::False
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Where
                                Expr::Builtin
                                    Builtin::True
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Destructure
                                Some
                                    -Destructure
                                        .field: Ident
                                            .text: "b"
                                        .op: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Destructure
                                Some
                                    -Destructure
                                        .field: Ident
                                            .text: "b"
                                        .op: Some
                                        DestructureOperator::Expr
                                            Expr::Integer
                                                +1
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Graph
                                .direction: Out
                                .lookup: Lookup::Subject
                                    LookupSubject::Table
                                        LookupSubjectTable
                                            .table: Ident
                                                .text: "b"
                                            .field: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Idiom
                                IdiomExpr
                                    .left: Expr::Path
                                        Path
                                            .start: Ident
                                                .text: "a"
                                            .parts: None
                                    .op: IdiomOperator::Graph
                                        .direction: Out
                                        .lookup: Lookup::Subject
                                            LookupSubject::Table
                                                LookupSubjectTable
                                                    .table: Ident
                                                        .text: "b"
                                                    .field: None
                            .op: IdiomOperator::Graph
                                .direction: Out
                                .lookup: Lookup::Subject
                                    LookupSubject::Table
                                        LookupSubjectTable
                                            .table: Ident
                                                .text: "c"
                                            .field: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Reference
                                Lookup::Subject
                                    LookupSubject::Table
                                        LookupSubjectTable
                                            .table: Ident
                                                .text: "b"
                                            .field: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Reference
                                Lookup::Any
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Reference
                                Lookup::Select
                                    SelectLookup
                                        .fields: Fields::List
                                                -ListSelector::Selector
                                                    Selector
                                                        .expr: Expr::Path
                                                            Path
                                                                .start: Ident
                                                                    .text: "foo"
                                                                .parts: None
                                                        .alias: None
                                        .only: false
                                        .from: Some
                                            -LookupSubject::Table
                                                LookupSubjectTable
                                                    .table: Ident
                                                        .text: "bar"
                                                    .field: None
                                        .condition: None
                                        .split: None
                                        .group: None
                                        .order: None
                                        .limit: None
                                        .start: None
                                        .alias: Some
                                        PresentPlace::Field
                                            Ident
                                                .text: "baz"
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Repeat
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::All
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
        -TopLevelExpr::Expr
            Expr::DefineField
                DefineField
                    .kind: Overwrite
                    .name: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Option
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "bar"
                            .parts: None
                    .ty: None
                    .flexible: false
                    .readonly: false
                    .value: None
                    .assert: None
                    .computed: None
                    .default: Some
                    FieldDefault::Some
                        Expr::Integer
                            +1
                    .permissions: None
                    .comment: None
                    .on_delete: None
*/