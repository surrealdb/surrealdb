// Copyright © 2016 Abcum Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"github.com/abcum/cork"
	"github.com/abcum/surreal/util/pack"
)

// ##################################################
// ##################################################
// ##################################################
// ##################################################
// ##################################################

// --------------------------------------------------
// Token
// --------------------------------------------------

func (this Token) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

func (this Token) MarshalBinary() (data []byte, err error) {
	return []byte(this.String()), err
}

func (this *Token) UnmarshalBinary(data []byte) (err error) {
	*this = newToken(string(data))
	return err
}

// --------------------------------------------------
// ALL
// --------------------------------------------------

func init() {
	cork.Register(&All{})
}

func (this *All) ExtendCORK() byte {
	return 0x01
}

func (this *All) MarshalCORK() (dst []byte, err error) {
	return
}

func (this *All) UnmarshalCORK(src []byte) (err error) {
	return
}

func (this All) MarshalText() (data []byte, err error) {
	return []byte("*"), err
}

// --------------------------------------------------
// ANY
// --------------------------------------------------

func init() {
	cork.Register(&Any{})
}

func (this *Any) ExtendCORK() byte {
	return 0x02
}

func (this *Any) MarshalCORK() (dst []byte, err error) {
	return
}

func (this *Any) UnmarshalCORK(src []byte) (err error) {
	return
}

func (this Any) MarshalText() (data []byte, err error) {
	return []byte("?"), err
}

// --------------------------------------------------
// NULL
// --------------------------------------------------

func init() {
	cork.Register(&Null{})
}

func (this *Null) ExtendCORK() byte {
	return 0x05
}

func (this *Null) MarshalCORK() (dst []byte, err error) {
	return
}

func (this *Null) UnmarshalCORK(src []byte) (err error) {
	return
}

func (this Null) MarshalText() (data []byte, err error) {
	return []byte("null"), err
}

// --------------------------------------------------
// VOID
// --------------------------------------------------

func init() {
	cork.Register(&Void{})
}

func (this *Void) ExtendCORK() byte {
	return 0x06
}

func (this *Void) MarshalCORK() (dst []byte, err error) {
	return
}

func (this *Void) UnmarshalCORK(src []byte) (err error) {
	return
}

func (this Void) MarshalText() (data []byte, err error) {
	return []byte("~VOID~"), err
}

// --------------------------------------------------
// EMPTY
// --------------------------------------------------

func init() {
	cork.Register(&Empty{})
}

func (this *Empty) ExtendCORK() byte {
	return 0x07
}

func (this *Empty) MarshalCORK() (dst []byte, err error) {
	return
}

func (this *Empty) UnmarshalCORK(src []byte) (err error) {
	return
}

func (this Empty) MarshalText() (data []byte, err error) {
	return []byte("~EMPTY~"), err
}

// --------------------------------------------------
// FIELD
// --------------------------------------------------

func init() {
	cork.Register(&Field{})
}

func (this *Field) ExtendCORK() byte {
	return 0x08
}

func (this *Field) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Expr)
	w.EncodeString(this.Field)
	w.EncodeString(this.Alias)
	return
}

func (this *Field) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Expr)
	r.DecodeString(&this.Field)
	r.DecodeString(&this.Alias)
	return
}

// --------------------------------------------------
// GROUP
// --------------------------------------------------

func init() {
	cork.Register(&Group{})
}

func (this *Group) ExtendCORK() byte {
	return 0x09
}

func (this *Group) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Expr)
	return
}

func (this *Group) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Expr)
	return
}

// --------------------------------------------------
// ORDER
// --------------------------------------------------

func init() {
	cork.Register(&Order{})
}

func (this *Order) ExtendCORK() byte {
	return 0x10
}

func (this *Order) MarshalCORK(w *cork.Writer) (dst []byte, err error) {
	w.EncodeAny(this.Expr)
	w.EncodeAny(this.Dir)
	return
}

func (this *Order) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Expr)
	r.DecodeAny(&this.Dir)
	return
}

// --------------------------------------------------
// FETCH
// --------------------------------------------------

func init() {
	cork.Register(&Fetch{})
}

func (this *Fetch) ExtendCORK() byte {
	return 0x11
}

func (this *Fetch) MarshalCORK(w *cork.Writer) (dst []byte, err error) {
	w.EncodeAny(this.Expr)
	return
}

func (this *Fetch) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Expr)
	return
}

// ##################################################
// ##################################################
// ##################################################
// ##################################################
// ##################################################

// --------------------------------------------------
// SubExpression
// --------------------------------------------------

func init() {
	cork.Register(&SubExpression{})
}

func (this *SubExpression) ExtendCORK() byte {
	return 0x21
}

func (this *SubExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Expr)
	return
}

func (this *SubExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Expr)
	return
}

// --------------------------------------------------
// MultExpression
// --------------------------------------------------

func init() {
	cork.Register(&MultExpression{})
}

func (this *MultExpression) ExtendCORK() byte {
	return 0x22
}

func (this *MultExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Expr)
	return
}

func (this *MultExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Expr)
	return
}

// --------------------------------------------------
// IfelExpression
// --------------------------------------------------

func init() {
	cork.Register(&IfelExpression{})
}

func (this *IfelExpression) ExtendCORK() byte {
	return 0x23
}

func (this *IfelExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Cond)
	w.EncodeAny(this.Then)
	w.EncodeAny(this.Else)
	return
}

func (this *IfelExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Cond)
	r.DecodeAny(&this.Then)
	r.DecodeAny(&this.Else)
	return
}

// --------------------------------------------------
// FuncExpression
// --------------------------------------------------

func init() {
	cork.Register(&FuncExpression{})
}

func (this *FuncExpression) ExtendCORK() byte {
	return 0x24
}

func (this *FuncExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.Name)
	w.EncodeAny(this.Args)
	w.EncodeBool(this.Aggr)
	return
}

func (this *FuncExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.Name)
	r.DecodeAny(&this.Args)
	r.DecodeBool(&this.Aggr)
	return
}

// --------------------------------------------------
// ItemExpression
// --------------------------------------------------

func init() {
	cork.Register(&ItemExpression{})
}

func (this *ItemExpression) ExtendCORK() byte {
	return 0x25
}

func (this *ItemExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.LHS)
	w.EncodeAny(this.Op)
	w.EncodeAny(this.RHS)
	return
}

func (this *ItemExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.LHS)
	r.DecodeAny(&this.Op)
	r.DecodeAny(&this.RHS)
	return
}

// --------------------------------------------------
// BinaryExpression
// --------------------------------------------------

func init() {
	cork.Register(&BinaryExpression{})
}

func (this *BinaryExpression) ExtendCORK() byte {
	return 0x26
}

func (this *BinaryExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.LHS)
	w.EncodeAny(this.Op)
	w.EncodeAny(this.RHS)
	return
}

func (this *BinaryExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.LHS)
	r.DecodeAny(&this.Op)
	r.DecodeAny(&this.RHS)
	return
}

// --------------------------------------------------
// PathExpression
// --------------------------------------------------

func init() {
	cork.Register(&PathExpression{})
}

func (this *PathExpression) ExtendCORK() byte {
	return 0x27
}

func (this *PathExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Expr)
	return
}

func (this *PathExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Expr)
	return
}

// --------------------------------------------------
// PartExpression
// --------------------------------------------------

func init() {
	cork.Register(&PartExpression{})
}

func (this *PartExpression) ExtendCORK() byte {
	return 0x28
}

func (this *PartExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Part)
	return
}

func (this *PartExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Part)
	return
}

// --------------------------------------------------
// JoinExpression
// --------------------------------------------------

func init() {
	cork.Register(&JoinExpression{})
}

func (this *JoinExpression) ExtendCORK() byte {
	return 0x29
}

func (this *JoinExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Join)
	return
}

func (this *JoinExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Join)
	return
}

// --------------------------------------------------
// SubpExpression
// --------------------------------------------------

func init() {
	cork.Register(&SubpExpression{})
}

func (this *SubpExpression) ExtendCORK() byte {
	return 0x30
}

func (this *SubpExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.What)
	w.EncodeAny(this.Name)
	w.EncodeAny(this.Cond)
	return
}

func (this *SubpExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.What)
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.Cond)
	return
}

// --------------------------------------------------
// PermExpression
// --------------------------------------------------

func init() {
	cork.Register(&PermExpression{})
}

func (this *PermExpression) ExtendCORK() byte {
	return 0x31
}

func (this *PermExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Select)
	w.EncodeAny(this.Create)
	w.EncodeAny(this.Update)
	w.EncodeAny(this.Delete)
	return
}

func (this *PermExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Select)
	r.DecodeAny(&this.Create)
	r.DecodeAny(&this.Update)
	r.DecodeAny(&this.Delete)
	return
}

// --------------------------------------------------
// DataExpression
// --------------------------------------------------

func init() {
	cork.Register(&DataExpression{})
}

func (this *DataExpression) ExtendCORK() byte {
	return 0x32
}

func (this *DataExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Data)
	return
}

func (this *DataExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Data)
	return
}

// --------------------------------------------------
// DiffExpression
// --------------------------------------------------

func init() {
	cork.Register(&DiffExpression{})
}

func (this *DiffExpression) ExtendCORK() byte {
	return 0x33
}

func (this *DiffExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Data)
	return
}

func (this *DiffExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Data)
	return
}

// --------------------------------------------------
// MergeExpression
// --------------------------------------------------

func init() {
	cork.Register(&MergeExpression{})
}

func (this *MergeExpression) ExtendCORK() byte {
	return 0x34
}

func (this *MergeExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Data)
	return
}

func (this *MergeExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Data)
	return
}

// --------------------------------------------------
// ContentExpression
// --------------------------------------------------

func init() {
	cork.Register(&ContentExpression{})
}

func (this *ContentExpression) ExtendCORK() byte {
	return 0x35
}

func (this *ContentExpression) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Data)
	return
}

func (this *ContentExpression) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Data)
	return
}

// ##################################################
// ##################################################
// ##################################################
// ##################################################
// ##################################################

// --------------------------------------------------
// Model
// --------------------------------------------------

func init() {
	cork.Register(&Model{})
}

func (this *Model) ExtendCORK() byte {
	return 0x51
}

func (this *Model) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.TB)
	w.EncodeFloat64(this.MIN)
	w.EncodeFloat64(this.INC)
	w.EncodeFloat64(this.MAX)
	return
}

func (this *Model) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.TB)
	r.DecodeFloat64(&this.MIN)
	r.DecodeFloat64(&this.INC)
	r.DecodeFloat64(&this.MAX)
	return
}

func (this Model) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

// --------------------------------------------------
// Param
// --------------------------------------------------

func init() {
	cork.Register(&Param{})
}

func (this *Param) ExtendCORK() byte {
	return 0x52
}

func (this *Param) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.ID)
	return
}

func (this *Param) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.ID)
	return
}

func (this Param) MarshalText() (data []byte, err error) {
	return []byte("ID:" + this.ID), err
}

// --------------------------------------------------
// Regex
// --------------------------------------------------

func init() {
	cork.Register(&Regex{})
}

func (this *Regex) ExtendCORK() byte {
	return 0x53
}

func (this *Regex) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.ID)
	return
}

func (this *Regex) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.ID)
	return
}

func (this Regex) MarshalText() (data []byte, err error) {
	return []byte("ID:" + this.ID), err
}

// --------------------------------------------------
// Value
// --------------------------------------------------

func init() {
	cork.Register(&Value{})
}

func (this *Value) ExtendCORK() byte {
	return 0x54
}

func (this *Value) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.ID)
	return
}

func (this *Value) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.ID)
	return
}

func (this Value) MarshalText() (data []byte, err error) {
	return []byte(this.ID), err
}

// --------------------------------------------------
// Ident
// --------------------------------------------------

func init() {
	cork.Register(&Ident{})
}

func (this *Ident) ExtendCORK() byte {
	return 0x55
}

func (this *Ident) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.ID)
	return
}

func (this *Ident) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.ID)
	return
}

func (this Ident) MarshalText() (data []byte, err error) {
	return []byte("ID:" + this.ID), err
}

// --------------------------------------------------
// Table
// --------------------------------------------------

func init() {
	cork.Register(&Table{})
}

func (this *Table) ExtendCORK() byte {
	return 0x56
}

func (this *Table) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.TB)
	return
}

func (this *Table) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.TB)
	return
}

func (this Table) MarshalText() (data []byte, err error) {
	return []byte("TB:" + this.TB), err
}

// --------------------------------------------------
// Batch
// --------------------------------------------------

func init() {
	cork.Register(&Batch{})
}

func (this *Batch) ExtendCORK() byte {
	return 0x57
}

func (this *Batch) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.BA)
	return
}

func (this *Batch) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.BA)
	return
}

func (this Batch) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

// --------------------------------------------------
// Thing
// --------------------------------------------------

func init() {
	cork.Register(&Thing{})
}

func (this Thing) Bytes() []byte {
	return []byte(this.String())
}

func (this *Thing) ExtendCORK() byte {
	return 0x58
}

func (this *Thing) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.TB)
	w.EncodeAny(this.ID)
	return
}

func (this *Thing) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.TB)
	r.DecodeAny(&this.ID)
	return
}

func (this Thing) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

// --------------------------------------------------
// Point
// --------------------------------------------------

func init() {
	cork.Register(&Point{})
}

func (this *Point) ExtendCORK() byte {
	return 0x59
}

func (this *Point) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeFloat64(this.LA)
	w.EncodeFloat64(this.LO)
	return
}

func (this *Point) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeFloat64(&this.LA)
	r.DecodeFloat64(&this.LO)
	return
}

func (this Point) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

func (this Point) MarshalJSON() (data []byte, err error) {
	return []byte(this.JSON()), err
}

// --------------------------------------------------
// Circle
// --------------------------------------------------

func init() {
	cork.Register(&Circle{})
}

func (this *Circle) ExtendCORK() byte {
	return 0x60
}

func (this *Circle) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.CE)
	w.EncodeFloat64(this.RA)
	return
}

func (this *Circle) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.CE)
	r.DecodeFloat64(&this.RA)
	return
}

func (this Circle) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

func (this Circle) MarshalJSON() (data []byte, err error) {
	return []byte(this.JSON()), err
}

// --------------------------------------------------
// Polygon
// --------------------------------------------------

func init() {
	cork.Register(&Polygon{})
}

func (this *Polygon) ExtendCORK() byte {
	return 0x61
}

func (this *Polygon) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.PS)
	return
}

func (this *Polygon) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.PS)
	return
}

func (this Polygon) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

func (this Polygon) MarshalJSON() (data []byte, err error) {
	return []byte(this.JSON()), err
}

// ##################################################
// ##################################################
// ##################################################
// ##################################################
// ##################################################

// --------------------------------------------------
// IfStatement
// --------------------------------------------------

func init() {
	cork.Register(&IfStatement{})
}

func (this *IfStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *IfStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *IfStatement) ExtendCORK() byte {
	return 0x71
}

func (this *IfStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Cond)
	w.EncodeAny(this.Then)
	w.EncodeAny(this.Else)
	return
}

func (this *IfStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Cond)
	r.DecodeAny(&this.Then)
	r.DecodeAny(&this.Else)
	return
}

// --------------------------------------------------
// RunStatement
// --------------------------------------------------

func init() {
	cork.Register(&RunStatement{})
}

func (this *RunStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RunStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RunStatement) ExtendCORK() byte {
	return 0x72
}

func (this *RunStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Expr)
	return
}

func (this *RunStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Expr)
	return
}

// --------------------------------------------------
// LiveStatement
// --------------------------------------------------

func init() {
	cork.Register(&LiveStatement{})
}

func (this *LiveStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *LiveStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *LiveStatement) ExtendCORK() byte {
	return 0x73
}

func (this *LiveStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.FB)
	w.EncodeString(this.ID)
	w.EncodeBool(this.Diff)
	w.EncodeAny(this.Expr)
	w.EncodeAny(this.What)
	w.EncodeAny(this.Cond)
	return
}

func (this *LiveStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.FB)
	r.DecodeString(&this.ID)
	r.DecodeBool(&this.Diff)
	r.DecodeAny(&this.Expr)
	r.DecodeAny(&this.What)
	r.DecodeAny(&this.Cond)
	return
}

// --------------------------------------------------
// SelectStatement
// --------------------------------------------------

func init() {
	cork.Register(&SelectStatement{})
}

func (this *SelectStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *SelectStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *SelectStatement) ExtendCORK() byte {
	return 0x74
}

func (this *SelectStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeBool(this.RW)
	w.EncodeString(this.KV)
	w.EncodeString(this.NS)
	w.EncodeString(this.DB)
	w.EncodeAny(this.Expr)
	w.EncodeAny(this.What)
	w.EncodeAny(this.Cond)
	w.EncodeAny(this.Group)
	w.EncodeAny(this.Order)
	w.EncodeAny(this.Limit)
	w.EncodeAny(this.Start)
	w.EncodeAny(this.Version)
	w.EncodeAny(this.Timeout)
	return
}

func (this *SelectStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeBool(&this.RW)
	r.DecodeString(&this.KV)
	r.DecodeString(&this.NS)
	r.DecodeString(&this.DB)
	r.DecodeAny(&this.Expr)
	r.DecodeAny(&this.What)
	r.DecodeAny(&this.Cond)
	r.DecodeAny(&this.Group)
	r.DecodeAny(&this.Order)
	r.DecodeAny(&this.Limit)
	r.DecodeAny(&this.Start)
	r.DecodeAny(&this.Version)
	r.DecodeAny(&this.Timeout)
	return
}

// --------------------------------------------------
// CreateStatement
// --------------------------------------------------

func init() {
	cork.Register(&CreateStatement{})
}

func (this *CreateStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *CreateStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *CreateStatement) ExtendCORK() byte {
	return 0x75
}

func (this *CreateStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.KV)
	w.EncodeString(this.NS)
	w.EncodeString(this.DB)
	w.EncodeAny(this.What)
	w.EncodeAny(this.Data)
	w.EncodeAny(this.Echo)
	w.EncodeAny(this.Timeout)
	return
}

func (this *CreateStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.KV)
	r.DecodeString(&this.NS)
	r.DecodeString(&this.DB)
	r.DecodeAny(&this.What)
	r.DecodeAny(&this.Data)
	r.DecodeAny(&this.Echo)
	r.DecodeAny(&this.Timeout)
	return
}

// --------------------------------------------------
// UpdateStatement
// --------------------------------------------------

func init() {
	cork.Register(&UpdateStatement{})
}

func (this *UpdateStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *UpdateStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *UpdateStatement) ExtendCORK() byte {
	return 0x76
}

func (this *UpdateStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.KV)
	w.EncodeString(this.NS)
	w.EncodeString(this.DB)
	w.EncodeAny(this.What)
	w.EncodeAny(this.Data)
	w.EncodeAny(this.Cond)
	w.EncodeAny(this.Echo)
	w.EncodeAny(this.Timeout)
	return
}

func (this *UpdateStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.KV)
	r.DecodeString(&this.NS)
	r.DecodeString(&this.DB)
	r.DecodeAny(&this.What)
	r.DecodeAny(&this.Data)
	r.DecodeAny(&this.Cond)
	r.DecodeAny(&this.Echo)
	r.DecodeAny(&this.Timeout)
	return
}

// --------------------------------------------------
// DeleteStatement
// --------------------------------------------------

func init() {
	cork.Register(&DeleteStatement{})
}

func (this *DeleteStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DeleteStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DeleteStatement) ExtendCORK() byte {
	return 0x77
}

func (this *DeleteStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.KV)
	w.EncodeString(this.NS)
	w.EncodeString(this.DB)
	w.EncodeBool(this.Hard)
	w.EncodeAny(this.What)
	w.EncodeAny(this.Cond)
	w.EncodeAny(this.Echo)
	w.EncodeAny(this.Timeout)
	return
}

func (this *DeleteStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.KV)
	r.DecodeString(&this.NS)
	r.DecodeString(&this.DB)
	r.DecodeBool(&this.Hard)
	r.DecodeAny(&this.What)
	r.DecodeAny(&this.Cond)
	r.DecodeAny(&this.Echo)
	r.DecodeAny(&this.Timeout)
	return
}

// --------------------------------------------------
// RelateStatement
// --------------------------------------------------

func init() {
	cork.Register(&RelateStatement{})
}

func (this *RelateStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RelateStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RelateStatement) ExtendCORK() byte {
	return 0x78
}

func (this *RelateStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.KV)
	w.EncodeString(this.NS)
	w.EncodeString(this.DB)
	w.EncodeAny(this.Type)
	w.EncodeAny(this.From)
	w.EncodeAny(this.With)
	w.EncodeAny(this.Data)
	w.EncodeBool(this.Uniq)
	w.EncodeAny(this.Echo)
	w.EncodeAny(this.Timeout)
	return
}

func (this *RelateStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.KV)
	r.DecodeString(&this.NS)
	r.DecodeString(&this.DB)
	r.DecodeAny(&this.Type)
	r.DecodeAny(&this.From)
	r.DecodeAny(&this.With)
	r.DecodeAny(&this.Data)
	r.DecodeBool(&this.Uniq)
	r.DecodeAny(&this.Echo)
	r.DecodeAny(&this.Timeout)
	return
}

// --------------------------------------------------
// InsertStatement
// --------------------------------------------------

func init() {
	cork.Register(&InsertStatement{})
}

func (this *InsertStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *InsertStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *InsertStatement) ExtendCORK() byte {
	return 0x79
}

func (this *InsertStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.KV)
	w.EncodeString(this.NS)
	w.EncodeString(this.DB)
	w.EncodeAny(this.Data)
	w.EncodeAny(this.Into)
	w.EncodeAny(this.Echo)
	w.EncodeAny(this.Timeout)
	return
}

func (this *InsertStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.KV)
	r.DecodeString(&this.NS)
	r.DecodeString(&this.DB)
	r.DecodeAny(&this.Data)
	r.DecodeAny(&this.Into)
	r.DecodeAny(&this.Echo)
	r.DecodeAny(&this.Timeout)
	return
}

// --------------------------------------------------
// UpsertStatement
// --------------------------------------------------

func init() {
	cork.Register(&UpsertStatement{})
}

func (this *UpsertStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *UpsertStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *UpsertStatement) ExtendCORK() byte {
	return 0x80
}

func (this *UpsertStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeString(this.KV)
	w.EncodeString(this.NS)
	w.EncodeString(this.DB)
	w.EncodeAny(this.Data)
	w.EncodeAny(this.Into)
	w.EncodeAny(this.Echo)
	w.EncodeAny(this.Timeout)
	return
}

func (this *UpsertStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeString(&this.KV)
	r.DecodeString(&this.NS)
	r.DecodeString(&this.DB)
	r.DecodeAny(&this.Data)
	r.DecodeAny(&this.Into)
	r.DecodeAny(&this.Echo)
	r.DecodeAny(&this.Timeout)
	return
}

// --------------------------------------------------
// DefineNamespaceStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineNamespaceStatement{})
}

func (this *DefineNamespaceStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineNamespaceStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineNamespaceStatement) ExtendCORK() byte {
	return 0x81
}

func (this *DefineNamespaceStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	return
}

func (this *DefineNamespaceStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	return
}

// --------------------------------------------------
// RemoveNamespaceStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveNamespaceStatement{})
}

func (this *RemoveNamespaceStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveNamespaceStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveNamespaceStatement) ExtendCORK() byte {
	return 0x82
}

func (this *RemoveNamespaceStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	return
}

func (this *RemoveNamespaceStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	return
}

// --------------------------------------------------
// DefineDatabaseStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineDatabaseStatement{})
}

func (this *DefineDatabaseStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineDatabaseStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineDatabaseStatement) ExtendCORK() byte {
	return 0x83
}

func (this *DefineDatabaseStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	return
}

func (this *DefineDatabaseStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	return
}

// --------------------------------------------------
// RemoveDatabaseStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveDatabaseStatement{})
}

func (this *RemoveDatabaseStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveDatabaseStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveDatabaseStatement) ExtendCORK() byte {
	return 0x84
}

func (this *RemoveDatabaseStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	return
}

func (this *RemoveDatabaseStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	return
}

// --------------------------------------------------
// DefineLoginStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineLoginStatement{})
}

func (this *DefineLoginStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineLoginStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineLoginStatement) ExtendCORK() byte {
	return 0x85
}

func (this *DefineLoginStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Kind)
	w.EncodeAny(this.User)
	w.EncodeBytes(this.Pass)
	w.EncodeBytes(this.Code)
	return
}

func (this *DefineLoginStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Kind)
	r.DecodeAny(&this.User)
	r.DecodeBytes(&this.Pass)
	r.DecodeBytes(&this.Code)
	return
}

// --------------------------------------------------
// RemoveLoginStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveLoginStatement{})
}

func (this *RemoveLoginStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveLoginStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveLoginStatement) ExtendCORK() byte {
	return 0x86
}

func (this *RemoveLoginStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Kind)
	w.EncodeAny(this.User)
	return
}

func (this *RemoveLoginStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Kind)
	r.DecodeAny(&this.User)
	return
}

// --------------------------------------------------
// DefineTokenStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineTokenStatement{})
}

func (this *DefineTokenStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineTokenStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineTokenStatement) ExtendCORK() byte {
	return 0x87
}

func (this *DefineTokenStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Kind)
	w.EncodeAny(this.Name)
	w.EncodeAny(this.Type)
	w.EncodeBytes(this.Code)
	return
}

func (this *DefineTokenStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Kind)
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.Type)
	r.DecodeBytes(&this.Code)
	return
}

// --------------------------------------------------
// RemoveTokenStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveTokenStatement{})
}

func (this *RemoveTokenStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveTokenStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveTokenStatement) ExtendCORK() byte {
	return 0x88
}

func (this *RemoveTokenStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Kind)
	w.EncodeAny(this.Name)
	return
}

func (this *RemoveTokenStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Kind)
	r.DecodeAny(&this.Name)
	return
}

// --------------------------------------------------
// DefineScopeStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineScopeStatement{})
}

func (this *DefineScopeStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineScopeStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineScopeStatement) ExtendCORK() byte {
	return 0x89
}

func (this *DefineScopeStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	w.EncodeAny(this.Time)
	w.EncodeBytes(this.Code)
	w.EncodeAny(this.Signup)
	w.EncodeAny(this.Signin)
	w.EncodeAny(this.Connect)
	w.EncodeAny(this.OnSignup)
	w.EncodeAny(this.OnSignin)
	return
}

func (this *DefineScopeStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.Time)
	r.DecodeBytes(&this.Code)
	r.DecodeAny(&this.Signup)
	r.DecodeAny(&this.Signin)
	r.DecodeAny(&this.Connect)
	r.DecodeAny(&this.OnSignup)
	r.DecodeAny(&this.OnSignin)
	return
}

// --------------------------------------------------
// RemoveScopeStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveScopeStatement{})
}

func (this *RemoveScopeStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveScopeStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveScopeStatement) ExtendCORK() byte {
	return 0x90
}

func (this *RemoveScopeStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	return
}

func (this *RemoveScopeStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	return
}

// --------------------------------------------------
// DefineTableStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineTableStatement{})
}

func (this *DefineTableStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineTableStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineTableStatement) ExtendCORK() byte {
	return 0x91
}

func (this *DefineTableStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	w.EncodeBool(this.Full)
	w.EncodeBool(this.Drop)
	w.EncodeBool(this.Lock)
	w.EncodeAny(this.Expr)
	w.EncodeAny(this.From)
	w.EncodeAny(this.Cond)
	w.EncodeAny(this.Group)
	w.EncodeAny(this.Perms)
	return
}

func (this *DefineTableStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	r.DecodeBool(&this.Full)
	r.DecodeBool(&this.Drop)
	r.DecodeBool(&this.Lock)
	r.DecodeAny(&this.Expr)
	r.DecodeAny(&this.From)
	r.DecodeAny(&this.Cond)
	r.DecodeAny(&this.Group)
	r.DecodeAny(&this.Perms)
	return
}

// --------------------------------------------------
// RemoveTableStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveTableStatement{})
}

func (this *RemoveTableStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveTableStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveTableStatement) ExtendCORK() byte {
	return 0x92
}

func (this *RemoveTableStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.What)
	return
}

func (this *RemoveTableStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.What)
	return
}

// --------------------------------------------------
// DefineEventStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineEventStatement{})
}

func (this *DefineEventStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineEventStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineEventStatement) ExtendCORK() byte {
	return 0x93
}

func (this *DefineEventStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	w.EncodeAny(this.What)
	w.EncodeAny(this.When)
	w.EncodeAny(this.Then)
	return
}

func (this *DefineEventStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.What)
	r.DecodeAny(&this.When)
	r.DecodeAny(&this.Then)
	return
}

// --------------------------------------------------
// RemoveEventStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveEventStatement{})
}

func (this *RemoveEventStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveEventStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveEventStatement) ExtendCORK() byte {
	return 0x94
}

func (this *RemoveEventStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	w.EncodeAny(this.What)
	return
}

func (this *RemoveEventStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.What)
	return
}

// --------------------------------------------------
// DefineFieldStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineFieldStatement{})
}

func (this *DefineFieldStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineFieldStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineFieldStatement) ExtendCORK() byte {
	return 0x95
}

func (this *DefineFieldStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	w.EncodeAny(this.What)
	w.EncodeString(this.Type)
	w.EncodeString(this.Kind)
	w.EncodeAny(this.Perms)
	w.EncodeAny(this.Value)
	w.EncodeAny(this.Assert)
	w.EncodeFloat64(this.Priority)
	return
}

func (this *DefineFieldStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.What)
	r.DecodeString(&this.Type)
	r.DecodeString(&this.Kind)
	r.DecodeAny(&this.Perms)
	r.DecodeAny(&this.Value)
	r.DecodeAny(&this.Assert)
	r.DecodeFloat64(&this.Priority)
	return
}

// --------------------------------------------------
// RemoveFieldStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveFieldStatement{})
}

func (this *RemoveFieldStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveFieldStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveFieldStatement) ExtendCORK() byte {
	return 0x96
}

func (this *RemoveFieldStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	w.EncodeAny(this.What)
	return
}

func (this *RemoveFieldStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.What)
	return
}

// --------------------------------------------------
// DefineIndexStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineIndexStatement{})
}

func (this *DefineIndexStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *DefineIndexStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *DefineIndexStatement) ExtendCORK() byte {
	return 0x97
}

func (this *DefineIndexStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	w.EncodeAny(this.What)
	w.EncodeAny(this.Cols)
	w.EncodeBool(this.Uniq)
	return
}

func (this *DefineIndexStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.What)
	r.DecodeAny(&this.Cols)
	r.DecodeBool(&this.Uniq)
	return
}

// --------------------------------------------------
// RemoveIndexStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveIndexStatement{})
}

func (this *RemoveIndexStatement) Decode(src []byte) {
	pack.Decode(src, this)
}

func (this *RemoveIndexStatement) Encode() (dst []byte) {
	return pack.Encode(this)
}

func (this *RemoveIndexStatement) ExtendCORK() byte {
	return 0x98
}

func (this *RemoveIndexStatement) MarshalCORK(w *cork.Writer) (err error) {
	w.EncodeAny(this.Name)
	w.EncodeAny(this.What)
	return
}

func (this *RemoveIndexStatement) UnmarshalCORK(r *cork.Reader) (err error) {
	r.DecodeAny(&this.Name)
	r.DecodeAny(&this.What)
	return
}
