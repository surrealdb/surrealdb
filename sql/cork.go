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
	"bytes"
	"fmt"

	"github.com/abcum/cork"
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
// ASC
// --------------------------------------------------

func init() {
	cork.Register(&Asc{})
}

func (this *Asc) ExtendCORK() byte {
	return 0x03
}

func (this *Asc) MarshalCORK() (dst []byte, err error) {
	return
}

func (this *Asc) UnmarshalCORK(src []byte) (err error) {
	return
}

func (this Asc) MarshalText() (data []byte, err error) {
	return []byte("~ASC~"), err
}

// --------------------------------------------------
// DESC
// --------------------------------------------------

func init() {
	cork.Register(&Desc{})
}

func (this *Desc) ExtendCORK() byte {
	return 0x04
}

func (this *Desc) MarshalCORK() (dst []byte, err error) {
	return
}

func (this *Desc) UnmarshalCORK(src []byte) (err error) {
	return
}

func (this Desc) MarshalText() (data []byte, err error) {
	return []byte("~DESC~"), err
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
	return []byte("~NULL~"), err
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

func (this *Field) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Expr)
	e.Encode(this.Alias)
	return b.Bytes(), nil
}

func (this *Field) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Expr)
	d.Decode(&this.Alias)
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

func (this *Group) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Expr)
	return b.Bytes(), nil
}

func (this *Group) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Expr)
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

func (this *Order) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Expr)
	e.Encode(this.Dir)
	return b.Bytes(), nil
}

func (this *Order) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Expr)
	d.Decode(&this.Dir)
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

func (this *SubExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Expr)
	return b.Bytes(), nil
}

func (this *SubExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Expr)
	return
}

// --------------------------------------------------
// FuncExpression
// --------------------------------------------------

func init() {
	cork.Register(&FuncExpression{})
}

func (this *FuncExpression) ExtendCORK() byte {
	return 0x22
}

func (this *FuncExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	e.Encode(this.Args)
	return b.Bytes(), nil
}

func (this *FuncExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	d.Decode(&this.Args)
	return
}

// --------------------------------------------------
// DataExpression
// --------------------------------------------------

func init() {
	cork.Register(&DataExpression{})
}

func (this *DataExpression) ExtendCORK() byte {
	return 0x23
}

func (this *DataExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.LHS)
	e.Encode(this.Op)
	e.Encode(this.RHS)
	return b.Bytes(), nil
}

func (this *DataExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.LHS)
	d.Decode(&this.Op)
	d.Decode(&this.RHS)
	return
}

// --------------------------------------------------
// BinaryExpression
// --------------------------------------------------

func init() {
	cork.Register(&BinaryExpression{})
}

func (this *BinaryExpression) ExtendCORK() byte {
	return 0x24
}

func (this *BinaryExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.LHS)
	e.Encode(this.Op)
	e.Encode(this.RHS)
	return b.Bytes(), nil
}

func (this *BinaryExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.LHS)
	d.Decode(&this.Op)
	d.Decode(&this.RHS)
	return
}

// --------------------------------------------------
// PathExpression
// --------------------------------------------------

func init() {
	cork.Register(&PathExpression{})
}

func (this *PathExpression) ExtendCORK() byte {
	return 0x25
}

func (this *PathExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Expr)
	return b.Bytes(), nil
}

func (this *PathExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Expr)
	return
}

// --------------------------------------------------
// PartExpression
// --------------------------------------------------

func init() {
	cork.Register(&PartExpression{})
}

func (this *PartExpression) ExtendCORK() byte {
	return 0x26
}

func (this *PartExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Part)
	return b.Bytes(), nil
}

func (this *PartExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Part)
	return
}

// --------------------------------------------------
// JoinExpression
// --------------------------------------------------

func init() {
	cork.Register(&JoinExpression{})
}

func (this *JoinExpression) ExtendCORK() byte {
	return 0x27
}

func (this *JoinExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Join)
	return b.Bytes(), nil
}

func (this *JoinExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Join)
	return
}

// --------------------------------------------------
// SubpExpression
// --------------------------------------------------

func init() {
	cork.Register(&SubpExpression{})
}

func (this *SubpExpression) ExtendCORK() byte {
	return 0x28
}

func (this *SubpExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.What)
	e.Encode(this.Name)
	e.Encode(this.Cond)
	return b.Bytes(), nil
}

func (this *SubpExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.What)
	d.Decode(&this.Name)
	d.Decode(&this.Cond)
	return
}

// --------------------------------------------------
// DiffExpression
// --------------------------------------------------

func init() {
	cork.Register(&DiffExpression{})
}

func (this *DiffExpression) ExtendCORK() byte {
	return 0x29
}

func (this *DiffExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.JSON)
	return b.Bytes(), nil
}

func (this *DiffExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.JSON)
	return
}

// --------------------------------------------------
// MergeExpression
// --------------------------------------------------

func init() {
	cork.Register(&MergeExpression{})
}

func (this *MergeExpression) ExtendCORK() byte {
	return 0x30
}

func (this *MergeExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.JSON)
	return b.Bytes(), nil
}

func (this *MergeExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.JSON)
	return
}

// --------------------------------------------------
// ContentExpression
// --------------------------------------------------

func init() {
	cork.Register(&ContentExpression{})
}

func (this *ContentExpression) ExtendCORK() byte {
	return 0x31
}

func (this *ContentExpression) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.JSON)
	return b.Bytes(), nil
}

func (this *ContentExpression) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.JSON)
	return
}

// ##################################################
// ##################################################
// ##################################################
// ##################################################
// ##################################################

// --------------------------------------------------
// Ident
// --------------------------------------------------

func init() {
	cork.Register(&Ident{})
}

func (this Ident) String() string {
	return this.ID
}

func (this *Ident) ExtendCORK() byte {
	return 0x51
}

func (this *Ident) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.ID)
	return b.Bytes(), nil
}

func (this *Ident) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.ID)
	return
}

func (this Ident) MarshalText() (data []byte, err error) {
	return []byte("ID:" + this.ID), err
}

// --------------------------------------------------
// Param
// --------------------------------------------------

func init() {
	cork.Register(&Param{})
}

func (this Param) String() string {
	return this.ID
}

func (this *Param) ExtendCORK() byte {
	return 0x52
}

func (this *Param) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.ID)
	return b.Bytes(), nil
}

func (this *Param) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.ID)
	return
}

func (this Param) MarshalText() (data []byte, err error) {
	return []byte("ID:" + this.ID), err
}

// --------------------------------------------------
// Table
// --------------------------------------------------

func init() {
	cork.Register(&Table{})
}

func (this Table) String() string {
	return this.TB
}

func (this *Table) ExtendCORK() byte {
	return 0x53
}

func (this *Table) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.TB)
	return b.Bytes(), nil
}

func (this *Table) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.TB)
	return
}

func (this Table) MarshalText() (data []byte, err error) {
	return []byte("TB:" + this.TB), err
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

func (this Thing) String() string {
	return fmt.Sprintf("@%s:%v", this.TB, this.ID)
}

func (this *Thing) ExtendCORK() byte {
	return 0x54
}

func (this *Thing) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.TB)
	e.Encode(this.ID)
	return b.Bytes(), nil
}

func (this *Thing) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.TB)
	d.Decode(&this.ID)
	return
}

func (this Thing) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

// ##################################################
// ##################################################
// ##################################################
// ##################################################
// ##################################################

// --------------------------------------------------
// LiveStatement
// --------------------------------------------------

func init() {
	cork.Register(&LiveStatement{})
}

func (this *LiveStatement) ExtendCORK() byte {
	return 0x71
}

func (this *LiveStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Expr)
	e.Encode(this.What)
	e.Encode(this.Cond)
	e.Encode(this.Echo)
	return b.Bytes(), nil
}

func (this *LiveStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Expr)
	d.Decode(&this.What)
	d.Decode(&this.Cond)
	d.Decode(&this.Echo)
	return
}

// --------------------------------------------------
// SelectStatement
// --------------------------------------------------

func init() {
	cork.Register(&SelectStatement{})
}

func (this *SelectStatement) ExtendCORK() byte {
	return 0x72
}

func (this *SelectStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Expr)
	e.Encode(this.What)
	e.Encode(this.Cond)
	e.Encode(this.Group)
	e.Encode(this.Order)
	e.Encode(this.Limit)
	e.Encode(this.Start)
	e.Encode(this.Version)
	e.Encode(this.Echo)
	return b.Bytes(), nil
}

func (this *SelectStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Expr)
	d.Decode(&this.What)
	d.Decode(&this.Cond)
	d.Decode(&this.Group)
	d.Decode(&this.Order)
	d.Decode(&this.Limit)
	d.Decode(&this.Start)
	d.Decode(&this.Version)
	d.Decode(&this.Echo)
	return
}

// --------------------------------------------------
// CreateStatement
// --------------------------------------------------

func init() {
	cork.Register(&CreateStatement{})
}

func (this *CreateStatement) ExtendCORK() byte {
	return 0x73
}

func (this *CreateStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.What)
	e.Encode(this.Data)
	e.Encode(this.Echo)
	return b.Bytes(), nil
}

func (this *CreateStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.What)
	d.Decode(&this.Data)
	d.Decode(&this.Echo)
	return
}

// --------------------------------------------------
// UpdateStatement
// --------------------------------------------------

func init() {
	cork.Register(&UpdateStatement{})
}

func (this *UpdateStatement) ExtendCORK() byte {
	return 0x74
}

func (this *UpdateStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Hard)
	e.Encode(this.What)
	e.Encode(this.Data)
	e.Encode(this.Cond)
	e.Encode(this.Echo)
	return b.Bytes(), nil
}

func (this *UpdateStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Hard)
	d.Decode(&this.What)
	d.Decode(&this.Data)
	d.Decode(&this.Cond)
	d.Decode(&this.Echo)
	return
}

// --------------------------------------------------
// DeleteStatement
// --------------------------------------------------

func init() {
	cork.Register(&DeleteStatement{})
}

func (this *DeleteStatement) ExtendCORK() byte {
	return 0x75
}

func (this *DeleteStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Hard)
	e.Encode(this.What)
	e.Encode(this.Cond)
	e.Encode(this.Echo)
	return b.Bytes(), nil
}

func (this *DeleteStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Hard)
	d.Decode(&this.What)
	d.Decode(&this.Cond)
	d.Decode(&this.Echo)
	return
}

// --------------------------------------------------
// RelateStatement
// --------------------------------------------------

func init() {
	cork.Register(&RelateStatement{})
}

func (this *RelateStatement) ExtendCORK() byte {
	return 0x76
}

func (this *RelateStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Type)
	e.Encode(this.From)
	e.Encode(this.With)
	e.Encode(this.Data)
	e.Encode(this.Uniq)
	e.Encode(this.Echo)
	return b.Bytes(), nil
}

func (this *RelateStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Type)
	d.Decode(&this.From)
	d.Decode(&this.With)
	d.Decode(&this.Data)
	d.Decode(&this.Uniq)
	d.Decode(&this.Echo)
	return
}

// --------------------------------------------------
// DefineNamespaceStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineNamespaceStatement{})
}

func (this *DefineNamespaceStatement) ExtendCORK() byte {
	return 0x77
}

func (this *DefineNamespaceStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	return b.Bytes(), nil
}

func (this *DefineNamespaceStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	return
}

// --------------------------------------------------
// RemoveNamespaceStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveNamespaceStatement{})
}

func (this *RemoveNamespaceStatement) ExtendCORK() byte {
	return 0x78
}

func (this *RemoveNamespaceStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	return b.Bytes(), nil
}

func (this *RemoveNamespaceStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	return
}

// --------------------------------------------------
// DefineDatabaseStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineDatabaseStatement{})
}

func (this *DefineDatabaseStatement) ExtendCORK() byte {
	return 0x79
}

func (this *DefineDatabaseStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	return b.Bytes(), nil
}

func (this *DefineDatabaseStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	return
}

// --------------------------------------------------
// RemoveDatabaseStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveDatabaseStatement{})
}

func (this *RemoveDatabaseStatement) ExtendCORK() byte {
	return 0x80
}

func (this *RemoveDatabaseStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	return b.Bytes(), nil
}

func (this *RemoveDatabaseStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	return
}

// --------------------------------------------------
// DefineLoginStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineLoginStatement{})
}

func (this *DefineLoginStatement) ExtendCORK() byte {
	return 0x81
}

func (this *DefineLoginStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Kind)
	e.Encode(this.User)
	e.Encode(this.Pass)
	return b.Bytes(), nil
}

func (this *DefineLoginStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Kind)
	d.Decode(&this.User)
	d.Decode(&this.Pass)
	return
}

// --------------------------------------------------
// RemoveLoginStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveLoginStatement{})
}

func (this *RemoveLoginStatement) ExtendCORK() byte {
	return 0x82
}

func (this *RemoveLoginStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Kind)
	e.Encode(this.User)
	return b.Bytes(), nil
}

func (this *RemoveLoginStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Kind)
	d.Decode(&this.User)
	return
}

// --------------------------------------------------
// DefineTokenStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineTokenStatement{})
}

func (this *DefineTokenStatement) ExtendCORK() byte {
	return 0x81
}

func (this *DefineTokenStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Kind)
	e.Encode(this.Name)
	e.Encode(this.Type)
	e.Encode(this.Text)
	return b.Bytes(), nil
}

func (this *DefineTokenStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Kind)
	d.Decode(&this.Name)
	d.Decode(&this.Type)
	d.Decode(&this.Text)
	return
}

// --------------------------------------------------
// RemoveTokenStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveTokenStatement{})
}

func (this *RemoveTokenStatement) ExtendCORK() byte {
	return 0x82
}

func (this *RemoveTokenStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Kind)
	e.Encode(this.Name)
	return b.Bytes(), nil
}

func (this *RemoveTokenStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Kind)
	d.Decode(&this.Name)
	return
}

// --------------------------------------------------
// DefineScopeStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineScopeStatement{})
}

func (this *DefineScopeStatement) ExtendCORK() byte {
	return 0x81
}

func (this *DefineScopeStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	e.Encode(this.Time)
	e.Encode(this.Signup)
	e.Encode(this.Signin)
	e.Encode(this.Policy)
	return b.Bytes(), nil
}

func (this *DefineScopeStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	d.Decode(&this.Time)
	d.Decode(&this.Signup)
	d.Decode(&this.Signin)
	d.Decode(&this.Policy)
	return
}

// --------------------------------------------------
// RemoveScopeStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveScopeStatement{})
}

func (this *RemoveScopeStatement) ExtendCORK() byte {
	return 0x82
}

func (this *RemoveScopeStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	return b.Bytes(), nil
}

func (this *RemoveScopeStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	return
}

// --------------------------------------------------
// DefineTableStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineTableStatement{})
}

func (this *DefineTableStatement) ExtendCORK() byte {
	return 0x83
}

func (this *DefineTableStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.What)
	e.Encode(this.Full)
	return b.Bytes(), nil
}

func (this *DefineTableStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.What)
	d.Decode(&this.Full)
	return
}

// --------------------------------------------------
// RemoveTableStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveTableStatement{})
}

func (this *RemoveTableStatement) ExtendCORK() byte {
	return 0x84
}

func (this *RemoveTableStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.What)
	return b.Bytes(), nil
}

func (this *RemoveTableStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.What)
	return
}

// --------------------------------------------------
// DefineRulesStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineRulesStatement{})
}

func (this *DefineRulesStatement) ExtendCORK() byte {
	return 0x85
}

func (this *DefineRulesStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.What)
	e.Encode(this.When)
	e.Encode(this.Rule)
	e.Encode(this.Cond)
	return b.Bytes(), nil
}

func (this *DefineRulesStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.What)
	d.Decode(&this.When)
	d.Decode(&this.Rule)
	d.Decode(&this.Cond)
	return
}

// --------------------------------------------------
// RemoveTableStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveRulesStatement{})
}

func (this *RemoveRulesStatement) ExtendCORK() byte {
	return 0x86
}

func (this *RemoveRulesStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.What)
	e.Encode(this.When)
	return b.Bytes(), nil
}

func (this *RemoveRulesStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.What)
	d.Decode(&this.When)
	return
}

// --------------------------------------------------
// DefineFieldStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineFieldStatement{})
}

func (this *DefineFieldStatement) ExtendCORK() byte {
	return 0x87
}

func (this *DefineFieldStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	e.Encode(this.What)
	e.Encode(this.Type)
	e.Encode(this.Enum)
	e.Encode(this.Code)
	e.Encode(this.Min)
	e.Encode(this.Max)
	e.Encode(this.Match)
	e.Encode(this.Default)
	e.Encode(this.Notnull)
	e.Encode(this.Readonly)
	e.Encode(this.Mandatory)
	e.Encode(this.Validate)
	return b.Bytes(), nil
}

func (this *DefineFieldStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	d.Decode(&this.What)
	d.Decode(&this.Type)
	d.Decode(&this.Enum)
	d.Decode(&this.Code)
	d.Decode(&this.Min)
	d.Decode(&this.Max)
	d.Decode(&this.Match)
	d.Decode(&this.Default)
	d.Decode(&this.Notnull)
	d.Decode(&this.Readonly)
	d.Decode(&this.Mandatory)
	d.Decode(&this.Validate)
	return
}

// --------------------------------------------------
// RemoveFieldStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveFieldStatement{})
}

func (this *RemoveFieldStatement) ExtendCORK() byte {
	return 0x88
}

func (this *RemoveFieldStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	e.Encode(this.What)
	return b.Bytes(), nil
}

func (this *RemoveFieldStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	d.Decode(&this.What)
	return
}

// --------------------------------------------------
// DefineIndexStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineIndexStatement{})
}

func (this *DefineIndexStatement) ExtendCORK() byte {
	return 0x89
}

func (this *DefineIndexStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	e.Encode(this.What)
	e.Encode(this.Cols)
	e.Encode(this.Uniq)
	return b.Bytes(), nil
}

func (this *DefineIndexStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	d.Decode(&this.What)
	d.Decode(&this.Cols)
	d.Decode(&this.Uniq)
	return
}

// --------------------------------------------------
// RemoveIndexStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveIndexStatement{})
}

func (this *RemoveIndexStatement) ExtendCORK() byte {
	return 0x90
}

func (this *RemoveIndexStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	e.Encode(this.What)
	return b.Bytes(), nil
}

func (this *RemoveIndexStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	d.Decode(&this.What)
	return
}

// --------------------------------------------------
// DefineViewStatement
// --------------------------------------------------

func init() {
	cork.Register(&DefineViewStatement{})
}

func (this *DefineViewStatement) ExtendCORK() byte {
	return 0x91
}

func (this *DefineViewStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	e.Encode(this.Expr)
	e.Encode(this.What)
	e.Encode(this.Cond)
	e.Encode(this.Group)
	return b.Bytes(), nil
}

func (this *DefineViewStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	d.Decode(&this.Expr)
	d.Decode(&this.What)
	d.Decode(&this.Cond)
	d.Decode(&this.Group)
	return
}

// --------------------------------------------------
// RemoveViewStatement
// --------------------------------------------------

func init() {
	cork.Register(&RemoveViewStatement{})
}

func (this *RemoveViewStatement) ExtendCORK() byte {
	return 0x92
}

func (this *RemoveViewStatement) MarshalCORK() (dst []byte, err error) {
	b := bytes.NewBuffer(dst)
	e := cork.NewEncoder(b)
	e.Encode(this.Name)
	return b.Bytes(), nil
}

func (this *RemoveViewStatement) UnmarshalCORK(src []byte) (err error) {
	b := bytes.NewBuffer(src)
	d := cork.NewDecoder(b)
	d.Decode(&this.Name)
	return
}
