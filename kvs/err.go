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

package kvs

import "fmt"

// DSError is an error which occurs when there is a
// problem with writing keys/values to the database.
type DSError struct {
	Err error
}

// Error returns the string representation of the error.
func (e *DSError) Error() string {
	return fmt.Sprintf("There was a problem connecting to the datastore: %s", e.Err.Error())
}

// DBError is an error which occurs when there is a
// problem with writing keys/values to the database.
type DBError struct {
	Err error
}

// Error returns the string representation of the error.
func (e *DBError) Error() string {
	return fmt.Sprintf("There was a problem writing to the database: %s", e.Err.Error())
}

// TXError is an error which occurs when there is a
// problem with a writable transaction.
type TXError struct {
	Err error
}

// Error returns the string representation of the error.
func (e *TXError) Error() string {
	return fmt.Sprintf("There was a problem with the transaction: %s", e.Err.Error())
}

// CKError is an error which occurs when there is a
// problem with encrypting/decrypting values.
type CKError struct {
	Err error
}

// Error returns the string representation of the error.
func (e *CKError) Error() string {
	return fmt.Sprintf("This cipherkey used is not valid: %s", e.Err.Error())
}

// KVError is an error which occurs when there is a
// problem with a conditional put or delete.
type KVError struct {
	Err error
	Key []byte
	Val []byte
	Exp []byte
}

// Error returns the string representation of the error.
func (e *KVError) Error() string {
	return fmt.Sprintf("Database record already exists.")
}
