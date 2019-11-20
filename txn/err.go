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

package txn

import "errors"

var (
	ErrorNSNotFound = errors.New("The namespace does not exist")
	ErrorNTNotFound = errors.New("The namespace token does not exist")
	ErrorNUNotFound = errors.New("The namespace user does not exist")
	ErrorDBNotFound = errors.New("The database does not exist")
	ErrorDTNotFound = errors.New("The database token does not exist")
	ErrorDUNotFound = errors.New("The database user does not exist")
	ErrorSCNotFound = errors.New("The scope does not exist")
	ErrorSTNotFound = errors.New("The scope token does not exist")
	ErrorTBNotFound = errors.New("The table does not exist")
	ErrorEVNotFound = errors.New("The event does not exist")
	ErrorFDNotFound = errors.New("The field does not exist")
	ErrorIXNotFound = errors.New("The index does not exist")
	ErrorFTNotFound = errors.New("The table does not exist")
	ErrorLVNotFound = errors.New("The query does not exist")
)
