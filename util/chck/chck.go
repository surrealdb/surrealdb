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

package chck

func IsBase64(s string) bool {
	return rBase64.MatchString(s)
}

func IsDomain(s string) bool {
	return rDomain.MatchString(s)
}

func IsEmail(s string) bool {
	return rEmail.MatchString(s)
}

func IsLatitude(s string) bool {
	return rLatitude.MatchString(s)
}

func IsLongitude(s string) bool {
	return rLongitude.MatchString(s)
}

func IsPhone(s string) bool {
	return rPhone.MatchString(s)
}

func IsUUID(s string) bool {
	return rUUID.MatchString(s)
}
