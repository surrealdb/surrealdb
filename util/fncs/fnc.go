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

package fncs

import "context"

func Run(ctx context.Context, name string, args ...interface{}) (interface{}, error) {

	switch name {

	case "array":
		return array(ctx, args...)
	case "batch":
		return batch(ctx, args...)
	case "difference":
		return difference(ctx, args...)
	case "distinct":
		return distinct(ctx, args...)
	case "get":
		return get(ctx, args...)
	case "if":
		return ifel(ctx, args...)
	case "intersect":
		return intersect(ctx, args...)
	case "model":
		return model(ctx, args...)
	case "regex":
		return regex(ctx, args...)
	case "table":
		return table(ctx, args...)
	case "thing":
		return thing(ctx, args...)
	case "union":
		return union(ctx, args...)

	// Count implementation
	case "count":
		return count(ctx, args...)
	case "count.if":
		return countIf(ctx, args...)
	case "count.not":
		return countNot(ctx, args...)

	// Json implementation
	case "json.decode":
		return jsonDecode(ctx, args...)
	case "json.encode":
		return jsonEncode(ctx, args...)

	// Geo implementation
	case "geo.point":
		return geoPoint(ctx, args...)
	case "geo.circle":
		return geoCircle(ctx, args...)
	case "geo.polygon":
		return geoPolygon(ctx, args...)
	case "geo.distance":
		return geoDistance(ctx, args...)
	case "geo.inside":
		return geoInside(ctx, args...)
	case "geo.intersects":
		return geoIntersects(ctx, args...)
	case "geo.hash.decode":
		return geoHashDecode(ctx, args...)
	case "geo.hash.encode":
		return geoHashEncode(ctx, args...)

	// Http implementation
	case "http.head":
		return httpHead(ctx, args...)
	case "http.get":
		return httpGet(ctx, args...)
	case "http.put":
		return httpPut(ctx, args...)
	case "http.post":
		return httpPost(ctx, args...)
	case "http.patch":
		return httpPatch(ctx, args...)
	case "http.delete":
		return httpDelete(ctx, args...)
	case "http.async.head":
		return httpAsyncHead(ctx, args...)
	case "http.async.get":
		return httpAsyncGet(ctx, args...)
	case "http.async.put":
		return httpAsyncPut(ctx, args...)
	case "http.async.post":
		return httpAsyncPost(ctx, args...)
	case "http.async.patch":
		return httpAsyncPatch(ctx, args...)
	case "http.async.delete":
		return httpAsyncDelete(ctx, args...)

	// Math implementation
	case "math.abs", "abs":
		return mathAbs(ctx, args...)
	case "math.bottom", "bottom":
		return mathBottom(ctx, args...)
	case "math.ceil", "ceil":
		return mathCeil(ctx, args...)
	case "math.correlation", "correlation":
		return mathCorrelation(ctx, args...)
	case "math.covariance", "covariance":
		return mathCovariance(ctx, args...)
	case "math.floor", "floor":
		return mathFloor(ctx, args...)
	case "math.geometricmean", "geometricmean":
		return mathGeometricmean(ctx, args...)
	case "math.harmonicmean", "harmonicmean":
		return mathHarmonicmean(ctx, args...)
	case "math.interquartile", "interquartile":
		return mathInterquartile(ctx, args...)
	case "math.max", "max":
		return mathMax(ctx, args...)
	case "math.mean", "mean":
		return mathMean(ctx, args...)
	case "math.median", "median":
		return mathMedian(ctx, args...)
	case "math.midhinge", "midhinge":
		return mathMidhinge(ctx, args...)
	case "math.min", "min":
		return mathMin(ctx, args...)
	case "math.mode", "mode":
		return mathMode(ctx, args...)
	case "math.percentile", "percentile":
		return mathPercentile(ctx, args...)
	case "math.round", "round":
		return mathRound(ctx, args...)
	case "math.sample", "sample":
		return mathSample(ctx, args...)
	case "math.spread", "spread":
		return mathSpread(ctx, args...)
	case "math.stddev", "stddev":
		return mathStddev(ctx, args...)
	case "math.sum", "sum":
		return mathSum(ctx, args...)
	case "math.top", "top":
		return mathTop(ctx, args...)
	case "math.trimean", "trimean":
		return mathTrimean(ctx, args...)
	case "math.variance", "variance":
		return mathVariance(ctx, args...)

	// String implementation
	case "string.concat":
		return stringConcat(ctx, args...)
	case "string.contains":
		return stringContains(ctx, args...)
	case "string.endsWith":
		return stringEndsWith(ctx, args...)
	case "string.format":
		return stringFormat(ctx, args...)
	case "string.includes":
		return stringIncludes(ctx, args...)
	case "string.join":
		return stringJoin(ctx, args...)
	case "string.length":
		return stringLength(ctx, args...)
	case "string.levenshtein":
		return stringLevenshtein(ctx, args...)
	case "string.lowercase":
		return stringLowercase(ctx, args...)
	case "string.repeat":
		return stringRepeat(ctx, args...)
	case "string.replace":
		return stringReplace(ctx, args...)
	case "string.reverse":
		return stringReverse(ctx, args...)
	case "string.search":
		return stringSearch(ctx, args...)
	case "string.slice":
		return stringSlice(ctx, args...)
	case "string.split":
		return stringSplit(ctx, args...)
	case "string.startsWith":
		return stringStartsWith(ctx, args...)
	case "string.substr":
		return stringSubstr(ctx, args...)
	case "string.trim":
		return stringTrim(ctx, args...)
	case "string.uppercase":
		return stringUppercase(ctx, args...)
	case "string.words":
		return stringWords(ctx, args...)

	// Hash implementation
	case "hash.md5":
		return hashMd5(ctx, args...)
	case "hash.sha1":
		return hashSha1(ctx, args...)
	case "hash.sha256":
		return hashSha256(ctx, args...)
	case "hash.sha512":
		return hashSha512(ctx, args...)

	// Time implementation
	case "time.now":
		return timeNow(ctx, args...)
	case "time.add":
		return timeAdd(ctx, args...)
	case "time.age":
		return timeAge(ctx, args...)
	case "time.floor":
		return timeFloor(ctx, args...)
	case "time.round":
		return timeRound(ctx, args...)
	case "time.day":
		return timeDay(ctx, args...)
	case "time.hour":
		return timeHour(ctx, args...)
	case "time.mins":
		return timeMins(ctx, args...)
	case "time.month":
		return timeMonth(ctx, args...)
	case "time.nano":
		return timeNano(ctx, args...)
	case "time.secs":
		return timeSecs(ctx, args...)
	case "time.unix":
		return timeUnix(ctx, args...)
	case "time.year":
		return timeYear(ctx, args...)

	// Url implementation
	case "url.domain":
		return urlHost(ctx, args...)
	case "url.host":
		return urlHost(ctx, args...)
	case "url.port":
		return urlPort(ctx, args...)
	case "url.path":
		return urlPath(ctx, args...)

	// Email implementation
	case "email.user":
		return emailUser(ctx, args...)
	case "email.domain":
		return emailDomain(ctx, args...)
	case "email.valid":
		return emailValid(ctx, args...)

	// Bcrypt implementation
	case "bcrypt.compare":
		return bcryptCompare(ctx, args...)
	case "bcrypt.generate":
		return bcryptGenerate(ctx, args...)

	// Scrypt implementation
	case "scrypt.compare":
		return scryptCompare(ctx, args...)
	case "scrypt.generate":
		return scryptGenerate(ctx, args...)

	// Rand implementation
	case "rand":
		return rand(ctx, args...)
	case "guid":
		return randGuid(ctx, args...)
	case "uuid":
		return randUuid(ctx, args...)
	case "rand.bool":
		return randBool(ctx, args...)
	case "rand.guid":
		return randGuid(ctx, args...)
	case "rand.uuid":
		return randUuid(ctx, args...)
	case "rand.enum":
		return randEnum(ctx, args...)
	case "rand.time":
		return randTime(ctx, args...)
	case "rand.string":
		return randString(ctx, args...)
	case "rand.integer":
		return randInteger(ctx, args...)
	case "rand.decimal":
		return randDecimal(ctx, args...)
	case "rand.word":
		return randWord(ctx, args...)
	case "rand.sentence":
		return randSentence(ctx, args...)
	case "rand.paragraph":
		return randParagraph(ctx, args...)
	case "rand.person.email":
		return randPersonEmail(ctx, args...)
	case "rand.person.phone":
		return randPersonPhone(ctx, args...)
	case "rand.person.fullname":
		return randPersonFullname(ctx, args...)
	case "rand.person.firstname":
		return randPersonFirstname(ctx, args...)
	case "rand.person.lastname":
		return randPersonLastname(ctx, args...)
	case "rand.person.username":
		return randPersonUsername(ctx, args...)
	case "rand.person.jobtitle":
		return randPersonJobtitle(ctx, args...)
	case "rand.company.name":
		return randCompanyName(ctx, args...)
	case "rand.company.industry":
		return randCompanyIndustry(ctx, args...)
	case "rand.location.name":
		return randLocationName(ctx, args...)
	case "rand.location.address":
		return randLocationAddress(ctx, args...)
	case "rand.location.street":
		return randLocationStreet(ctx, args...)
	case "rand.location.city":
		return randLocationCity(ctx, args...)
	case "rand.location.state":
		return randLocationState(ctx, args...)
	case "rand.location.county":
		return randLocationCounty(ctx, args...)
	case "rand.location.zipcode":
		return randLocationZipcode(ctx, args...)
	case "rand.location.postcode":
		return randLocationPostcode(ctx, args...)
	case "rand.location.country":
		return randLocationCountry(ctx, args...)
	case "rand.location.altitude":
		return randLocationAltitude(ctx, args...)
	case "rand.location.latitude":
		return randLocationLatitude(ctx, args...)
	case "rand.location.longitude":
		return randLocationLongitude(ctx, args...)

	default:
		return nil, nil // Should never get here

	}

}
