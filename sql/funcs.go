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

var rolls = map[string]bool{

	"distinct": true,

	// Count implementation

	"count":     true,
	"count.if":  true,
	"count.not": true,

	// Math implementation

	"math.max":      true,
	"math.mean":     true,
	"math.min":      true,
	"math.stddev":   true,
	"math.sum":      true,
	"math.variance": true,
}

var aggrs = map[string]bool{

	"distinct": true,

	// Count implementation

	"count":     true,
	"count.if":  true,
	"count.not": true,

	// Math implementation

	"math.bottom":        true,
	"math.geometricmean": true,
	"math.harmonicmean":  true,
	"math.interquartile": true,
	"math.max":           true,
	"math.mean":          true,
	"math.median":        true,
	"math.midhinge":      true,
	"math.min":           true,
	"math.mode":          true,
	"math.nearestrank":   true,
	"math.percentile":    true,
	"math.sample":        true,
	"math.spread":        true,
	"math.stddev":        true,
	"math.sum":           true,
	"math.top":           true,
	"math.trimean":       true,
	"math.variance":      true,
}

var funcs = map[string]map[int]interface{}{

	"array":      {-1: nil},
	"batch":      {2: nil},
	"difference": {-1: nil},
	"distinct":   {1: nil},
	"either":     {-1: nil},
	"get":        {2: nil},
	"if":         {3: nil},
	"intersect":  {-1: nil},
	"model":      {2: nil, 3: nil, 4: nil},
	"regex":      {1: nil},
	"table":      {1: nil},
	"thing":      {2: nil},
	"union":      {-1: nil},

	// Count implementation
	"count":     {1: nil},
	"count.if":  {2: nil},
	"count.not": {2: nil},

	// Purge implementation
	"purge":     {1: nil},
	"purge.if":  {2: nil},
	"purge.not": {2: nil},

	// Geo implementation
	"geo.point":       {1: nil, 2: nil},
	"geo.circle":      {2: nil},
	"geo.polygon":     {-1: nil},
	"geo.distance":    {2: nil},
	"geo.inside":      {2: nil},
	"geo.intersects":  {2: nil},
	"geo.hash.decode": {1: nil},
	"geo.hash.encode": {2: nil},

	// Http implementation
	"http.head":         {1: nil, 2: nil},
	"http.get":          {1: nil, 2: nil},
	"http.put":          {1: nil, 2: nil, 3: nil},
	"http.post":         {1: nil, 2: nil, 3: nil},
	"http.patch":        {1: nil, 2: nil, 3: nil},
	"http.delete":       {1: nil, 2: nil},
	"http.async.head":   {1: nil, 2: nil},
	"http.async.get":    {1: nil, 2: nil},
	"http.async.put":    {1: nil, 2: nil, 3: nil},
	"http.async.post":   {1: nil, 2: nil, 3: nil},
	"http.async.patch":  {1: nil, 2: nil, 3: nil},
	"http.async.delete": {1: nil, 2: nil},

	// Math implementation
	"math.abs":           {1: nil},
	"math.bottom":        {2: nil},
	"math.ceil":          {1: nil},
	"math.correlation":   {2: nil},
	"math.covariance":    {2: nil},
	"math.fixed":         {2: nil},
	"math.floor":         {1: nil},
	"math.geometricmean": {1: nil},
	"math.harmonicmean":  {1: nil},
	"math.interquartile": {1: nil},
	"math.max":           {1: nil},
	"math.mean":          {1: nil},
	"math.median":        {1: nil},
	"math.midhinge":      {1: nil},
	"math.min":           {1: nil},
	"math.mode":          {1: nil},
	"math.nearestrank":   {2: nil},
	"math.percentile":    {2: nil},
	"math.round":         {1: nil},
	"math.sample":        {2: nil},
	"math.spread":        {1: nil},
	"math.sqrt":          {1: nil},
	"math.stddev":        {1: nil},
	"math.sum":           {1: nil},
	"math.top":           {2: nil},
	"math.trimean":       {1: nil},
	"math.variance":      {1: nil},

	// String implementation
	"string.concat":     {-1: nil},
	"string.contains":   {2: nil},
	"string.endsWith":   {2: nil},
	"string.format":     {-1: nil},
	"string.includes":   {2: nil},
	"string.join":       {-1: nil},
	"string.length":     {1: nil},
	"string.lowercase":  {1: nil},
	"string.repeat":     {2: nil},
	"string.replace":    {3: nil},
	"string.reverse":    {1: nil},
	"string.search":     {2: nil},
	"string.slice":      {3: nil},
	"string.slug":       {1: nil, 2: nil},
	"string.split":      {2: nil},
	"string.startsWith": {2: nil},
	"string.substr":     {3: nil},
	"string.trim":       {1: nil},
	"string.uppercase":  {1: nil},
	"string.words":      {1: nil},

	// Hash implementation
	"hash.md5":    {1: nil},
	"hash.sha1":   {1: nil},
	"hash.sha256": {1: nil},
	"hash.sha512": {1: nil},

	// Time implementation
	"time.now":   {0: nil},
	"time.add":   {2: nil},
	"time.age":   {2: nil},
	"time.floor": {2: nil},
	"time.round": {2: nil},
	"time.day":   {0: nil, 1: nil},
	"time.hour":  {0: nil, 1: nil},
	"time.mins":  {0: nil, 1: nil},
	"time.month": {0: nil, 1: nil},
	"time.nano":  {0: nil, 1: nil},
	"time.secs":  {0: nil, 1: nil},
	"time.unix":  {0: nil, 1: nil},
	"time.wday":  {0: nil, 1: nil},
	"time.week":  {0: nil, 1: nil},
	"time.yday":  {0: nil, 1: nil},
	"time.year":  {0: nil, 1: nil},

	// Url implementation
	"url.domain": {1: nil},
	"url.host":   {1: nil},
	"url.port":   {1: nil},
	"url.path":   {1: nil},

	// Email implementation
	"email.user":   {1: nil},
	"email.domain": {1: nil},
	"email.valid":  {1: nil},

	// Bcrypt implementation
	"bcrypt.compare":  {2: nil},
	"bcrypt.generate": {1: nil},

	// Scrypt implementation
	"scrypt.compare":  {2: nil},
	"scrypt.generate": {1: nil},

	// Check implementation
	"is.alpha":       {1: nil},
	"is.alphanum":    {1: nil},
	"is.ascii":       {1: nil},
	"is.domain":      {1: nil},
	"is.email":       {1: nil},
	"is.hexadecimal": {1: nil},
	"is.latitude":    {1: nil},
	"is.longitude":   {1: nil},
	"is.numeric":     {1: nil},
	"is.semver":      {1: nil},
	"is.uuid":        {1: nil},

	// Random implementation
	"rand":                    {0: nil},
	"guid":                    {0: nil},
	"uuid":                    {0: nil},
	"rand.bool":               {0: nil},
	"rand.guid":               {0: nil},
	"rand.uuid":               {0: nil},
	"rand.enum":               {-1: nil},
	"rand.time":               {0: nil, 2: nil},
	"rand.string":             {0: nil, 1: nil, 2: nil},
	"rand.integer":            {0: nil, 2: nil},
	"rand.decimal":            {0: nil, 2: nil},
	"rand.sentence":           {0: nil, 2: nil},
	"rand.paragraph":          {0: nil, 2: nil},
	"rand.person.email":       {0: nil},
	"rand.person.phone":       {0: nil},
	"rand.person.fullname":    {0: nil},
	"rand.person.firstname":   {0: nil},
	"rand.person.lastname":    {0: nil},
	"rand.person.username":    {0: nil},
	"rand.person.jobtitle":    {0: nil},
	"rand.company.name":       {0: nil},
	"rand.company.industry":   {0: nil},
	"rand.location.name":      {0: nil},
	"rand.location.address":   {0: nil},
	"rand.location.street":    {0: nil},
	"rand.location.city":      {0: nil},
	"rand.location.state":     {0: nil},
	"rand.location.county":    {0: nil},
	"rand.location.zipcode":   {0: nil},
	"rand.location.postcode":  {0: nil},
	"rand.location.country":   {0: nil},
	"rand.location.altitude":  {0: nil},
	"rand.location.latitude":  {0: nil},
	"rand.location.longitude": {0: nil},
}
