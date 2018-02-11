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

package mysql

const sqlClr = `
	DELETE FROM kv
	WHERE k=?
`

const sqlClrP = `
	DELETE FROM kv
	WHERE k LIKE CONCAT(?, '%')
	LIMIT ?
`

const sqlClrR = `
	DELETE FROM kv
	WHERE k>=? AND k<?
	LIMIT ?
`

const sqlGet = `
	SELECT * FROM kv
	WHERE t<=? AND k=?
	ORDER BY t DESC
	LIMIT 1
`

const sqlGetP = `
	SELECT q1.t, q1.k, v
	FROM kv q1
	JOIN (
		SELECT k, MAX(t) AS t
		FROM kv
		WHERE t<=? AND k LIKE CONCAT(?, '%')
		GROUP BY k
	) AS q2
	ON q1.t = q2.t AND q1.k = q2.k
	ORDER BY q1.k
	LIMIT ?
`

const sqlGetR = `
	SELECT q1.t, q1.k, v
	FROM kv q1
	JOIN (
		SELECT k, MAX(t) AS t
		FROM kv
		WHERE t<=? AND k>=? AND k<?
		GROUP BY k
	) AS q2
	ON q1.t = q2.t AND q1.k = q2.k
	ORDER BY q1.k
	LIMIT ?
`

const sqlDel = `
	DELETE FROM kv
	WHERE t<=? AND k=?
	ORDER BY t DESC
	LIMIT 1
`

const sqlDelC = `
	DELETE FROM kv
	WHERE t<=? AND k=? AND v=?
	ORDER BY t DESC
	LIMIT 1
`

const sqlDelP = `
	DELETE q1 FROM kv
	JOIN (
		SELECT k, MAX(t) AS t
		FROM kv
		WHERE t<=? AND k LIKE CONCAT(?, '%')
		GROUP BY k
	) AS q2
	ON q1.t = q2.t AND q1.k = q2.k
	ORDER BY q1.k
	LIMIT ?
`

const sqlDelR = `
	DELETE q1 FROM kv
	JOIN (
		SELECT k, MAX(t) AS t
		FROM kv
		WHERE t<=? AND k>=? AND k<?
		GROUP BY k
	) AS q2
	ON q1.t = q2.t AND q1.k = q2.k
	ORDER BY q1.k
	LIMIT ?
`

const sqlPut = `
	INSERT INTO kv
	(t, k, v)
	VALUES
	(?, ?, ?)
	ON DUPLICATE KEY UPDATE v=?
`

const sqlPutN = `
	INSERT INTO kv
	(t, k, v)
	VALUES
	(?, ?, ?)
`

const sqlPutC = `
	UPDATE kv
	SET v=?
	WHERE t<=? AND k=? AND v=?
	ORDER BY t DESC
	LIMIT 1
`
