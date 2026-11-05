/**
[env]
imports = ["bench/util/embeddings-data-768d-1000.surql"]

[bench]
reason = "Paths into `$this` and `$self` over a table scan: each of the 1000 rows carries a 768-float `vec`, and the projection reads eight of its elements, each through its own path rooted at the row. `$this` and `$self` read the row in place, so each path's cost must track what it steps into rather than a copy of the whole row per read."
run = true
rebuild = false
warmup = "1s"
sample-size = 20
measurement-time = "10s"

[test]
run = false
*/

SELECT VALUE [
	$this.vec[0], $this.vec[1], $this.vec[2], $this.vec[3],
	$self.vec[4], $self.vec[5], $self.vec[6], $self.vec[7],
] FROM embeddings;
