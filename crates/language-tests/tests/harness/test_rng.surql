/**

[env]
imports = ["harness/rng.surql"]

[test]
wip = true
reason = "Random seeding is not deterministic across hosts."

[[test.results]]
value = "[]"

[[test.results]]
value = "0.9647921830037376f"

[[test.results]]
value = "0.06001842679688707f"

[[test.results]]
value = "0.32509319511882495f"

[[test.results]]
value = "0.8656932966769091f"

[[test.results]]
value = "0.09646457283452037f"

[[test.results]]
value = "[]"

[[test.results]]
value = "0.5068747645782423f"

[[test.results]]
value = "0.6997780499295914f"

[[test.results]]
value = "0.5803652132090065f"

[[test.results]]
value = "0.5133429374327534f"

[[test.results]]
value = "0.9215841659606667f"

[[test.results]]
value = "[]"

[[test.results]]
value = "0.7718618081562454f"

[[test.results]]
value = "0.49376563260739204f"

[[test.results]]
value = "0.2947812800848624f"

[[test.results]]
value = "0.9252045129469479f"

[[test.results]]
value = "0.44337102941062767f"
*/

fn::set_seed(1.0);

fn::rand();
fn::rand();
fn::rand();
fn::rand();
fn::rand();

fn::set_seed(0.1);

fn::rand();
fn::rand();
fn::rand();
fn::rand();
fn::rand();

fn::set_seed(0.9);

fn::rand();
fn::rand();
fn::rand();
fn::rand();
fn::rand();
