/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An object nested forty-eight levels deep (each level an object holding an
array) is written and read back whole.
"""
issue = 6692

[[test.results]]
value = "[]"

[[test.results]]
value = "[40]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: ws_article:1, inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ age: 0, inner: [], name: 'first' }], level: 1, name: 'n1' }], level: 2, name: 'n2' }], level: 3, name: 'n3' }], level: 4, name: 'n4' }], level: 5, name: 'n5' }], level: 6, name: 'n6' }], level: 7, name: 'n7' }], level: 8, name: 'n8' }], level: 9, name: 'n9' }], level: 10, name: 'n10' }], level: 11, name: 'n11' }], level: 12, name: 'n12' }], level: 13, name: 'n13' }], level: 14, name: 'n14' }], level: 15, name: 'n15' }], level: 16, name: 'n16' }], level: 17, name: 'n17' }], level: 18, name: 'n18' }], level: 19, name: 'n19' }], level: 20, name: 'n20' }], level: 21, name: 'n21' }], level: 22, name: 'n22' }], level: 23, name: 'n23' }], level: 24, name: 'n24' }], level: 25, name: 'n25' }], level: 26, name: 'n26' }], level: 27, name: 'n27' }], level: 28, name: 'n28' }], level: 29, name: 'n29' }], level: 30, name: 'n30' }], level: 31, name: 'n31' }], level: 32, name: 'n32' }], level: 33, name: 'n33' }], level: 34, name: 'n34' }], level: 35, name: 'n35' }], level: 36, name: 'n36' }], level: 37, name: 'n37' }], level: 38, name: 'n38' }], level: 39, name: 'n39' }], level: 40, name: 'n40' }], level: 41, name: 'n41' }], level: 42, name: 'n42' }], level: 43, name: 'n43' }], level: 44, name: 'n44' }], level: 45, name: 'n45' }], level: 46, name: 'n46' }], level: 47, name: 'n47' }], level: 48, name: 'n48' }]"
*/
CREATE ws_article:1 CONTENT {name: 'n48', level: 48, inner: [{name: 'n47', level: 47, inner: [{name: 'n46', level: 46, inner: [{name: 'n45', level: 45, inner: [{name: 'n44', level: 44, inner: [{name: 'n43', level: 43, inner: [{name: 'n42', level: 42, inner: [{name: 'n41', level: 41, inner: [{name: 'n40', level: 40, inner: [{name: 'n39', level: 39, inner: [{name: 'n38', level: 38, inner: [{name: 'n37', level: 37, inner: [{name: 'n36', level: 36, inner: [{name: 'n35', level: 35, inner: [{name: 'n34', level: 34, inner: [{name: 'n33', level: 33, inner: [{name: 'n32', level: 32, inner: [{name: 'n31', level: 31, inner: [{name: 'n30', level: 30, inner: [{name: 'n29', level: 29, inner: [{name: 'n28', level: 28, inner: [{name: 'n27', level: 27, inner: [{name: 'n26', level: 26, inner: [{name: 'n25', level: 25, inner: [{name: 'n24', level: 24, inner: [{name: 'n23', level: 23, inner: [{name: 'n22', level: 22, inner: [{name: 'n21', level: 21, inner: [{name: 'n20', level: 20, inner: [{name: 'n19', level: 19, inner: [{name: 'n18', level: 18, inner: [{name: 'n17', level: 17, inner: [{name: 'n16', level: 16, inner: [{name: 'n15', level: 15, inner: [{name: 'n14', level: 14, inner: [{name: 'n13', level: 13, inner: [{name: 'n12', level: 12, inner: [{name: 'n11', level: 11, inner: [{name: 'n10', level: 10, inner: [{name: 'n9', level: 9, inner: [{name: 'n8', level: 8, inner: [{name: 'n7', level: 7, inner: [{name: 'n6', level: 6, inner: [{name: 'n5', level: 5, inner: [{name: 'n4', level: 4, inner: [{name: 'n3', level: 3, inner: [{name: 'n2', level: 2, inner: [{name: 'n1', level: 1, inner: [{name: 'first', age: 0, inner: []}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]}]} RETURN NONE;
SELECT VALUE inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].level FROM ws_article;
RETURN (SELECT * FROM ONLY ws_article:1) == (SELECT * FROM ONLY ws_article:1);
SELECT * FROM ws_article;
