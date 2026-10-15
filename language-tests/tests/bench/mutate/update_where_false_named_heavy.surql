/**
[bench]
reason = "UPDATE of 200 named records under `WHERE false`: each record runs the stages up to the `WHERE` and skips there, so the statement costs its compile and 200 record loads. The statement binds every stage and removes those after the `WHERE` when the table's definitions let it: the `light` arm stores no event, view or `REFERENCE` field, so its stages after the `WHERE` are removed before they are lowered. The `heavy` arm stores events and views and lowers every stage, so it is the control."
run = true
rebuild = true
rebuild-file-backend = true
datasets = { heavy = "bench/util/records-10000-heavy-schema.surql", light = "bench/util/records-10000-light-schema.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

UPDATE record:1, record:2, record:3, record:4, record:5, record:6, record:7, record:8, record:9, record:10, record:11, record:12, record:13, record:14, record:15, record:16, record:17, record:18, record:19, record:20, record:21, record:22, record:23, record:24, record:25, record:26, record:27, record:28, record:29, record:30, record:31, record:32, record:33, record:34, record:35, record:36, record:37, record:38, record:39, record:40, record:41, record:42, record:43, record:44, record:45, record:46, record:47, record:48, record:49, record:50, record:51, record:52, record:53, record:54, record:55, record:56, record:57, record:58, record:59, record:60, record:61, record:62, record:63, record:64, record:65, record:66, record:67, record:68, record:69, record:70, record:71, record:72, record:73, record:74, record:75, record:76, record:77, record:78, record:79, record:80, record:81, record:82, record:83, record:84, record:85, record:86, record:87, record:88, record:89, record:90, record:91, record:92, record:93, record:94, record:95, record:96, record:97, record:98, record:99, record:100, record:101, record:102, record:103, record:104, record:105, record:106, record:107, record:108, record:109, record:110, record:111, record:112, record:113, record:114, record:115, record:116, record:117, record:118, record:119, record:120, record:121, record:122, record:123, record:124, record:125, record:126, record:127, record:128, record:129, record:130, record:131, record:132, record:133, record:134, record:135, record:136, record:137, record:138, record:139, record:140, record:141, record:142, record:143, record:144, record:145, record:146, record:147, record:148, record:149, record:150, record:151, record:152, record:153, record:154, record:155, record:156, record:157, record:158, record:159, record:160, record:161, record:162, record:163, record:164, record:165, record:166, record:167, record:168, record:169, record:170, record:171, record:172, record:173, record:174, record:175, record:176, record:177, record:178, record:179, record:180, record:181, record:182, record:183, record:184, record:185, record:186, record:187, record:188, record:189, record:190, record:191, record:192, record:193, record:194, record:195, record:196, record:197, record:198, record:199, record:200 SET active = true WHERE false;
