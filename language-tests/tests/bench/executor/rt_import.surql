/**
[test]

[[test.results]]
value = "'OK'"

*/
{

	DEFINE TABLE material schemafull PERMISSIONS FULL;
	DEFINE FIELD albedo ON material type array<number,3>;
	DEFINE FIELD fuzz ON material TYPE number;
	DEFINE FIELD emissive ON material TYPE bool;

	DEFINE TABLE spheres schemafull;
	DEFINE FIELD position ON spheres type array<number,3>;
	DEFINE FIELD radius ON spheres TYPE number;
	DEFINE FIELD material ON spheres TYPE record<material>;

	DEFINE TABLE triangles schemafull;
	DEFINE FIELD vertices ON triangles TYPE array<array<number,3>,3>;
	DEFINE FIELD material ON triangles TYPE record<material>;
	DEFINE FIELD dirs ON triangles TYPE array DEFAULT [];
	DEFINE FIELD normal ON triangles TYPE array DEFAULT [];

	// for some reason this doesn't work.
	DEFINE EVENT set_fields ON triangles WHEN $before.vertices != $after.vertices THEN {
		let $dirs = [
			vector::subtract($after.vertices[1], $after.vertices[0]),
			vector::subtract($after.vertices[2], $after.vertices[0]),
		];

		let $normal = vector::normalize(vector::cross($dirs[0],$dirs[1]));

		UPDATE triangles SET dirs=$dirs,normal=$normal WHERE id = $after.id;
	};

	DEFINE TABLE pixels schemafull;
	DEFINE FIELD color ON pixels type array<number,3>;
	DEFINE FIELD position ON pixels type array<number,2>;
	DEFINE FIELD samples ON pixels type number;
	DEFINE FIELD image ON pixels TYPE record<image>;

	DEFINE TABLE image schemafull;
	DEFINE FIELD width ON image TYPE number DEFAULT 64;
	DEFINE FIELD height ON image TYPE number DEFAULT 64;

	DEFINE EVENT resize ON image WHEN $before.width != $after.width OR $before.height != $after.height THEN {
		DELETE pixels WHERE image = $before.id;

		let $x_coords = array::range(0,$after.width);
		let $y_coords = array::range(0,$after.height);

		FOR $x IN $x_coords {
			FOR $y IN $y_coords {
				let $position = [$x,$y];
				CREATE pixels CONTENT {
					id: [$before.id,$x,$y],
					image: $after.id,
					position: $position,
					color: [255,0,0],
					samples: 0,
				};
			};
		};
	};

	DEFINE FUNCTION fn::trace($image: record<image>){
		let $aspectRatio = $image.width / $image.height;
		let $focalLength = 1.0;
		let $samples = 2;
		let $depth = 4;

		let $cameraPos = [0.0,0.0,0.5];
		let $viewportHeight = 2.0;
		let $viewportWidth = $viewportHeight * $aspectRatio;

		let $pixelDelta = vector::divide([$viewportWidth,-$viewportHeight],[$image.width,$image.height]);

		let $pixelSpaceDeltaHalf = vector::divide([$pixelDelta[0],$pixelDelta[1],0],[2.0,2.0,1.0]);

		let $viewportUpperLeft = vector::subtract(vector::subtract($cameraPos,[0,0,$focalLength]),[$viewportWidth / 2.0, -$viewportHeight / 2.0,0]);

		let $pixel0Loc = vector::add($viewportUpperLeft,$pixelSpaceDeltaHalf);

		UPDATE pixels SET color = [0,0,0];

		//let $samples = array::range(0,1);
		FOR $pix in (select * from pixels) {
			let $delta = vector::multiply($pixelDelta,$pix.position);
			let $pixelCenter = vector::add($pixel0Loc,[$delta[0],$delta[1],0.0]);

			UPDATE $pix.id SET color = [0,0,0];

			for $_ in array::range(0,$samples){
				let $jitter = vector::multiply($pixelDelta,[rand::float(-0.5,0.5),rand::float(-0.5,0.5)]);
				let $rayDir = vector::subtract(vector::add($pixelCenter,[$jitter[0],$jitter[1],0.0]),$cameraPos);

				UPDATE $pix.id SET color = vector::add(color,vector::multiply(fn::rayColor($cameraPos,$rayDir,$depth),[255,255,255]));
			};

		};

		UPDATE pixels SET color = vector::divide(color, [$samples,$samples,$samples]);
		RETURN null;
	};

	DEFINE FUNCTION fn::rayColor($origin: any, $direction: any, $depth: any){

		let $hitTestSphere = (SELECT fn::hitTestSphere(position,radius,$origin,$direction) as result, material from spheres ORDER BY result ASC)[0];
		let $hitTestTriangle = (SELECT fn::hitTestTriangle(vertices,$origin,$direction) as result, material from triangles ORDER BY result ASC)[0];

		let $hitTest = (if ($hitTestSphere.result[0] || MATH::INF) < ($hitTestTriangle.result[0] || MATH::INF) {
			$hitTestSphere
				} else {
			$hitTestTriangle
		});

		if $hitTest && $hitTest.result[0] < MATH::INF {
			let $mat = $hitTest.material;

			if $depth == 0 || $mat.emissive {
				return $mat.albedo;
			};

			let $res = $hitTest.result;
			let $normal = $res[2];
			let $hitPos = $res[1];
			let $dot2 = vector::dot($direction,$normal) * 2;
			let $reflect = vector::subtract($direction,vector::multiply($normal,[$dot2,$dot2,$dot2]));

			let $phi = rand::float() * MATH::PI * 2;
			let $thetaCos = rand::float() * 2 - 1;
			let $theta = MATH::acos($thetaCos);
			let $fuzzFac = $mat.fuzz;
			let $thetaSin = MATH::sin($theta);
			let $fuzz = [
				$thetaSin * MATH::cos($phi) * $fuzzFac,
				$thetaSin * MATH::sin($phi) * $fuzzFac,
				$thetaCos * $fuzzFac,
			];

			let $reflected = vector::add(vector::normalize($reflect),$fuzz);

			let $newColor = fn::rayColor($hitPos,$reflected,$depth - 1);

			return vector::multiply($mat.albedo,$newColor);
		};

		let $norm = vector::normalize($direction);
		let $lerpFactor = 0.5 * ($norm[1] + 1.0);
		let $neglerpFactor = 1.0 - $lerpFactor;
		return vector::add([$neglerpFactor,$neglerpFactor,$neglerpFactor],vector::multiply([0.5,0.7,1.0],[$lerpFactor,$lerpFactor,$lerpFactor]))
	};

	DEFINE FUNCTION fn::hitTestTriangle($vertices: array, $origin: array, $direction: array){
		let $e1 = vector::subtract($vertices[1], $vertices[0]);
		let $e2 = vector::subtract($vertices[2], $vertices[0]);

		let $rayCrossE2 = vector::cross($direction,$e2);
		let $det = vector::dot($e1,$rayCrossE2);

		if(math::abs($det) < 0.00001){
			return [MATH::INF]
		};

		let $invDet = 1.0 / $det;
		let $s = vector::subtract($origin,$vertices[0]);
		let $u = $invDet * vector::dot($s,$rayCrossE2);

		if ($u < 0 || $u > 1){
			return [MATH::INF]
		};

		let $sCrossE1 = vector::cross($s,$e1);
		let $v = $invDet * vector::dot($direction,$sCrossE1);
		if ($v < 0 || $u + $v > 1){
			return [MATH::INF]
		};

		let $t = $invDet * vector::dot($e2,$sCrossE1);
		if ($t < 0.0001){
			return [MATH::INF]
		};

		let $hitPos = vector::add($origin,vector::multiply($direction,[$t,$t,$t]));

		let $normal = vector::normalize(vector::cross($e1,$e2));

		let $normal = (IF vector::dot($direction,$normal) > 0 {
			vector::multiply($normal,[-1,-1,-1])
		}else{
			$normal
		});
		RETURN [$t, $hitPos, $normal]
	};

	DEFINE FUNCTION fn::hitTestSphere($position: array,$radius: number,$origin: array, $direction: array){
		let $oc = vector::subtract($origin,$position);
		let $a = vector::dot($direction, $direction);
		let $b = vector::dot($oc,$direction);
		let $c = vector::dot($oc,$oc) - ($radius * $radius);
		let $discr = ($b*$b) - ($a*$c);

		IF $discr < 0 {
			RETURN [MATH::INF];
		};

		let $sqrtd = math::sqrt($discr);
		let $t = (-$b - $sqrtd) / $a;
		if ($t <= 0.0001){
			let $t = (-$b + $sqrtd) / $a;

			if ($t <= 0.0001){
				return [MATH::INF];
			};

			let $hitPos = vector::add($origin,vector::multiply($direction,[$t,$t,$t]));
			let $normal = vector::divide(vector::subtract($hitPos,$position),[$radius,$radius,$radius]);
			let $normal = (IF vector::dot($direction,$normal) > 0 {
				vector::multiply($normal,[-1,-1,-1])
			}else{
				$normal
			});

			RETURN [$t, $hitPos, $normal]
		};

		let $hitPos = vector::add($origin,vector::multiply($direction,[$t,$t,$t]));
		let $normal = vector::divide(vector::subtract($hitPos,$position),[$radius,$radius,$radius]);
		let $normal = (IF vector::dot($direction,$normal) > 0 {
			vector::multiply($normal,[-1,-1,-1])
		}else{
			$normal
		});

		RETURN [$t, $hitPos, $normal]
	};

	INSERT [ { albedo: [1, 0, 0], emissive: false, fuzz: 0, id: material:4jwx0w4rmrcplixd31od }, { albedo: [0.7372549019607844f, 0, 0.8666666666666667f], emissive: true, fuzz: 0, id: material:aar75tybb81obxg9ntjc }, { albedo: [0.8f, 1, 0.8f], emissive: false, fuzz: 0, id: material:b6v4k184f7j0irx50bem } ];

	INSERT [
		{ id: spheres:9bzzvlmq1lw6h3rl0yei, material: material:4jwx0w4rmrcplixd31od, position: [-1, 0, -1], radius: 0.5f },
		{ id: spheres:a5id20337vuebjkb4cxg, material: material:b6v4k184f7j0irx50bem, position: [6, 0, -11], radius: 10 },
		{ id: spheres:ky3300quykhnoq52cins, material: material:b6v4k184f7j0irx50bem, position: [-1, 1.3f, -1], radius: 0.5f }
	];

	INSERT [ { dirs: [[-0.416665f, -0.281444f, 0f], [-0.463005f, -0.25585899999999995f, 0f]], id: triangles:0m0wp8keew4ry5qs4wpt, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.13884502083333317f, 0.15353791666666666f, -0.8f], [-0.27781997916666684f, -0.12790608333333336f, -0.8f], [-0.32415997916666683f, -0.10232108333333328f, -0.8f]] }, { dirs: [[0.324086f, -0.12786200000000003f, 0f], [0.277845f, -0.15345599999999998f, 0f]], id: triangles:0r81rns2kdjxv50g84l9, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.000020020833333189714f, 0.3325119166666667f, -0.8f], [0.3241060208333332f, 0.20464991666666665f, -0.8f], [0.2778650208333332f, 0.1790559166666667f, -0.8f]] }, { dirs: [[0.04643900000000001f, 0.025695000000000023f, 0f], [0.00009899999999996023f, -0.46042600000000006f, 0f]], id: triangles:1yfa2ksnjxwcwgkcbfyi, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.37034702083333315f, 0.2302259166666667f, -0.8f], [0.41678602083333316f, 0.2559209166666667f, -0.8f], [0.3704460208333331f, -0.23020008333333336f, -0.8f]] }, { dirs: [[-0.27786700000000003f, 0.204635f, 0f], [-0.000030000000000085514f, 0.05118200000000006f, 0f]], id: triangles:265xrao5cm9fyzl2uk8g, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.00004702083333318896f, -0.38376208333333334f, -0.8f], [-0.27781997916666684f, -0.17912708333333333f, -0.8f], [0.000017020833333103447f, -0.3325800833333333f, -0.8f]] }, { dirs: [[0.416763f, -0.17897199999999996f, 0f], [0.370324f, -0.204667f, 0f]], id: triangles:2tjgg52k4tlmwk24oldx, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.00002302083333316496f, 0.4348929166666667f, -0.8f], [0.41678602083333316f, 0.2559209166666667f, -0.8f], [0.37034702083333315f, 0.2302259166666667f, -0.8f]] }, { dirs: [[0.04633999999999999f, -0.486022f, 0f], [0f, -0.5116179999999999f, 0f]], id: triangles:3wdavtqadehe754gkk2k, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.41674097916666686f, 0.25582191666666665f, -0.8f], [-0.37040097916666687f, -0.23020008333333336f, -0.8f], [-0.41674097916666686f, -0.2557960833333333f, -0.8f]] }, { dirs: [[0.04633999999999999f, -0.02558500000000008f, 0f], [0.04633999999999999f, -0.07680600000000004f, 0f]], id: triangles:4bk9nrvarsbx5i3mlpjh, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.32415997916666683f, -0.10232108333333328f, -0.8f], [-0.27781997916666684f, -0.12790608333333336f, -0.8f], [-0.27781997916666684f, -0.17912708333333333f, -0.8f]] }, { dirs: [[0f, -0.051389999999999936f, 0f], [-0.23150199999999999f, -0.12817800000000001f, 0f]], id: triangles:4d3wkc2wwkk0tlrdw02z, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[0.23152502083333315f, -0.10212208333333334f, -0.8f], [0.23152502083333315f, -0.15351208333333327f, -0.8f], [0.00002302083333316496f, -0.23030008333333335f, -0.8f]] }, { dirs: [[0f, 0.051192000000000015f, 0f], [0.416763f, -0.17897199999999996f, 0f]], id: triangles:50pjqotnztoq91bc15ho, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.00002302083333316496f, 0.4348929166666667f, -0.8f], [0.00002302083333316496f, 0.4860849166666667f, -0.8f], [0.41678602083333316f, 0.2559209166666667f, -0.8f]] }, { dirs: [[0.04624099999999998f, -0.07670900000000003f, 0f], [0f, -0.05121300000000001f, 0f]], id: triangles:5u1r3omlebc8bymx9s8k, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.2778650208333332f, 0.1790559166666667f, -0.8f], [0.3241060208333332f, 0.10234691666666668f, -0.8f], [0.2778650208333332f, 0.1278429166666667f, -0.8f]] }, { dirs: [[0.416763f, 0.17907099999999998f, 0f], [0f, -0.051192000000000015f, 0f]], id: triangles:6yu1fii47sl1rktux6zn, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.00002302083333316496f, -0.4348670833333333f, -0.8f], [0.41678602083333316f, -0.2557960833333333f, -0.8f], [0.00002302083333316496f, -0.4860590833333333f, -0.8f]] }, { dirs: [[0.416764f, 0.17907100000000004f, 0f], [0.04633999999999999f, -0.025595999999999952f, 0f]], id: triangles:73c0fhljck5sqobyqeyj, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.41674097916666686f, 0.25582191666666665f, -0.8f], [0.00002302083333316496f, 0.4348929166666667f, -0.8f], [-0.37040097916666687f, 0.2302259166666667f, -0.8f]] }, { dirs: [[0.04633999999999999f, -0.17917100000000002f, 0f], [0f, -0.25585899999999995f, 0f]], id: triangles:8nbl517wdiduqwrqgpaq, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.32415997916666683f, 0.20472991666666668f, -0.8f], [-0.27781997916666684f, 0.025558916666666653f, -0.8f], [-0.32415997916666683f, -0.05112908333333327f, -0.8f]] }, { dirs: [[0.09257900000000002f, -0.000022999999999995246f, 0f], [0.231501f, -0.07681100000000007f, 0f]], id: triangles:9zjvpae19w8i5ual6v8n, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.23147797916666685f, -0.15348908333333328f, -0.8f], [-0.13889897916666682f, -0.15351208333333327f, -0.8f], [0.00002302083333316496f, -0.23030008333333335f, -0.8f]] }, { dirs: [[-0.04624099999999998f, -0.07678699999999994f, 0f], [-0.37042400000000003f, -0.20466699999999993f, 0f]], id: triangles:ax6cc1zhz4t4vgqfw0y6, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.3241060208333332f, 0.05115491666666666f, -0.8f], [0.2778650208333332f, -0.025632083333333278f, -0.8f], [-0.046317979166666856f, -0.15351208333333327f, -0.8f]] }, { dirs: [[0.04634000000000005f, 0.4861210000000001f, 0f], [0.04634000000000005f, -0.025595999999999952f, 0f]], id: triangles:bbizi7dllrdwp1e6rjgy, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.3704460208333331f, -0.23020008333333336f, -0.8f], [0.41678602083333316f, 0.2559209166666667f, -0.8f], [0.41678602083333316f, -0.2557960833333333f, -0.8f]] }, { dirs: [[0.04633999999999994f, -0.025595999999999952f, 0f], [-0.277843f, -0.153476f, 0f]], id: triangles:bscs47yqyx6jj8zn1cws, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[0.00002302083333316496f, 0.17903491666666665f, -0.8f], [0.0463630208333331f, 0.1534389166666667f, -0.8f], [-0.27781997916666684f, 0.025558916666666653f, -0.8f]] }, { dirs: [[-0.000001999999999946489f, -0.051188999999999985f, 0f], [-0.277842f, -0.20461399999999996f, 0f]], id: triangles:cojq3kgz4pcfh0lga1z1, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.000022020833333136203f, 0.38370091666666667f, -0.8f], [0.000020020833333189714f, 0.3325119166666667f, -0.8f], [-0.27781997916666684f, 0.1790869166666667f, -0.8f]] }, { dirs: [[0.04624099999999998f, 0.02559399999999995f, 0f], [0.04624099999999998f, -0.07670900000000003f, 0f]], id: triangles:d5zjfdv7xum7jyr8okud, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.2778650208333332f, 0.1790559166666667f, -0.8f], [0.3241060208333332f, 0.20464991666666665f, -0.8f], [0.3241060208333332f, 0.10234691666666668f, -0.8f]] }, { dirs: [[0.04633999999999999f, -0.02564299999999997f, 0f], [0.04633999999999999f, -0.17917100000000002f, 0f]], id: triangles:elh5h7znbk5zhpoqpmz4, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.32415997916666683f, 0.20472991666666668f, -0.8f], [-0.27781997916666684f, 0.1790869166666667f, -0.8f], [-0.27781997916666684f, 0.025558916666666653f, -0.8f]] }, { dirs: [[0.04624099999999998f, -0.025638999999999967f, 0f], [-0.2778480000000001f, -0.15351499999999996f, 0f]], id: triangles:fo6kpq1257wiug9cqlga, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[0.2778650208333332f, -0.17906508333333332f, -0.8f], [0.3241060208333332f, -0.2047040833333333f, -0.8f], [0.000017020833333103447f, -0.3325800833333333f, -0.8f]] }, { dirs: [[0.32418300000000005f, 0.12788f, 0f], [0.04634100000000002f, -0.025596000000000063f, 0f]], id: triangles:fxr9tsle46m26o2in0m2, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.046317979166666856f, -0.15351208333333327f, -0.8f], [0.2778650208333332f, -0.025632083333333278f, -0.8f], [0.00002302083333316496f, -0.17910808333333333f, -0.8f]] }, { dirs: [[0.04624099999999998f, 0.07678699999999994f, 0f], [0f, -0.15343300000000004f, 0f]], id: triangles:gfs15j5lkbe2v3954os1, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.2778650208333332f, -0.025632083333333278f, -0.8f], [0.3241060208333332f, 0.05115491666666666f, -0.8f], [0.2778650208333332f, -0.17906508333333332f, -0.8f]] }, { dirs: [[0.000001999999999946489f, 0.051188999999999985f, 0f], [0.324086f, -0.12786200000000003f, 0f]], id: triangles:gtaoxr3vbttrxh621gfl, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.000020020833333189714f, 0.3325119166666667f, -0.8f], [0.000022020833333136203f, 0.38370091666666667f, -0.8f], [0.3241060208333332f, 0.20464991666666665f, -0.8f]] }, { dirs: [[0.23150199999999999f, 0.07678800000000008f, 0f], [0.000005999999999950489f, -0.05108800000000002f, 0f]], id: triangles:he3sbfgmjnt6rvpeiidx, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.00002302083333316496f, -0.23030008333333335f, -0.8f], [0.23152502083333315f, -0.15351208333333327f, -0.8f], [0.000029020833333115448f, -0.28138808333333337f, -0.8f]] }, { dirs: [[0.231501f, -0.07681100000000007f, 0f], [0.23150699999999996f, -0.1278990000000001f, 0f]], id: triangles:i5w073ybm0trihhiddkb, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.23147797916666685f, -0.15348908333333328f, -0.8f], [0.00002302083333316496f, -0.23030008333333335f, -0.8f], [0.000029020833333115448f, -0.28138808333333337f, -0.8f]] }, { dirs: [[0.04633999999999999f, -0.07680600000000004f, 0f], [0f, -0.10235900000000009f, 0f]], id: triangles:iyca1fjjxvxvpqiokj06, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.32415997916666683f, -0.10232108333333328f, -0.8f], [-0.27781997916666684f, -0.17912708333333333f, -0.8f], [-0.32415997916666683f, -0.20468008333333337f, -0.8f]] }, { dirs: [[0.04633999999999999f, 0.025553000000000048f, 0f], [0.324207f, -0.17908199999999996f, 0f]], id: triangles:kcc55e4ipnsv5nbpuf1v, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.32415997916666683f, -0.20468008333333337f, -0.8f], [-0.27781997916666684f, -0.17912708333333333f, -0.8f], [0.00004702083333318896f, -0.38376208333333334f, -0.8f]] }, { dirs: [[0.23148999999999997f, -0.07680399999999998f, 0f], [0.138922f, -0.076787f, 0f]], id: triangles:m1q1s6a00acz58b5fpgj, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.00007697916666682403f, 0.23032491666666666f, -0.8f], [0.23141302083333315f, 0.15352091666666667f, -0.8f], [0.13884502083333317f, 0.15353791666666666f, -0.8f]] }, { dirs: [[0.000006000000000061512f, -0.05108900000000005f, 0f], [-0.23149599999999998f, -0.12787600000000005f, 0f]], id: triangles:n3wvqwmfofe5775ilpdi, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[-0.00008297916666688554f, 0.2814139166666667f, -0.8f], [-0.00007697916666682403f, 0.23032491666666666f, -0.8f], [-0.23157897916666687f, 0.15353791666666666f, -0.8f]] }, { dirs: [[0.23150200000000004f, 0.076787f, 0f], [0f, -0.05138999999999999f, 0f]], id: triangles:nbtssv41ocuk40i4xazc, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.23157897916666687f, 0.15353791666666666f, -0.8f], [-0.00007697916666682403f, 0.23032491666666666f, -0.8f], [-0.23157897916666687f, 0.10214791666666667f, -0.8f]] }, { dirs: [[0.416764f, -0.17907099999999998f, 0f], [0.416764f, -0.230263f, 0f]], id: triangles:o5kgt8zunmoxy4os4e10, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.41674097916666686f, -0.2557960833333333f, -0.8f], [0.00002302083333316496f, -0.4348670833333333f, -0.8f], [0.00002302083333316496f, -0.4860590833333333f, -0.8f]] }, { dirs: [[0.32408900000000007f, 0.127876f, 0f], [0.000030000000000085514f, -0.05118200000000006f, 0f]], id: triangles:ojvp5ru1sokejl8o68ua, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.000017020833333103447f, -0.3325800833333333f, -0.8f], [0.3241060208333332f, -0.2047040833333333f, -0.8f], [0.00004702083333318896f, -0.38376208333333334f, -0.8f]] }, { dirs: [[0.04633999999999999f, -0.025595999999999952f, 0f], [0.04633999999999999f, -0.486022f, 0f]], id: triangles:pfr4bqvb6jjywyv5bld8, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.41674097916666686f, 0.25582191666666665f, -0.8f], [-0.37040097916666687f, 0.2302259166666667f, -0.8f], [-0.37040097916666687f, -0.23020008333333336f, -0.8f]] }, { dirs: [[-0.277842f, -0.20461399999999996f, 0f], [-0.32418199999999997f, -0.178971f, 0f]], id: triangles:q6cuqa3gfh6z0f6kzp7v, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.000022020833333136203f, 0.38370091666666667f, -0.8f], [-0.27781997916666684f, 0.1790869166666667f, -0.8f], [-0.32415997916666683f, 0.20472991666666668f, -0.8f]] }, { dirs: [[0.32418299999999994f, 0.12788000000000005f, 0f], [-0.04633999999999999f, -0.07668799999999992f, 0f]], id: triangles:qjlmzxtbm0smu67chif5, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[-0.27781997916666684f, 0.025558916666666653f, -0.8f], [0.0463630208333331f, 0.1534389166666667f, -0.8f], [-0.32415997916666683f, -0.05112908333333327f, -0.8f]] }, { dirs: [[0f, -0.051192000000000015f, 0f], [-0.416764f, -0.23026300000000005f, 0f]], id: triangles:r4ny0hzhg3xicbi0o1g5, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[0.00002302083333316496f, 0.4860849166666667f, -0.8f], [0.00002302083333316496f, 0.4348929166666667f, -0.8f], [-0.41674097916666686f, 0.25582191666666665f, -0.8f]] }, { dirs: [[0.09256799999999998f, -0.000016999999999989246f, 0f], [-0.416665f, -0.281444f, 0f]], id: triangles:rdap1a99scpqgkyehzuw, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[0.13884502083333317f, 0.15353791666666666f, -0.8f], [0.23141302083333315f, 0.15352091666666667f, -0.8f], [-0.27781997916666684f, -0.12790608333333336f, -0.8f]] }, { dirs: [[0.04624099999999998f, -0.02549600000000002f, 0f], [-0.5093430000000001f, -0.28133199999999997f, 0f]], id: triangles:rrtv49q51dk8i5sl8fo4, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[0.2778650208333332f, 0.1278429166666667f, -0.8f], [0.3241060208333332f, 0.10234691666666668f, -0.8f], [-0.23147797916666685f, -0.15348908333333328f, -0.8f]] }, { dirs: [[0.04634000000000005f, -0.025595999999999952f, 0f], [-0.37042299999999995f, -0.20466699999999993f, 0f]], id: triangles:s7sxtwlwygk3v7ai6n9v, material: material:aar75tybb81obxg9ntjc, normal: [0f, -0f, -1f], vertices: [[0.3704460208333331f, -0.23020008333333336f, -0.8f], [0.41678602083333316f, -0.2557960833333333f, -0.8f], [0.00002302083333316496f, -0.4348670833333333f, -0.8f]] }, { dirs: [[0.04624099999999998f, 0.23021999999999998f, 0f], [0.04624099999999998f, -0.025638999999999967f, 0f]], id: triangles:sjomhu6slokpsi0p6hba, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[0.2778650208333332f, -0.17906508333333332f, -0.8f], [0.3241060208333332f, 0.05115491666666666f, -0.8f], [0.3241060208333332f, -0.2047040833333333f, -0.8f]] }, { dirs: [[0.04633999999999999f, 0.025595999999999952f, 0f], [0.416764f, -0.17907099999999998f, 0f]], id: triangles:xgg277x58n3nhsdf1kr0, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.41674097916666686f, -0.2557960833333333f, -0.8f], [-0.37040097916666687f, -0.23020008333333336f, -0.8f], [0.00002302083333316496f, -0.4348670833333333f, -0.8f]] }, { dirs: [[0.5555840000000001f, 0.25583599999999995f, 0f], [0.09257900000000002f, -0.000022999999999995246f, 0f]], id: triangles:y1tj021tzo4vw2o8qozu, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.23147797916666685f, -0.15348908333333328f, -0.8f], [0.3241060208333332f, 0.10234691666666668f, -0.8f], [-0.13889897916666682f, -0.15351208333333327f, -0.8f]] }, { dirs: [[-0.000006000000000061512f, 0.05108900000000005f, 0f], [0.23148999999999997f, -0.07680399999999998f, 0f]], id: triangles:z1069piaq3ilwi289h8h, material: material:aar75tybb81obxg9ntjc, normal: [0f, 0f, -1f], vertices: [[-0.00007697916666682403f, 0.23032491666666666f, -0.8f], [-0.00008297916666688554f, 0.2814139166666667f, -0.8f], [0.23141302083333315f, 0.15352091666666667f, -0.8f]] } ];

	RETURN "OK";
}
