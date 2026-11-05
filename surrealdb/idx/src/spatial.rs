//! Hierarchical Hilbert-curve spatial cell decomposition and region covering over the WGS84
//! `(lon, lat)` plane.
//!
//! This module implements a 64-bit hierarchical spatial cell identifier
//! ([`SpatialCellId`]) and adaptive region coverer for SurrealDB's `SPATIAL` index.
//!
//! ## Coordinate Space: Planar vs. Spherical Semantics
//! The quadtree hierarchy partitions the equirectangular WGS84 domain
//! `lon in [-180°, 180°]`, `lat in [-90°, 90°]` into $2^L \times 2^L$ axis-aligned `(lon, lat)`
//! rectangles at each level `L in 0..=28`, ordered along a 28-level Hilbert curve:
//! - **Planar predicates** (`CONTAINS`, `INSIDE`, `INTERSECTS`, `OUTSIDE`) operate in the `(lon,
//!   lat)` coordinate plane matching `geo`'s planar predicates. Out-of-domain finite coordinates
//!   are clamped (or covered via their clamped bounding box) so planar queries and indexed
//!   geometries agree on boundary cells.
//! - **Spherical metric predicates** (`geo::distance(col, center) <= radius_meters`) are covered by
//!   [`plan_cap_scans`] using conservative Haversine great-circle distance bounds
//!   (`min_haversine_distance_to_rect` / `max_haversine_distance_to_rect`) against each cell's
//!   `(lon, lat)` rectangle. This handles antimeridian (`±180°`) and trans-polar (`±90°`) caps
//!   without false negatives. Because constant-`\Delta lon` cells narrow in physical east-west
//!   width near the poles (`cos(lat) -> 0`), polar caps naturally span more longitudinal cells and
//!   rely on [`CoverConfig::max_cells`] and [`MAX_SPATIAL_SCAN_OPS`] to bound scan fan-out.
//!
//! ## Cell ID Layout (`i64`)
//! - Bit 63: `0` (always positive so signed `i64` order matches unsigned bit order).
//! - Bits 60..=62: `001` (header prefix ensuring all valid cell IDs are strictly positive).
//! - Bits 4..=59 (56 bits): 28 levels (`level = 1..=28`) of 2-bit Hilbert curve steps. Level `k`
//!   (`1 <= k <= 28`) occupies bits `(4 + 2 * (28 - k)) ..= (5 + 2 * (28 - k))`.
//! - Sentinel bit: for a cell at level `L` (`0 <= L <= 28`), bit `pos = 3 + 2 * (28 - L)` is `1`,
//!   and all bits `0..pos` are `0`.
//!
//! Because a cell `C` at level `L` has its sentinel `1` bit at `pos = 3 + 2 * (28 - L)`,
//! its own ID sits at the midpoint (`100...000` in bits `0..=pos`) of all its descendants
//! at levels `L' in L..=28`:
//! - [`SpatialCellId::range_min`] (`000...01000` in bits `0..=pos`) is the minimum descendant.
//! - [`SpatialCellId::range_max`] (`111...11000` in bits `0..=pos`) is the maximum descendant.
//! - Every descendant cell `D` at level `L' >= L` (and `C` itself) satisfies `C.range_min() <= D <=
//!   C.range_max()`, while no ancestor or non-descendant cell ever falls inside `[C.range_min(),
//!   C.range_max()]`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use geo::algorithm::contains::Contains;
use geo::algorithm::intersects::Intersects;
use geo::{Coord, Distance, Haversine, Point, Rect};

use crate::val::Geometry;

/// Maximum quadtree hierarchy depth along the Hilbert curve (28 levels = 56 Hilbert bits,
/// ~7.5 cm x 15 cm leaf resolution on Earth).
pub const MAX_LEVEL: u8 = 28;

/// Header prefix in bits 60..=62 (`001`), keeping all cell IDs strictly positive `i64`.
const HEADER_BITS: i64 = 1i64 << 60;

/// Geographic domain bounds in degrees.
const LON_MIN: f64 = -180.0;
const LON_MAX: f64 = 180.0;
const LON_SPAN: f64 = 360.0;
const LAT_MIN: f64 = -90.0;
const LAT_MAX: f64 = 90.0;
const LAT_SPAN: f64 = 180.0;

/// Boundary padding (in degrees, ~1 micrometer on Earth) used for conservative intersection
/// and interior containment checks to prevent floating-point rounding false negatives on cell
/// edges.
const BOUNDARY_EPS: f64 = 1e-11;

/// Mean Earth radius in meters used by `geo::Haversine`.
const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// Returns `true` if `(lon, lat)` is finite and within standard WGS84 bounds
/// (`lon in [-180, 180]`, `lat in [-90, 90]`).
pub fn point_in_wgs84_bounds(lon: f64, lat: f64) -> bool {
	(LON_MIN..=LON_MAX).contains(&lon) && (LAT_MIN..=LAT_MAX).contains(&lat)
}

/// Normalize a spherical `(lon, lat)` point in degrees into canonical WGS84 bounds
/// (`lon in [-180, 180]`, `lat in [-90, 90]`) so that trigonometric `geo::Haversine`
/// evaluations on out-of-domain coordinates map to the same physical point on the sphere.
pub fn normalize_spherical_point(pt: Point<f64>) -> Point<f64> {
	let lon = pt.x();
	let lat = pt.y();
	if point_in_wgs84_bounds(lon, lat) || !lon.is_finite() || !lat.is_finite() {
		return pt;
	}
	let (sin_lat, cos_lat) = lat.to_radians().sin_cos();
	let (sin_lon, cos_lon) = lon.to_radians().sin_cos();
	let x = cos_lat * cos_lon;
	let y = cos_lat * sin_lon;
	let z = sin_lat.clamp(-1.0, 1.0);
	let norm_lat = z.asin().to_degrees().clamp(LAT_MIN, LAT_MAX);
	let norm_lon = if x == 0.0 && y == 0.0 {
		0.0
	} else {
		y.atan2(x).to_degrees().clamp(LON_MIN, LON_MAX)
	};
	Point::new(norm_lon, norm_lat)
}

/// Top-down 4-state Hilbert curve lookup tables.
///
/// Quadrant index `q = (bx << 1) | by` in `0..4`:
/// - `q = 0`: `(0, 0)` (SW)
/// - `q = 1`: `(0, 1)` (NW)
/// - `q = 2`: `(1, 0)` (SE)
/// - `q = 3`: `(1, 1)` (NE)
///
/// `HILBERT_POS[state][quad]` -> 2-bit Hilbert step `h` in `0..4`.
const HILBERT_POS: [[u8; 4]; 4] = [
	// State 0: q sequence [0, 1, 3, 2]
	[0, 1, 3, 2],
	// State 1: q sequence [0, 2, 3, 1]
	[0, 3, 1, 2],
	// State 2: q sequence [3, 2, 0, 1]
	[2, 3, 1, 0],
	// State 3: q sequence [3, 1, 0, 2]
	[2, 1, 3, 0],
];

/// Inverse of `HILBERT_POS`: `HILBERT_QUAD[state][h]` -> quadrant `q = (bx << 1) | by`.
const HILBERT_QUAD: [[u8; 4]; 4] = [[0, 1, 3, 2], [0, 2, 3, 1], [3, 2, 0, 1], [3, 1, 0, 2]];

/// Next Hilbert orientation state given `(state, quad)`.
const HILBERT_NEXT_STATE: [[u8; 4]; 4] = [
	// State 0: h=0(q=0)->1, h=1(q=1)->0, h=2(q=3)->0, h=3(q=2)->3
	[1, 0, 3, 0],
	// State 1: h=0(q=0)->0, h=1(q=2)->1, h=2(q=3)->1, h=3(q=1)->2
	[0, 2, 1, 1],
	// State 2: h=0(q=3)->3, h=1(q=2)->2, h=2(q=0)->2, h=3(q=1)->1
	[2, 1, 2, 3],
	// State 3: h=0(q=3)->2, h=1(q=1)->3, h=2(q=0)->3, h=3(q=2)->0
	[3, 3, 0, 2],
];

/// A 64-bit hierarchical Hilbert-curve spatial cell identifier over the WGS84 `(lon, lat)` plane.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SpatialCellId(pub i64);

impl SpatialCellId {
	/// Root cell at level 0 covering the full domain.
	#[inline]
	pub const fn root() -> Self {
		let pos = 3 + 2 * (MAX_LEVEL as u32);
		Self(HEADER_BITS | (1i64 << pos))
	}

	/// Encode a 2D point `(lon, lat)` into its leaf cell at [`MAX_LEVEL`].
	#[inline]
	pub fn from_point(lon: f64, lat: f64) -> Self {
		let grid_size = (1u64 << MAX_LEVEL) as f64;
		let u = ((lon.clamp(LON_MIN, LON_MAX) - LON_MIN) / LON_SPAN).clamp(0.0, 1.0);
		let v = ((lat.clamp(LAT_MIN, LAT_MAX) - LAT_MIN) / LAT_SPAN).clamp(0.0, 1.0);
		let max_idx = (1u32 << MAX_LEVEL) - 1;
		let ix = ((u * grid_size).floor() as u64).min(max_idx as u64) as u32;
		let iy = ((v * grid_size).floor() as u64).min(max_idx as u64) as u32;
		Self::from_ij_level(ix, iy, MAX_LEVEL)
	}

	/// Encode grid coordinates `(ix, iy)` in `0..2^level` at hierarchy depth `level` (`0..=28`).
	#[inline]
	pub fn from_ij_level(ix: u32, iy: u32, level: u8) -> Self {
		debug_assert!(level <= MAX_LEVEL);
		let mut state = 0usize;
		let mut bits = HEADER_BITS;
		for k in 1..=level {
			let shift_xy = level - k;
			let bx = ((ix >> shift_xy) & 1) as u8;
			let by = ((iy >> shift_xy) & 1) as u8;
			let quad = ((bx << 1) | by) as usize;
			let h = HILBERT_POS[state][quad] as i64;
			state = HILBERT_NEXT_STATE[state][quad] as usize;
			let bit_pos = 4 + 2 * ((MAX_LEVEL - k) as u32);
			bits |= h << bit_pos;
		}
		let sentinel_pos = 3 + 2 * ((MAX_LEVEL - level) as u32);
		bits |= 1i64 << sentinel_pos;
		Self(bits)
	}

	/// Bit position of the sentinel `1` bit (`3 + 2 * (28 - level)`).
	#[inline]
	fn sentinel_pos(self) -> u32 {
		self.0.trailing_zeros()
	}

	/// Hierarchy level `L` in `0..=28`.
	#[inline]
	pub fn level(self) -> u8 {
		let pos = self.sentinel_pos();
		MAX_LEVEL - ((pos - 3) / 2) as u8
	}

	/// Ancestor cell at `parent_level` (`0 <= parent_level <= self.level()`).
	#[inline]
	pub fn parent(self, parent_level: u8) -> Self {
		debug_assert!(parent_level <= self.level());
		let parent_pos = 3 + 2 * ((MAX_LEVEL - parent_level) as u32);
		let clear_mask = !((1i64 << (parent_pos + 1)) - 1);
		Self((self.0 & clear_mask) | (1i64 << parent_pos))
	}

	/// The 4 direct child cells at `self.level() + 1` in Hilbert curve order.
	#[inline]
	pub fn children(self) -> [Self; 4] {
		let lvl = self.level();
		debug_assert!(lvl < MAX_LEVEL);
		let pos = self.sentinel_pos();
		let base = self.0 & !(1i64 << pos);
		let child_step_pos = pos - 1;
		let child_sentinel = 1i64 << (pos - 2);
		[
			Self(base | (0i64 << child_step_pos) | child_sentinel),
			Self(base | (1i64 << child_step_pos) | child_sentinel),
			Self(base | (2i64 << child_step_pos) | child_sentinel),
			Self(base | (3i64 << child_step_pos) | child_sentinel),
		]
	}

	/// Minimum descendant cell ID (at [`MAX_LEVEL`]) contained within `self`.
	#[inline]
	pub fn range_min(self) -> Self {
		let pos = self.sentinel_pos();
		Self((self.0 & !(1i64 << pos)) | (1i64 << 3))
	}

	/// Maximum descendant cell ID (at [`MAX_LEVEL`]) contained within `self`.
	#[inline]
	pub fn range_max(self) -> Self {
		let pos = self.sentinel_pos();
		let mask = ((1i64 << pos) - 1) & !0b111i64;
		Self(self.0 | mask)
	}

	/// Decode `(ix, iy, level)` grid coordinates for `self`.
	#[inline]
	pub fn to_ij_level(self) -> (u32, u32, u8) {
		let level = self.level();
		let mut state = 0usize;
		let mut ix = 0u32;
		let mut iy = 0u32;
		for k in 1..=level {
			let bit_pos = 4 + 2 * ((MAX_LEVEL - k) as u32);
			let h = ((self.0 >> bit_pos) & 0b11) as usize;
			let quad = HILBERT_QUAD[state][h];
			state = HILBERT_NEXT_STATE[state][quad as usize] as usize;
			let bx = (quad >> 1) & 1;
			let by = quad & 1;
			ix = (ix << 1) | (bx as u32);
			iy = (iy << 1) | (by as u32);
		}
		(ix, iy, level)
	}

	/// Exact axis-aligned bounding rectangle `[lon_min..=lon_max, lat_min..=lat_max]` of this cell.
	#[inline]
	pub fn rect(self) -> Rect<f64> {
		let (ix, iy, level) = self.to_ij_level();
		let inv = 1.0 / ((1u64 << level) as f64);
		let x_min = LON_MIN + LON_SPAN * (ix as f64) * inv;
		let x_max = LON_MIN + LON_SPAN * ((ix + 1) as f64) * inv;
		let y_min = LAT_MIN + LAT_SPAN * (iy as f64) * inv;
		let y_max = LAT_MIN + LAT_SPAN * ((iy + 1) as f64) * inv;
		Rect::new(
			Coord {
				x: x_min,
				y: y_min,
			},
			Coord {
				x: x_max,
				y: y_max,
			},
		)
	}

	/// Bounding rectangle expanded outward by `eps` in all directions for conservative checks.
	#[inline]
	fn expanded_rect(self, eps: f64) -> Rect<f64> {
		let r = self.rect();
		let min = r.min();
		let max = r.max();
		Rect::new(
			Coord {
				x: min.x - eps,
				y: min.y - eps,
			},
			Coord {
				x: max.x + eps,
				y: max.y + eps,
			},
		)
	}
}

/// Configuration for adaptive region covering.
#[derive(Clone, Copy, Debug)]
pub struct CoverConfig {
	/// Minimum cell level to emit (`0..=28`).
	pub min_level: u8,
	/// Maximum cell level to subdivide non-point geometries (`0..=28`).
	pub max_level: u8,
	/// Target maximum number of covering cells.
	pub max_cells: usize,
}

impl CoverConfig {
	/// Default covering budget when writing a geometry into a `SPATIAL` index.
	#[inline]
	pub const fn default_index() -> Self {
		Self {
			min_level: 0,
			max_level: 16,
			max_cells: 12,
		}
	}

	/// Default covering budget when expanding a spatial predicate in the query planner.
	#[inline]
	pub const fn default_query() -> Self {
		Self {
			min_level: 0,
			max_level: 16,
			max_cells: 24,
		}
	}
}

/// A single B-Tree scan operation produced by decomposing a spatial query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpatialScanOp {
	/// Point lookup for an exact ancestor cell ID `id`.
	Exact(i64),
	/// Inclusive range scan `[min, max]` covering a cell and all of its descendants.
	Range {
		min: i64,
		max: i64,
	},
}

impl SpatialScanOp {
	/// Returns `true` if this scan operation is a full-index descendant range over
	/// [`SpatialCellId::root()`].
	#[inline]
	pub fn covers_root(self) -> bool {
		let root = SpatialCellId::root();
		matches!(
			self,
			Self::Range {
				min,
				max,
			} if min <= root.range_min().0 && max >= root.range_max().0
		)
	}
}

/// Appends the index leaf cell(s) for a single `Point` to `out`: its planar-clamped leaf cell at
/// [`MAX_LEVEL`], plus its spherical-normalized leaf cell when `pt` has finite out-of-domain
/// coordinates (`|lon| > 180` or `|lat| > 90`). Non-finite points emit nothing.
#[inline]
fn push_index_point_cells(pt: Point<f64>, out: &mut Vec<SpatialCellId>) {
	if pt.x().is_finite() && pt.y().is_finite() {
		let clamped = SpatialCellId::from_point(pt.x(), pt.y());
		if point_in_wgs84_bounds(pt.x(), pt.y()) {
			out.push(clamped);
		} else {
			let norm = normalize_spherical_point(pt);
			let spherical = SpatialCellId::from_point(norm.x(), norm.y());
			if clamped == spherical {
				out.push(clamped);
			} else if clamped < spherical {
				out.push(clamped);
				out.push(spherical);
			} else {
				out.push(spherical);
				out.push(clamped);
			}
		}
	}
}

/// Cover a [`Geometry`] with Hilbert quadtree cells for the index write path:
///
/// - A single finite `Point` is indexed at its exact leaf cell (`MAX_LEVEL`) with no boundary
///   padding, so point-only tables store exactly 1 index entry per point. If the point has
///   out-of-domain finite coordinates (`|lon| > 180` or `|lat| > 90`), both its planar-clamped leaf
///   cell and its spherical-normalized leaf cell are emitted so both planar (`INTERSECTS`,
///   `CONTAINS`, `INSIDE`, `OUTSIDE`) and spherical (`geo::distance`) queries discover it.
///   Non-finite `Point`s emit no entries.
/// - `MultiPoint`s with all-finite coordinates emit exact leaf cells for each point and coarsen to
///   `config.max_cells` when the point count exceeds the per-record cell budget.
/// - Non-point geometries (`Line`, `Polygon`, `MultiLine`, `MultiPolygon`, `Collection`) are
///   adaptively covered up to `config.max_cells`.
/// - Empty geometries (`MultiPoint([])`, `MultiLine([])`, `MultiPolygon([])`, `Collection([])`, or
///   lines/polygons with no coordinates) and any non-`Point` geometry containing a non-finite
///   coordinate (`NaN` or `±inf`) emit `[SpatialCellId::root()]` so queries with ancestor probes
///   visit them and allow the residual `WHERE` filter to evaluate exact `geo` semantics.
pub fn cover_geometry_for_index(geom: &Geometry, config: &CoverConfig) -> Vec<SpatialCellId> {
	if let Geometry::Point(pt) = geom {
		let mut out = Vec::with_capacity(2);
		push_index_point_cells(*pt, &mut out);
		return out;
	}

	if !geometry_all_coords_finite(geom) || !geometry_has_any_coordinate(geom) {
		return vec![SpatialCellId::root()];
	}

	let cells = cover_geometry(geom, config, true);
	if cells.is_empty() {
		vec![SpatialCellId::root()]
	} else {
		cells
	}
}

/// Hard upper bound on the total number of [`SpatialScanOp`]s (descendant ranges plus ancestor
/// exact lookups) emitted by [`plan_geometry_scans`] and [`plan_cap_scans`].
pub const MAX_SPATIAL_SCAN_OPS: usize = 96;

/// Maximum forced subdivision depth allowed for [`CoverConfig::min_level`] before budget checks
/// take effect (`4^8 = 65_536` maximum forced cells).
const MAX_FORCED_MIN_LEVEL: u8 = 8;

/// Maximum total vertex count across a geometry before [`cover_geometry`] falls back to
/// coarse bounding-box covering instead of exact segment/ring subdivision.
const MAX_EXACT_GEOMETRY_VERTICES: usize = 16_384;

/// Number of consecutive segments grouped into a single bounding rectangle for fast candidate-cell
/// culling during quadtree subdivision.
const SEGMENT_CHUNK_SIZE: usize = 64;

fn cover_points(pts: &[Point<f64>], config: &CoverConfig, for_index: bool) -> Vec<SpatialCellId> {
	if pts.is_empty() {
		return Vec::new();
	}
	let batch_cap = config.max_cells.max(1).saturating_mul(16).max(64);
	if for_index {
		let mut cells = Vec::with_capacity(pts.len().min(batch_cap));
		for pt in pts {
			push_index_point_cells(*pt, &mut cells);
			if cells.len() >= batch_cap {
				cells = coarsen_to_budget(cells, config);
			}
		}
		if cells.is_empty() {
			return Vec::new();
		}
		return coarsen_to_budget(cells, config);
	}
	let target_level = config.max_level.min(MAX_LEVEL).max(config.min_level.min(MAX_LEVEL));
	let pad = 2.0 * BOUNDARY_EPS;
	let mut cells = Vec::with_capacity((pts.len().saturating_mul(4)).min(batch_cap));
	for pt in pts {
		if !pt.x().is_finite() || !pt.y().is_finite() {
			continue;
		}
		let cx = pt.x().clamp(LON_MIN, LON_MAX);
		let cy = pt.y().clamp(LAT_MIN, LAT_MAX);
		for dx in [-pad, pad] {
			for dy in [-pad, pad] {
				cells.push(SpatialCellId::from_point(cx + dx, cy + dy).parent(target_level));
			}
		}
		if cells.len() >= batch_cap {
			cells = coarsen_to_budget(cells, config);
		}
	}
	if cells.is_empty() {
		return Vec::new();
	}
	coarsen_to_budget(cells, config)
}

/// Adaptively cover any [`Geometry`] with at most `config.max_cells` cells (up to sibling merges).
fn cover_geometry(geom: &Geometry, config: &CoverConfig, for_index: bool) -> Vec<SpatialCellId> {
	match geom {
		Geometry::Point(pt) => return cover_points(std::slice::from_ref(pt), config, for_index),
		Geometry::MultiPoint(pts) => return cover_points(&pts.0, config, for_index),
		Geometry::Line(_)
		| Geometry::MultiLine(_)
		| Geometry::Polygon(_)
		| Geometry::MultiPolygon(_)
		| Geometry::Collection(_) => {}
	}

	let exceeds_vertex_limit = geometry_exceeds_exact_vertex_limit(geom);
	let mut parts = CollectedParts::default();
	collect_geometry_parts(geom, exceeds_vertex_limit, &mut parts);

	let has_shapes =
		!parts.lines.is_empty() || !parts.polys.is_empty() || !parts.fallback_rects.is_empty();
	if !has_shapes {
		return cover_points(&parts.points, config, for_index);
	}

	let line_groups = build_group_bboxes(&parts.lines, |l| l.bbox);
	let poly_groups = build_group_bboxes(&parts.polys, |p| p.bbox);
	let rect_groups = build_group_bboxes(&parts.fallback_rects, |r| Some(*r));

	let shape_cells = cover_with_predicates(
		config,
		|cell| {
			let exp = cell.expanded_rect(BOUNDARY_EPS);
			poly_groups.iter().any(|&(ref gbbox, s, e)| {
				gbbox.intersects(&exp) && parts.polys[s..e].iter().any(|p| p.intersects_rect(&exp))
			}) || line_groups.iter().any(|&(ref gbbox, s, e)| {
				gbbox.intersects(&exp) && parts.lines[s..e].iter().any(|l| l.intersects_rect(&exp))
			}) || rect_groups.iter().any(|&(ref gbbox, s, e)| {
				gbbox.intersects(&exp)
					&& parts.fallback_rects[s..e].iter().any(|r| r.intersects(&exp))
			})
		},
		|cell| {
			if poly_groups.is_empty() && rect_groups.is_empty() {
				return false;
			}
			let exp = cell.expanded_rect(BOUNDARY_EPS);
			poly_groups.iter().any(|&(ref gbbox, s, e)| {
				gbbox.contains(&exp) && parts.polys[s..e].iter().any(|p| p.contains_rect(&exp))
			}) || rect_groups.iter().any(|&(ref gbbox, s, e)| {
				gbbox.contains(&exp)
					&& parts.fallback_rects[s..e].iter().any(|r| {
						r.min().x <= exp.min().x
							&& r.max().x >= exp.max().x
							&& r.min().y <= exp.min().y
							&& r.max().y >= exp.max().y
					})
			})
		},
	);

	if parts.points.is_empty() {
		shape_cells
	} else {
		let mut combined = cover_points(&parts.points, config, for_index);
		combined.extend(shape_cells);
		coarsen_to_budget(combined, config)
	}
}

/// Adaptively cover a spherical Haversine cap `geo::distance(col, center) <= radius_meters`.
fn cover_cap(center: Point<f64>, radius_meters: f64, config: &CoverConfig) -> Vec<SpatialCellId> {
	if radius_meters < 0.0 || !center.x().is_finite() || !center.y().is_finite() {
		return Vec::new();
	}
	let center = normalize_spherical_point(center);
	let padded_radius = radius_meters * (1.0 + 1e-6) + 0.05;
	let inner_radius = (radius_meters * (1.0 - 1e-6) - 0.05).max(0.0);
	if padded_radius >= std::f64::consts::PI * EARTH_RADIUS_M && config.min_level == 0 {
		return vec![SpatialCellId::root()];
	}

	cover_with_predicates(
		config,
		|cell| {
			min_haversine_distance_to_rect(center, cell.expanded_rect(BOUNDARY_EPS))
				<= padded_radius
		},
		|cell| {
			inner_radius > 0.0
				&& max_haversine_distance_to_rect(center, cell.expanded_rect(BOUNDARY_EPS))
					<= inner_radius
		},
	)
}

/// Build the deduplicated list of [`SpatialScanOp`]s for a query [`Geometry`], optionally
/// emitting coarse ancestor probes (`include_ancestors = false` skips ancestor probes when the
/// indexed column is guaranteed to hold only single-cell points, which are always indexed at
/// [`MAX_LEVEL`]).
///
/// The total number of emitted scan operations (descendant ranges plus ancestor exact probes) is
/// bounded by [`MAX_SPATIAL_SCAN_OPS`]. Any geometry containing a non-finite coordinate returns
/// an empty scan list so the planner declines spatial index acceleration.
pub fn plan_geometry_scans(
	geom: &Geometry,
	config: &CoverConfig,
	include_ancestors: bool,
) -> Vec<SpatialScanOp> {
	plan_geometry_scans_with_max_ops(geom, config, include_ancestors, MAX_SPATIAL_SCAN_OPS)
}

/// Build the deduplicated list of [`SpatialScanOp`]s for a query [`Geometry`], bounded by a
/// caller-supplied `max_ops` budget (e.g. when sharing [`MAX_SPATIAL_SCAN_OPS`] across branches of
/// a spatial `OR` union).
pub fn plan_geometry_scans_with_max_ops(
	geom: &Geometry,
	config: &CoverConfig,
	include_ancestors: bool,
	max_ops: usize,
) -> Vec<SpatialScanOp> {
	plan_geometry_scans_with_area(geom, config, include_ancestors, max_ops).0
}

/// Build the deduplicated list of [`SpatialScanOp`]s for a query [`Geometry`], together with the
/// spherical surface-area fraction (`0.0..=1.0`) of the covering cells on the unit sphere, bounded
/// by a caller-supplied `max_ops` budget.
pub fn plan_geometry_scans_with_area(
	geom: &Geometry,
	config: &CoverConfig,
	include_ancestors: bool,
	max_ops: usize,
) -> (Vec<SpatialScanOp>, f64) {
	if max_ops == 0 || !geometry_all_coords_finite(geom) {
		return (Vec::new(), 1.0);
	}
	let effective_cfg = effective_query_cover_config(config, include_ancestors, max_ops);
	let cells = cover_geometry(geom, &effective_cfg, false);
	cells_to_bounded_scan_ops(cells, &effective_cfg, include_ancestors, max_ops)
}

/// Build the deduplicated list of [`SpatialScanOp`]s for a spherical cap
/// `geo::distance(col, center) <= radius_meters`, optionally emitting coarse ancestor probes.
///
/// The total number of emitted scan operations (descendant ranges plus ancestor exact probes) is
/// bounded by [`MAX_SPATIAL_SCAN_OPS`].
pub fn plan_cap_scans(
	center: Point<f64>,
	radius_meters: f64,
	config: &CoverConfig,
	include_ancestors: bool,
) -> Vec<SpatialScanOp> {
	plan_cap_scans_with_max_ops(
		center,
		radius_meters,
		config,
		include_ancestors,
		MAX_SPATIAL_SCAN_OPS,
	)
}

/// Build the deduplicated list of [`SpatialScanOp`]s for a spherical cap
/// `geo::distance(col, center) <= radius_meters`, bounded by a caller-supplied `max_ops` budget.
pub fn plan_cap_scans_with_max_ops(
	center: Point<f64>,
	radius_meters: f64,
	config: &CoverConfig,
	include_ancestors: bool,
	max_ops: usize,
) -> Vec<SpatialScanOp> {
	plan_cap_scans_with_area(center, radius_meters, config, include_ancestors, max_ops).0
}

/// Build the deduplicated list of [`SpatialScanOp`]s for a spherical cap
/// `geo::distance(col, center) <= radius_meters`, together with the spherical surface-area fraction
/// (`0.0..=1.0`) of the covering cells on the unit sphere, bounded by a caller-supplied `max_ops`
/// budget.
pub fn plan_cap_scans_with_area(
	center: Point<f64>,
	radius_meters: f64,
	config: &CoverConfig,
	include_ancestors: bool,
	max_ops: usize,
) -> (Vec<SpatialScanOp>, f64) {
	if max_ops == 0 {
		return (Vec::new(), 1.0);
	}
	let effective_cfg = effective_query_cover_config(config, include_ancestors, max_ops);
	let cells = cover_cap(center, radius_meters, &effective_cfg);
	cells_to_bounded_scan_ops(cells, &effective_cfg, include_ancestors, max_ops)
}

fn effective_query_cover_config(
	config: &CoverConfig,
	include_ancestors: bool,
	max_ops: usize,
) -> CoverConfig {
	if max_ops < MAX_SPATIAL_SCAN_OPS {
		CoverConfig {
			min_level: if include_ancestors {
				config.min_level.min(max_ops.saturating_sub(1) as u8)
			} else {
				config.min_level
			},
			max_level: config.max_level,
			max_cells: config.max_cells.min(max_ops.max(1)),
		}
	} else {
		*config
	}
}

/// Spherical surface-area fraction (`0.0..=1.0`) of a single quadtree cell on the unit sphere:
/// `((lon_max - lon_min) / 360.0) * ((sin(lat_max) - sin(lat_min)) / 2.0)`.
fn cell_spherical_area_fraction(cell: SpatialCellId) -> f64 {
	let r = cell.rect();
	let lon_frac = ((r.max().x - r.min().x).max(0.0) / LON_SPAN).clamp(0.0, 1.0);
	let lat_min = r.min().y.clamp(LAT_MIN, LAT_MAX).to_radians();
	let lat_max = r.max().y.clamp(LAT_MIN, LAT_MAX).to_radians();
	let lat_frac = ((lat_max.sin() - lat_min.sin()).max(0.0) / 2.0).clamp(0.0, 1.0);
	(lon_frac * lat_frac).clamp(0.0, 1.0)
}

fn cells_to_bounded_scan_ops(
	mut cells: Vec<SpatialCellId>,
	config: &CoverConfig,
	include_ancestors: bool,
	max_ops: usize,
) -> (Vec<SpatialScanOp>, f64) {
	let max_ops = max_ops.max(1);
	let mut ops = normalized_cells_to_scan_ops(&cells, include_ancestors);
	// When `include_ancestors` is true, lifting the deepest covering cells one level at a time
	// sheds up to `k` distinct `Exact` ancestor probes per level (where `k` is the number of
	// distinct parent chains across quadrant boundaries) while only doubling linear cell size,
	// whereas pairwise LCA merging (`coarsen_to_budget`) across a high-level quadrant boundary
	// would jump directly to a coarse shared ancestor (e.g. level 1 across `lon = 90°` / `lat =
	// 45°`). Lift depth uniformly first, then merge siblings if still needed.
	while ops.len() > max_ops && include_ancestors {
		let max_lvl = cells.iter().map(|c| c.level()).max().unwrap_or(0);
		if max_lvl <= 1 {
			break;
		}
		cells = normalize_covering(
			cells
				.into_iter()
				.map(|c| {
					if c.level() == max_lvl {
						c.parent(max_lvl - 1)
					} else {
						c
					}
				})
				.collect(),
		);
		ops = normalized_cells_to_scan_ops(&cells, include_ancestors);
	}
	while ops.len() > max_ops && cells.len() > 1 {
		let target_cells = (cells.len() * max_ops / ops.len()).max(1).min(cells.len() - 1);
		let coarsen_cfg = CoverConfig {
			min_level: 1,
			max_level: config.max_level,
			max_cells: target_cells,
		};
		let next_cells = coarsen_to_budget(cells.clone(), &coarsen_cfg);
		if next_cells.len() == cells.len() {
			break;
		}
		cells = next_cells;
		ops = normalized_cells_to_scan_ops(&cells, include_ancestors);
	}
	if ops.len() > max_ops && !cells.is_empty() {
		cells = vec![SpatialCellId::root()];
		ops = normalized_cells_to_scan_ops(&cells, include_ancestors);
	}
	let area = cells.iter().copied().map(cell_spherical_area_fraction).sum::<f64>().clamp(0.0, 1.0);
	(ops, area)
}

/// Convert a set of covering cells into descendant `Range` scans plus (when `include_ancestors`
/// is `true`) deduplicated `Exact` ancestor lookups for levels `0..cell.level()`.
///
/// Consecutive covering cells whose leaf Hilbert intervals `[range_min, range_max]` are
/// adjacent along the Hilbert curve (`prev_max + 16 == next_min`) are coalesced into a single
/// `SpatialScanOp::Range`, which also absorbs any shared ancestor cell ID (`prev_max + 8`) that
/// sits at the midpoint between their subtrees.
#[cfg(test)]
fn cells_to_scan_ops(cells: &[SpatialCellId], include_ancestors: bool) -> Vec<SpatialScanOp> {
	if cells.is_empty() {
		return Vec::new();
	}
	let normalized = normalize_covering(cells.to_vec());
	normalized_cells_to_scan_ops(&normalized, include_ancestors)
}

fn normalized_cells_to_scan_ops(
	normalized: &[SpatialCellId],
	include_ancestors: bool,
) -> Vec<SpatialScanOp> {
	if normalized.is_empty() {
		return Vec::new();
	}
	let mut ops = Vec::with_capacity(if include_ancestors {
		normalized.len() * 3
	} else {
		normalized.len()
	});
	let mut ancestors = if include_ancestors {
		Vec::with_capacity(normalized.len() * 16)
	} else {
		Vec::new()
	};
	let mut current_range: Option<(i64, i64)> = None;

	let flush_range = |ops: &mut Vec<SpatialScanOp>, (min, max): (i64, i64)| {
		if min == max {
			ops.push(SpatialScanOp::Exact(min));
		} else {
			ops.push(SpatialScanOp::Range {
				min,
				max,
			});
		}
	};

	for &cell in normalized {
		let min = cell.range_min().0;
		let max = cell.range_max().0;
		match current_range {
			Some((cur_min, cur_max)) if cur_max.saturating_add(16) == min => {
				current_range = Some((cur_min, max));
			}
			Some(prev) => {
				flush_range(&mut ops, prev);
				current_range = Some((min, max));
			}
			None => {
				current_range = Some((min, max));
			}
		}
		if include_ancestors {
			let lvl = cell.level();
			for anc_lvl in 0..lvl {
				ancestors.push(cell.parent(anc_lvl).0);
			}
		}
	}
	if let Some(last) = current_range {
		flush_range(&mut ops, last);
	}

	if include_ancestors {
		ancestors.sort_unstable();
		ancestors.dedup();
		// Both `ops[..range_ops_len]` and `ancestors` are sorted ascending, and the ranges in
		// `ops[..range_ops_len]` are disjoint, so a single linear cursor skips any ancestor
		// already absorbed by a descendant or coalesced range.
		let range_ops_len = ops.len();
		let mut op_idx = 0;
		for anc_id in ancestors {
			while op_idx < range_ops_len {
				let op_max = match ops[op_idx] {
					SpatialScanOp::Exact(id) => id,
					SpatialScanOp::Range {
						max,
						..
					} => max,
				};
				if op_max < anc_id {
					op_idx += 1;
				} else {
					break;
				}
			}
			let already_in_range = op_idx < range_ops_len
				&& match ops[op_idx] {
					SpatialScanOp::Exact(id) => anc_id == id,
					SpatialScanOp::Range {
						min,
						..
					} => anc_id >= min,
				};
			if !already_in_range {
				ops.push(SpatialScanOp::Exact(anc_id));
			}
		}
	}

	ops
}

/// Priority-queue candidate during adaptive quadtree subdivision.
/// Coarser cells (smaller `cell.level()`) have higher priority so the coverer refines the largest
/// boundary cells first.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Candidate {
	cell: SpatialCellId,
	intersecting_children: u8,
}

impl Ord for Candidate {
	fn cmp(&self, other: &Self) -> Ordering {
		// Lower level = larger cell = higher priority; break ties by fewer intersecting children
		// (tighter fit gain) and then cell ID.
		other
			.cell
			.level()
			.cmp(&self.cell.level())
			.then_with(|| other.intersecting_children.cmp(&self.intersecting_children))
			.then_with(|| other.cell.cmp(&self.cell))
	}
}

impl PartialOrd for Candidate {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

fn cover_with_predicates<FInt, FCont>(
	config: &CoverConfig,
	may_intersect: FInt,
	is_contained: FCont,
) -> Vec<SpatialCellId>
where
	FInt: Fn(SpatialCellId) -> bool,
	FCont: Fn(SpatialCellId) -> bool,
{
	let max_level = config.max_level.min(MAX_LEVEL);
	let min_level = config.min_level.min(max_level).min(MAX_FORCED_MIN_LEVEL);
	let max_cells = config.max_cells.max(1);

	let root = SpatialCellId::root();
	if !may_intersect(root) {
		return Vec::new();
	}
	if max_level == 0 || (min_level == 0 && is_contained(root)) {
		return vec![root];
	}

	let mut result: Vec<SpatialCellId> = Vec::new();
	let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
	heap.push(Candidate {
		cell: root,
		intersecting_children: 4,
	});

	while let Some(cand) = heap.pop() {
		let cell = cand.cell;
		let lvl = cell.level();

		// Evaluate the 4 children.
		let children = cell.children();
		let mut active_children = Vec::with_capacity(4);
		for child in children {
			if may_intersect(child) {
				active_children.push(child);
			}
		}

		if active_children.is_empty() {
			// Conservative fallback: keep parent if epsilon boundary touched parent but no child.
			result.push(cell);
			continue;
		}

		// Always subdivide if only 1 child intersects (zero cell-count growth!) or if we are
		// below min_level, or if subdividing stays within the max_cells budget.
		let total_after_split = result.len() + heap.len() + active_children.len();
		if lvl < min_level || active_children.len() == 1 || total_after_split <= max_cells {
			let count = active_children.len() as u8;
			for child in active_children {
				let child_lvl = lvl + 1;
				if child_lvl >= max_level || (child_lvl >= min_level && is_contained(child)) {
					result.push(child);
				} else {
					heap.push(Candidate {
						cell: child,
						intersecting_children: count,
					});
				}
			}
		} else {
			// Budget reached for this multi-child split: emit current cell and allow remaining
			// heap candidates (which may still have single-child free refinements) to proceed.
			result.push(cell);
		}
	}

	normalize_covering(result)
}

/// Deduplicate cells, remove any cell whose ancestor is already present, and coalesce any
/// complete group of 4 sibling cells into their parent cell.
fn normalize_covering(mut cells: Vec<SpatialCellId>) -> Vec<SpatialCellId> {
	if cells.len() <= 1 {
		return cells;
	}

	// Sort by range_min ascending, and if equal, coarser level (wider range_max) first.
	cells.sort_unstable_by(|a, b| {
		a.range_min().cmp(&b.range_min()).then_with(|| b.range_max().cmp(&a.range_max()))
	});

	// In-place stack fold: skip any cell contained inside the previous cell's subtree range,
	// and coalesce any complete group of 4 sibling cells into their parent cell.
	let mut write = 0usize;
	for read in 0..cells.len() {
		let cell = cells[read];
		if write > 0 {
			let last = cells[write - 1];
			if cell.range_min() >= last.range_min() && cell.range_max() <= last.range_max() {
				continue;
			}
		}
		cells[write] = cell;
		write += 1;

		while write >= 4 {
			let c0 = cells[write - 4];
			let lvl = c0.level();
			if lvl == 0
				|| cells[write - 3].level() != lvl
				|| cells[write - 2].level() != lvl
				|| cells[write - 1].level() != lvl
			{
				break;
			}
			let parent = c0.parent(lvl - 1);
			if cells[write - 3].parent(lvl - 1) == parent
				&& cells[write - 2].parent(lvl - 1) == parent
				&& cells[write - 1].parent(lvl - 1) == parent
			{
				cells[write - 4] = parent;
				write -= 3;
			} else {
				break;
			}
		}
	}

	cells.truncate(write);
	cells
}

/// Minimum Haversine distance (in meters) from `center` `(lon0, lat0)` to any point in `rect`.
fn min_haversine_distance_to_rect(center: Point<f64>, rect: Rect<f64>) -> f64 {
	let lon0 = center.x();
	let lat0 = center.y();
	let min = rect.min();
	let max = rect.max();
	let lon_min = min.x.clamp(LON_MIN, LON_MAX);
	let lon_max = max.x.clamp(LON_MIN, LON_MAX);
	let lat_min = min.y.clamp(LAT_MIN, LAT_MAX);
	let lat_max = max.y.clamp(LAT_MIN, LAT_MAX);

	// Check if center is inside the rectangle (accounting for antimeridian wrap).
	let lon_inside = lon0 >= lon_min && lon0 <= lon_max;
	let lat_inside = lat0 >= lat_min && lat0 <= lat_max;
	if lon_inside && lat_inside {
		return 0.0;
	}

	if lon_inside {
		let lat_clamped = lat0.clamp(lat_min, lat_max);
		return Haversine.distance(center, Point::new(lon0, lat_clamped));
	}

	// Evaluate distance to both bounding meridian segments `lon_min` and `lon_max` and both
	// bounding parallels `lat_min` and `lat_max`.
	let phi0 = lat0.to_radians();
	let sin_phi0 = phi0.sin();
	let cos_phi0 = phi0.cos();

	let mut best = f64::INFINITY;
	for &lon_edge in &[lon_min, lon_max] {
		let cos_dlon = (lon_edge - lon0).to_radians().cos();
		// Stationary point of cos(d/R) = sin(phi0)*sin(phi) + cos(phi0)*cos(phi)*cos(d_lon)
		// on [-90°, 90°] exists when cos(d_lon) > 0: tan(phi*) = tan(phi0) / cos(d_lon).
		if cos_dlon > 0.0 {
			let phi_star_deg =
				sin_phi0.atan2(cos_phi0 * cos_dlon).to_degrees().clamp(lat_min, lat_max);
			let d = Haversine.distance(center, Point::new(lon_edge, phi_star_deg));
			if d < best {
				best = d;
			}
		}
		for lat_cand in [lat0.clamp(lat_min, lat_max), lat_min, lat_max] {
			let d = Haversine.distance(center, Point::new(lon_edge, lat_cand));
			if d < best {
				best = d;
			}
		}
	}

	best
}

/// Conservative upper bound on the Haversine distance (in meters) from `center` to any point in
/// `rect`.
fn max_haversine_distance_to_rect(center: Point<f64>, rect: Rect<f64>) -> f64 {
	let min = rect.min();
	let max = rect.max();
	let lon_min = min.x.clamp(LON_MIN, LON_MAX);
	let lon_max = max.x.clamp(LON_MIN, LON_MAX);
	let lat_min = min.y.clamp(LAT_MIN, LAT_MAX);
	let lat_max = max.y.clamp(LAT_MIN, LAT_MAX);

	// Only allow early interior containment for cells narrower than 90 degrees that lie entirely
	// within the near longitudinal hemisphere (|dlon| <= 90 degrees) so the maximum distance is
	// guaranteed to occur at one of the 4 corners and cannot peak at an interior antipodal
	// meridian.
	let dlon_min = (lon_min - center.x()).rem_euclid(360.0);
	let dlon_max = (lon_max - center.x()).rem_euclid(360.0);
	if (lon_max - lon_min) > 90.0
		|| (lat_max - lat_min) > 90.0
		|| (dlon_min > 90.0 && dlon_min < 270.0)
		|| (dlon_max > 90.0 && dlon_max < 270.0)
	{
		return f64::INFINITY;
	}

	let corners = [
		Point::new(lon_min, lat_min),
		Point::new(lon_min, lat_max),
		Point::new(lon_max, lat_min),
		Point::new(lon_max, lat_max),
	];
	corners.iter().map(|&c| Haversine.distance(center, c)).fold(0.0f64, f64::max)
}

fn geometry_bounding_rect(geom: &Geometry) -> Option<Rect<f64>> {
	fn add_coord(bounds: &mut Option<(Coord<f64>, Coord<f64>)>, c: Coord<f64>) {
		if !c.x.is_finite() || !c.y.is_finite() {
			return;
		}
		*bounds = Some(match *bounds {
			None => (c, c),
			Some((min, max)) => (
				Coord {
					x: min.x.min(c.x),
					y: min.y.min(c.y),
				},
				Coord {
					x: max.x.max(c.x),
					y: max.y.max(c.y),
				},
			),
		});
	}
	fn walk(geom: &Geometry, bounds: &mut Option<(Coord<f64>, Coord<f64>)>) {
		match geom {
			Geometry::Point(p) => add_coord(bounds, p.0),
			Geometry::Line(l) => {
				for &c in &l.0 {
					add_coord(bounds, c);
				}
			}
			Geometry::Polygon(p) => {
				for &c in &p.exterior().0 {
					add_coord(bounds, c);
				}
				for ring in p.interiors() {
					for &c in &ring.0 {
						add_coord(bounds, c);
					}
				}
			}
			Geometry::MultiPoint(mp) => {
				for p in &mp.0 {
					add_coord(bounds, p.0);
				}
			}
			Geometry::MultiLine(ml) => {
				for l in &ml.0 {
					for &c in &l.0 {
						add_coord(bounds, c);
					}
				}
			}
			Geometry::MultiPolygon(mp) => {
				for p in &mp.0 {
					for &c in &p.exterior().0 {
						add_coord(bounds, c);
					}
					for ring in p.interiors() {
						for &c in &ring.0 {
							add_coord(bounds, c);
						}
					}
				}
			}
			Geometry::Collection(col) => {
				for g in col {
					walk(g, bounds);
				}
			}
		}
	}
	let mut bounds = None;
	walk(geom, &mut bounds);
	bounds.map(|(min, max)| Rect::new(min, max))
}

/// Returns the `(min_x, max_x, min_y, max_y)` axis-aligned bounding box of `geom`.
pub fn geometry_bounding_box(geom: &Geometry) -> Option<(f64, f64, f64, f64)> {
	let rect = geometry_bounding_rect(geom)?;
	Some((rect.min().x, rect.max().x, rect.min().y, rect.max().y))
}

/// Returns `true` if every coordinate in `geom` is finite (`!x.is_nan() && !y.is_nan() &&
/// x.is_finite() && y.is_finite()`).
pub fn geometry_all_coords_finite(geom: &Geometry) -> bool {
	fn coord_finite(c: &Coord<f64>) -> bool {
		c.x.is_finite() && c.y.is_finite()
	}
	match geom {
		Geometry::Point(pt) => coord_finite(&pt.0),
		Geometry::MultiPoint(mp) => mp.0.iter().all(|pt| coord_finite(&pt.0)),
		Geometry::Line(l) => l.0.iter().all(coord_finite),
		Geometry::MultiLine(ml) => ml.0.iter().all(|l| l.0.iter().all(coord_finite)),
		Geometry::Polygon(p) => {
			p.exterior().0.iter().all(coord_finite)
				&& p.interiors().iter().all(|r| r.0.iter().all(coord_finite))
		}
		Geometry::MultiPolygon(mp) => mp.0.iter().all(|p| {
			p.exterior().0.iter().all(coord_finite)
				&& p.interiors().iter().all(|r| r.0.iter().all(coord_finite))
		}),
		Geometry::Collection(col) => col.iter().all(geometry_all_coords_finite),
	}
}

#[inline]
fn common_ancestor_level(a: SpatialCellId, b: SpatialCellId) -> u8 {
	let max_common = a.level().min(b.level());
	let diff = ((a.0 ^ b.0) as u64) >> 4;
	if diff == 0 {
		return max_common;
	}
	// Bits 0..56 of `diff` hold the 28 2-bit Hilbert steps from level 1 (bits 54..=55)
	// down to level 28 (bits 0..=1). Thus `leading_zeros().saturating_sub(8) / 2` is the
	// number of matching 2-bit levels from the root.
	let matching_levels = (diff.leading_zeros().saturating_sub(8) / 2) as u8;
	matching_levels.min(max_common)
}

fn coarsen_to_budget(cells: Vec<SpatialCellId>, config: &CoverConfig) -> Vec<SpatialCellId> {
	let mut normalized = normalize_covering(cells);
	let max_cells = config.max_cells.max(1);
	let min_level = config.min_level.min(MAX_LEVEL).min(MAX_FORCED_MIN_LEVEL);
	while normalized.len() > max_cells {
		// Find the adjacent pair in Hilbert order with the deepest common ancestor
		// (`lca_level >= min_level.max(1)`), breaking ties by the deeper constituent cell.
		let mut best_pair: Option<(usize, u8, u8)> = None;
		for i in 0..(normalized.len() - 1) {
			let a = normalized[i];
			let b = normalized[i + 1];
			let lca_lvl = common_ancestor_level(a, b);
			if lca_lvl < min_level.max(1) {
				continue;
			}
			let max_pair_lvl = a.level().max(b.level());
			let better = match best_pair {
				None => true,
				Some((_, best_lca, best_max_lvl)) => {
					lca_lvl > best_lca || (lca_lvl == best_lca && max_pair_lvl > best_max_lvl)
				}
			};
			if better {
				best_pair = Some((i, lca_lvl, max_pair_lvl));
			}
		}

		if let Some((idx, lca_lvl, _)) = best_pair {
			let lca = normalized[idx].parent(lca_lvl);
			normalized[idx] = lca;
			normalized.remove(idx + 1);
			normalized = normalize_covering(normalized);
		} else if min_level == 0 {
			return vec![SpatialCellId::root()];
		} else {
			break;
		}
	}
	normalized
}

fn polygon_vertex_count(poly: &geo::Polygon<f64>) -> usize {
	poly.exterior().0.len() + poly.interiors().iter().map(|r| r.0.len()).sum::<usize>()
}

/// Counts only line and polygon vertices (which participate in segment/ring subdivision);
/// `Point` and `MultiPoint` vertices are covered in linear time by `cover_points`.
fn geometry_line_poly_vertex_count(geom: &Geometry) -> usize {
	match geom {
		Geometry::Point(_) | Geometry::MultiPoint(_) => 0,
		Geometry::Line(l) => l.0.len(),
		Geometry::MultiLine(ml) => ml.0.iter().map(|l| l.0.len()).sum(),
		Geometry::Polygon(p) => polygon_vertex_count(p),
		Geometry::MultiPolygon(mp) => mp.0.iter().map(polygon_vertex_count).sum(),
		Geometry::Collection(col) => col.iter().map(geometry_line_poly_vertex_count).sum(),
	}
}

fn geometry_exceeds_exact_vertex_limit(geom: &Geometry) -> bool {
	geometry_line_poly_vertex_count(geom) > MAX_EXACT_GEOMETRY_VERTICES
}

struct LineChunks<'a> {
	line: &'a geo::LineString<f64>,
	bbox: Option<Rect<f64>>,
	chunk_rects: Vec<(Rect<f64>, usize, usize)>,
}

impl<'a> LineChunks<'a> {
	fn new(line: &'a geo::LineString<f64>) -> Self {
		let coords = &line.0;
		let bbox = if coords.is_empty() {
			None
		} else {
			let mut min = coords[0];
			let mut max = coords[0];
			for &c in &coords[1..] {
				min.x = min.x.min(c.x);
				min.y = min.y.min(c.y);
				max.x = max.x.max(c.x);
				max.y = max.y.max(c.y);
			}
			Some(Rect::new(min, max))
		};
		if coords.len() <= SEGMENT_CHUNK_SIZE {
			return Self {
				line,
				bbox,
				chunk_rects: Vec::new(),
			};
		}
		let num_segments = coords.len().saturating_sub(1);
		let mut chunk_rects = Vec::with_capacity(num_segments.div_ceil(SEGMENT_CHUNK_SIZE));
		let mut start = 0;
		while start < num_segments {
			let end = (start + SEGMENT_CHUNK_SIZE).min(num_segments);
			let slice = &coords[start..=end];
			let mut min = slice[0];
			let mut max = slice[0];
			for &c in &slice[1..] {
				min.x = min.x.min(c.x);
				min.y = min.y.min(c.y);
				max.x = max.x.max(c.x);
				max.y = max.y.max(c.y);
			}
			chunk_rects.push((Rect::new(min, max), start, end));
			start = end;
		}
		Self {
			line,
			bbox,
			chunk_rects,
		}
	}

	fn intersects_rect(&self, rect: &Rect<f64>) -> bool {
		let Some(ref bbox) = self.bbox else {
			return false;
		};
		if !bbox.intersects(rect) {
			return false;
		}
		if self.chunk_rects.is_empty() {
			return rect.intersects(self.line);
		}
		for &(ref chunk_rect, start, end) in &self.chunk_rects {
			if chunk_rect.intersects(rect) {
				for seg in self.line.0[start..=end].windows(2) {
					if rect.intersects(&geo::Line::new(seg[0], seg[1])) {
						return true;
					}
				}
			}
		}
		false
	}
}

struct PolygonChunks<'a> {
	poly: &'a geo::Polygon<f64>,
	bbox: Option<Rect<f64>>,
	exterior: LineChunks<'a>,
	interiors: Vec<LineChunks<'a>>,
}

impl<'a> PolygonChunks<'a> {
	fn new(poly: &'a geo::Polygon<f64>) -> Self {
		let exterior = LineChunks::new(poly.exterior());
		let interiors: Vec<LineChunks<'a>> = poly.interiors().iter().map(LineChunks::new).collect();
		let bbox = interiors.iter().filter_map(|r| r.bbox).fold(exterior.bbox, |acc, r| {
			Some(match acc {
				None => r,
				Some(a) => Rect::new(
					Coord {
						x: a.min().x.min(r.min().x),
						y: a.min().y.min(r.min().y),
					},
					Coord {
						x: a.max().x.max(r.max().x),
						y: a.max().y.max(r.max().y),
					},
				),
			})
		});
		Self {
			poly,
			bbox,
			exterior,
			interiors,
		}
	}

	fn intersects_rect(&self, rect: &Rect<f64>) -> bool {
		let Some(ref bbox) = self.bbox else {
			return false;
		};
		if !bbox.intersects(rect) {
			return false;
		}
		self.exterior.intersects_rect(rect)
			|| self.interiors.iter().any(|ring| ring.intersects_rect(rect))
			|| self.poly.contains(&rect.center())
	}

	fn contains_rect(&self, rect: &Rect<f64>) -> bool {
		let Some(ref bbox) = self.bbox else {
			return false;
		};
		if !bbox.contains(rect) {
			return false;
		}
		!self.exterior.intersects_rect(rect)
			&& self.interiors.iter().all(|ring| !ring.intersects_rect(rect))
			&& self.poly.contains(&rect.center())
	}
}

#[derive(Default)]
struct CollectedParts<'a> {
	points: Vec<Point<f64>>,
	lines: Vec<LineChunks<'a>>,
	polys: Vec<PolygonChunks<'a>>,
	fallback_rects: Vec<Rect<f64>>,
}

fn coord_in_domain(c: &Coord<f64>) -> bool {
	(LON_MIN..=LON_MAX).contains(&c.x) && (LAT_MIN..=LAT_MAX).contains(&c.y)
}

fn line_in_domain(l: &geo::LineString<f64>) -> bool {
	l.0.len() >= 2 && l.0.iter().all(coord_in_domain)
}

fn ring_in_domain(r: &geo::LineString<f64>) -> bool {
	r.0.len() >= 4 && r.0.iter().all(coord_in_domain)
}

fn poly_in_domain(p: &geo::Polygon<f64>) -> bool {
	ring_in_domain(p.exterior()) && p.interiors().iter().all(ring_in_domain)
}

fn clamped_padded_rect_from_coords(coords: impl Iterator<Item = Coord<f64>>) -> Option<Rect<f64>> {
	let mut bounds: Option<(Coord<f64>, Coord<f64>)> = None;
	for c in coords {
		if !c.x.is_finite() || !c.y.is_finite() {
			continue;
		}
		bounds = Some(match bounds {
			None => (c, c),
			Some((min, max)) => (
				Coord {
					x: min.x.min(c.x),
					y: min.y.min(c.y),
				},
				Coord {
					x: max.x.max(c.x),
					y: max.y.max(c.y),
				},
			),
		});
	}
	let (min, max) = bounds?;
	let cx_min = min.x.clamp(LON_MIN, LON_MAX);
	let cy_min = min.y.clamp(LAT_MIN, LAT_MAX);
	let cx_max = max.x.clamp(LON_MIN, LON_MAX);
	let cy_max = max.y.clamp(LAT_MIN, LAT_MAX);
	Some(Rect::new(
		Coord {
			x: (cx_min - BOUNDARY_EPS).max(LON_MIN),
			y: (cy_min - BOUNDARY_EPS).max(LAT_MIN),
		},
		Coord {
			x: (cx_max + BOUNDARY_EPS).min(LON_MAX),
			y: (cy_max + BOUNDARY_EPS).min(LAT_MAX),
		},
	))
}

fn push_line_fallback_rects(line: &geo::LineString<f64>, out: &mut Vec<Rect<f64>>) {
	let coords = &line.0;
	if coords.is_empty() {
		return;
	}
	if coords.len() <= SEGMENT_CHUNK_SIZE {
		if let Some(r) = clamped_padded_rect_from_coords(coords.iter().copied()) {
			out.push(r);
		}
		return;
	}
	let num_segments = coords.len().saturating_sub(1);
	let mut start = 0;
	while start < num_segments {
		let end = (start + SEGMENT_CHUNK_SIZE).min(num_segments);
		if let Some(r) = clamped_padded_rect_from_coords(coords[start..=end].iter().copied()) {
			out.push(r);
		}
		start = end;
	}
}

fn push_polygon_fallback_rect(poly: &geo::Polygon<f64>, out: &mut Vec<Rect<f64>>) {
	let iter = poly
		.exterior()
		.0
		.iter()
		.copied()
		.chain(poly.interiors().iter().flat_map(|r| r.0.iter().copied()));
	if let Some(r) = clamped_padded_rect_from_coords(iter) {
		out.push(r);
	}
}

fn collect_geometry_parts<'a>(
	geom: &'a Geometry,
	exceeds_vertex_limit: bool,
	parts: &mut CollectedParts<'a>,
) {
	match geom {
		Geometry::Point(pt) => {
			if pt.x().is_finite() && pt.y().is_finite() {
				parts.points.push(*pt);
			}
		}
		Geometry::MultiPoint(mp) => {
			parts
				.points
				.extend(mp.0.iter().copied().filter(|pt| pt.x().is_finite() && pt.y().is_finite()));
		}
		Geometry::Line(l) => {
			if !exceeds_vertex_limit && line_in_domain(l) {
				parts.lines.push(LineChunks::new(l));
			} else {
				push_line_fallback_rects(l, &mut parts.fallback_rects);
			}
		}
		Geometry::MultiLine(ml) => {
			for l in &ml.0 {
				if !exceeds_vertex_limit && line_in_domain(l) {
					parts.lines.push(LineChunks::new(l));
				} else {
					push_line_fallback_rects(l, &mut parts.fallback_rects);
				}
			}
		}
		Geometry::Polygon(p) => {
			if !exceeds_vertex_limit && poly_in_domain(p) {
				parts.polys.push(PolygonChunks::new(p));
			} else {
				push_polygon_fallback_rect(p, &mut parts.fallback_rects);
			}
		}
		Geometry::MultiPolygon(mp) => {
			for p in &mp.0 {
				if !exceeds_vertex_limit && poly_in_domain(p) {
					parts.polys.push(PolygonChunks::new(p));
				} else {
					push_polygon_fallback_rect(p, &mut parts.fallback_rects);
				}
			}
		}
		Geometry::Collection(col) => {
			for item in col {
				collect_geometry_parts(item, exceeds_vertex_limit, parts);
			}
		}
	}
}

fn build_group_bboxes<T>(
	items: &[T],
	bbox_of: impl Fn(&T) -> Option<Rect<f64>>,
) -> Vec<(Rect<f64>, usize, usize)> {
	if items.is_empty() {
		return Vec::new();
	}
	let mut groups = Vec::with_capacity(items.len().div_ceil(SEGMENT_CHUNK_SIZE));
	let mut start = 0;
	while start < items.len() {
		let end = (start + SEGMENT_CHUNK_SIZE).min(items.len());
		let mut acc: Option<Rect<f64>> = None;
		for item in &items[start..end] {
			if let Some(r) = bbox_of(item) {
				acc = Some(match acc {
					None => r,
					Some(cur) => Rect::new(
						Coord {
							x: cur.min().x.min(r.min().x),
							y: cur.min().y.min(r.min().y),
						},
						Coord {
							x: cur.max().x.max(r.max().x),
							y: cur.max().y.max(r.max().y),
						},
					),
				});
			}
		}
		if let Some(gbbox) = acc {
			groups.push((gbbox, start, end));
		}
		start = end;
	}
	groups
}

fn geometry_has_any_coordinate(geom: &Geometry) -> bool {
	match geom {
		Geometry::Point(_) => true,
		Geometry::Line(l) => !l.0.is_empty(),
		Geometry::Polygon(p) => {
			!p.exterior().0.is_empty() || p.interiors().iter().any(|r| !r.0.is_empty())
		}
		Geometry::MultiPoint(mp) => !mp.0.is_empty(),
		Geometry::MultiLine(ml) => ml.0.iter().any(|l| !l.0.is_empty()),
		Geometry::MultiPolygon(mp) => mp
			.0
			.iter()
			.any(|p| !p.exterior().0.is_empty() || p.interiors().iter().any(|r| !r.0.is_empty())),
		Geometry::Collection(col) => col.iter().any(geometry_has_any_coordinate),
	}
}

#[cfg(test)]
mod tests {
	use geo::{LineString, Polygon};

	use super::*;

	#[test]
	fn test_cell_id_hilbert_roundtrip_and_hierarchy() {
		let pts = [
			(0.0, 0.0),
			(-0.118092, 51.509865),
			(139.6917, 35.6895),
			(-122.4194, 37.7749),
			(-180.0, -90.0),
			(180.0, 90.0),
			(179.9999, -89.9999),
		];

		for (lon, lat) in pts {
			let leaf = SpatialCellId::from_point(lon, lat);
			assert!(leaf.0 > 0);
			assert_eq!(leaf.level(), MAX_LEVEL);
			assert_eq!(leaf.range_min(), leaf);
			assert_eq!(leaf.range_max(), leaf);

			let (ix, iy, lvl) = leaf.to_ij_level();
			assert_eq!(lvl, MAX_LEVEL);
			assert_eq!(SpatialCellId::from_ij_level(ix, iy, MAX_LEVEL), leaf);

			for parent_lvl in 0..=MAX_LEVEL {
				let anc = leaf.parent(parent_lvl);
				assert_eq!(anc.level(), parent_lvl);
				// Leaf and ancestor itself must both lie inside [anc.range_min(), anc.range_max()]
				assert!(leaf >= anc.range_min() && leaf <= anc.range_max());
				assert!(anc >= anc.range_min() && anc <= anc.range_max());

				// Strict ancestors of `anc` must NEVER lie inside `[anc.range_min(),
				// anc.range_max()]`
				for strict_anc_lvl in 0..parent_lvl {
					let strict_anc = anc.parent(strict_anc_lvl);
					assert!(
						strict_anc < anc.range_min() || strict_anc > anc.range_max(),
						"strict ancestor at level {strict_anc_lvl} fell inside level {parent_lvl} range"
					);
				}
			}
		}
	}

	#[test]
	fn test_polygon_covering_contains_all_interior_points() {
		let poly = Polygon::new(
			LineString::from(vec![
				(-0.5, 51.2),
				(0.3, 51.2),
				(0.3, 51.7),
				(-0.5, 51.7),
				(-0.5, 51.2),
			]),
			vec![],
		);
		let geom = Geometry::Polygon(poly);
		let idx_cells = cover_geometry_for_index(&geom, &CoverConfig::default_index());
		assert!(!idx_cells.is_empty());
		assert!(idx_cells.len() <= 12);

		let scan_ops = plan_geometry_scans(&geom, &CoverConfig::default_query(), true);
		assert!(!scan_ops.is_empty());

		// Sample points inside and on the boundary of the polygon
		for i in 0..=10 {
			for j in 0..=10 {
				let lon = -0.5 + 0.8 * (i as f64 / 10.0);
				let lat = 51.2 + 0.5 * (j as f64 / 10.0);
				let pt = Point::new(lon, lat);
				let leaf = SpatialCellId::from_point(lon, lat);

				// 1. Point leaf must match `plan_geometry_scans(poly)`
				let matched_by_poly_query = scan_ops.iter().any(|op| match op {
					SpatialScanOp::Exact(id) => *id == leaf.0,
					SpatialScanOp::Range {
						min,
						max,
					} => leaf.0 >= *min && leaf.0 <= *max,
				});
				assert!(matched_by_poly_query, "point ({lon}, {lat}) missed by polygon scan");

				// 2. Indexed polygon cells must match `plan_geometry_scans(Point)`
				let pt_ops =
					plan_geometry_scans(&Geometry::Point(pt), &CoverConfig::default_query(), true);
				let poly_matched_by_pt_query = idx_cells.iter().any(|c| {
					pt_ops.iter().any(|op| match op {
						SpatialScanOp::Exact(id) => *id == c.0,
						SpatialScanOp::Range {
							min,
							max,
						} => c.0 >= *min && c.0 <= *max,
					})
				});
				assert!(
					poly_matched_by_pt_query,
					"indexed polygon missed by point query at ({lon}, {lat})"
				);
			}
		}
	}

	#[test]
	fn test_cap_covering_haversine_distance() {
		let center = Point::new(-0.1276, 51.5072); // London
		let radius_m = 5_000.0; // 5 km
		let ops = plan_cap_scans(center, radius_m, &CoverConfig::default_query(), true);
		assert!(!ops.is_empty());

		// Points inside 5km radius of London
		let inside_pts = [
			Point::new(-0.1276, 51.5072),
			Point::new(-0.1419, 51.5014), // Buckingham Palace (~1.2km)
			Point::new(-0.0759, 51.5055), // Tower Bridge (~3.6km)
		];
		for pt in inside_pts {
			assert!(Haversine.distance(center, pt) <= radius_m);
			let leaf = SpatialCellId::from_point(pt.x(), pt.y());
			let matched = ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == leaf.0,
				SpatialScanOp::Range {
					min,
					max,
				} => leaf.0 >= *min && leaf.0 <= *max,
			});
			assert!(matched, "point {pt:?} within 5km was missed by cap scan");
		}
	}

	#[test]
	fn test_polar_and_antimeridian_cap_covering() {
		// 1. High-latitude cap where great-circle closest approach on a meridian curves poleward
		let center_high_lat = Point::new(0.0, 60.0);
		let target_on_meridian = Point::new(45.0, 67.79); // near atan(tan(60°)/cos(45°))
		let dist_m = Haversine.distance(center_high_lat, target_on_meridian);
		let ops =
			plan_cap_scans(center_high_lat, dist_m + 1_000.0, &CoverConfig::default_query(), true);
		let leaf = SpatialCellId::from_point(target_on_meridian.x(), target_on_meridian.y());
		assert!(
			ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == leaf.0,
				SpatialScanOp::Range {
					min,
					max,
				} => leaf.0 >= *min && leaf.0 <= *max,
			}),
			"high-latitude meridian optimum point was missed by cap scan"
		);

		// 2. Trans-polar cap centered at (0.0, 89.0) reaching across the North Pole to (180.0,
		//    89.0)
		let polar_center = Point::new(0.0, 89.0);
		let across_pole = Point::new(180.0, 89.0);
		let polar_dist = Haversine.distance(polar_center, across_pole);
		let polar_ops =
			plan_cap_scans(polar_center, polar_dist + 500.0, &CoverConfig::default_query(), true);
		let across_leaf = SpatialCellId::from_point(across_pole.x(), across_pole.y());
		assert!(
			polar_ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == across_leaf.0,
				SpatialScanOp::Range {
					min,
					max,
				} => across_leaf.0 >= *min && across_leaf.0 <= *max,
			}),
			"trans-polar point across North Pole was missed by cap scan"
		);

		// 3. Antimeridian-crossing cap centered at (179.95, 0.0) reaching (-179.95, 0.0)
		let anti_center = Point::new(179.95, 0.0);
		let anti_target = Point::new(-179.95, 0.0);
		let anti_dist = Haversine.distance(anti_center, anti_target);
		let anti_ops =
			plan_cap_scans(anti_center, anti_dist + 500.0, &CoverConfig::default_query(), true);
		let anti_leaf = SpatialCellId::from_point(anti_target.x(), anti_target.y());
		assert!(
			anti_ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == anti_leaf.0,
				SpatialScanOp::Range {
					min,
					max,
				} => anti_leaf.0 >= *min && anti_leaf.0 <= *max,
			}),
			"antimeridian-crossing point was missed by cap scan"
		);
	}

	#[test]
	fn test_polygon_with_hole_and_complex_geometries() {
		// Polygon with an interior hole: exterior [0..10, 0..10], hole [3..7, 3..7]
		let poly_with_hole = Polygon::new(
			LineString::from(vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]),
			vec![LineString::from(vec![
				(3.0, 3.0),
				(7.0, 3.0),
				(7.0, 7.0),
				(3.0, 7.0),
				(3.0, 3.0),
			])],
		);
		let geom = Geometry::Polygon(poly_with_hole);
		let idx_cells = cover_geometry_for_index(&geom, &CoverConfig::default_index());
		// Point on the ring between exterior and hole (1.5, 1.5) must match
		let pt_on_ring = Geometry::Point(Point::new(1.5, 1.5));
		let pt_ops = plan_geometry_scans(&pt_on_ring, &CoverConfig::default_query(), true);
		assert!(
			idx_cells.iter().any(|c| {
				pt_ops.iter().any(|op| match op {
					SpatialScanOp::Exact(id) => *id == c.0,
					SpatialScanOp::Range {
						min,
						max,
					} => c.0 >= *min && c.0 <= *max,
				})
			}),
			"polygon with hole missed point in exterior ring"
		);

		// LineString and GeometryCollection
		let line = Geometry::Line(LineString::from(vec![(-10.0, -10.0), (10.0, 10.0)]));
		let collection = Geometry::Collection(vec![line, geom]);
		let col_cells = cover_geometry_for_index(&collection, &CoverConfig::default_index());
		assert!(!col_cells.is_empty());
		let origin_ops = plan_geometry_scans(
			&Geometry::Point(Point::new(0.0, 0.0)),
			&CoverConfig::default_query(),
			true,
		);
		assert!(
			col_cells.iter().any(|c| {
				origin_ops.iter().any(|op| match op {
					SpatialScanOp::Exact(id) => *id == c.0,
					SpatialScanOp::Range {
						min,
						max,
					} => c.0 >= *min && c.0 <= *max,
				})
			}),
			"GeometryCollection missed intersection at origin"
		);

		// Point-only scan planning (`include_ancestors = false`) omits all coarse ancestor probes
		let point_only_ops = plan_geometry_scans(
			&Geometry::Point(Point::new(-0.12, 51.5)),
			&CoverConfig::default_query(),
			false,
		);
		assert_eq!(point_only_ops.len(), 1);
		assert!(point_only_ops.iter().all(|op| matches!(op, SpatialScanOp::Range { .. })));
	}

	#[test]
	fn test_max_haversine_distance_antipodal_meridian() {
		// Cell [-67.5, -22.5] x [22.5, 45.0] straddles the antipodal meridian lon = -45.0
		// relative to center = (135.0, -45.0). The antipodal point (-45.0, 45.0) is on the top
		// edge at ~20,015 km, further than all 4 corners (~18,452 km).
		let center = Point::new(135.0, -45.0);
		let rect = Rect::new(
			Coord {
				x: -67.5,
				y: 22.5,
			},
			Coord {
				x: -22.5,
				y: 45.0,
			},
		);
		let ub = max_haversine_distance_to_rect(center, rect);
		let antipodal_edge_pt = Point::new(-45.0, 45.0);
		let true_dist = Haversine.distance(center, antipodal_edge_pt);
		assert!(
			ub >= true_dist,
			"max_haversine_distance_to_rect ({ub}) underestimated true edge distance ({true_dist})"
		);
	}

	#[test]
	fn test_multipoint_covering_matches_points_and_coarsens_gracefully() {
		let cfg = CoverConfig::default_query();
		let p1 = Point::new(-0.12, 51.5);
		let p2 = Point::new(2.35, 48.85);
		let mp = Geometry::MultiPoint(geo::MultiPoint(vec![p1, p2]));
		let mp_cells = cover_geometry(&mp, &cfg, false);
		let mut expected = cover_geometry(&Geometry::Point(p1), &cfg, false);
		expected.extend(cover_geometry(&Geometry::Point(p2), &cfg, false));
		let expected = normalize_covering(expected);
		assert_eq!(mp_cells, expected);
		assert!(mp_cells.iter().all(|c| c.level() == cfg.max_level));

		// 30 points under default_query (max_cells = 24) coarsens only as needed
		let many_pts: Vec<Point<f64>> = (0..30).map(|i| Point::new(i as f64, i as f64)).collect();
		let many_mp = Geometry::MultiPoint(geo::MultiPoint(many_pts));
		let many_cells = cover_geometry(&many_mp, &cfg, false);
		assert!(many_cells.len() <= cfg.max_cells);
		assert!(many_cells.iter().all(|c| c.level() >= cfg.min_level));
	}

	#[test]
	fn test_hilbert_contiguous_range_coalescing() {
		let parent = SpatialCellId::from_ij_level(5, 9, 4);
		let ch = parent.children();
		// Selecting 3 consecutive Hilbert children (ch[0], ch[1], ch[2]) cannot normalize into
		// `parent` (since ch[3] is missing), but their leaf intervals are contiguous along the
		// Hilbert curve and straddle `parent.0` (which sits between ch[1] and ch[2]).
		// Coalescing merges all 3 child ranges AND absorbs `Exact(parent.0)` into a single Range!
		let ops_no_anc = cells_to_scan_ops(&[ch[0], ch[1], ch[2]], false);
		assert_eq!(
			ops_no_anc,
			vec![SpatialScanOp::Range {
				min: ch[0].range_min().0,
				max: ch[2].range_max().0,
			}]
		);

		let ops_with_anc = cells_to_scan_ops(&[ch[0], ch[1], ch[2]], true);
		// `parent.0` is absorbed into the single range `[ch[0].range_min(), ch[2].range_max()]`;
		// only the 4 strict ancestors above `parent` (levels 0..4) remain as `Exact` probes.
		assert_eq!(ops_with_anc.len(), 1 + 4);
		assert!(ops_with_anc.iter().any(|op| match op {
			SpatialScanOp::Range {
				min,
				max,
			} => parent.0 >= *min && parent.0 <= *max,
			SpatialScanOp::Exact(id) => *id == parent.0,
		}));
		assert!(!ops_with_anc.iter().any(|op| match op {
			SpatialScanOp::Range {
				min,
				max,
			} => ch[3].range_min().0 >= *min && ch[3].range_min().0 <= *max,
			SpatialScanOp::Exact(id) => *id == ch[3].range_min().0,
		}));
	}

	#[test]
	fn test_hilbert_curve_continuity() {
		// Verify that at every level 1..=6, sorting all 4^level cells by their Hilbert ID
		// traces a continuous path where every consecutive pair of cells is edge-adjacent
		// (Manhattan distance == 1 in (ix, iy) grid coordinates).
		for level in 1..=6u8 {
			let side = 1u32 << level;
			let mut cells = Vec::with_capacity((side * side) as usize);
			for ix in 0..side {
				for iy in 0..side {
					cells.push(SpatialCellId::from_ij_level(ix, iy, level));
				}
			}
			cells.sort_unstable();
			for pair in cells.windows(2) {
				let (x0, y0, l0) = pair[0].to_ij_level();
				let (x1, y1, l1) = pair[1].to_ij_level();
				assert_eq!(l0, level);
				assert_eq!(l1, level);
				let dist = x0.abs_diff(x1) + y0.abs_diff(y1);
				assert_eq!(
					dist, 1,
					"Hilbert curve discontinuity at level {level}: ({x0}, {y0}) -> ({x1}, {y1})"
				);
			}
		}
	}

	#[test]
	fn test_coarsen_to_budget_preserves_fine_cells_when_scattered() {
		let idx_cfg = CoverConfig::default_index(); // max_cells = 12
		// 13 scattered points across all 4 quadrants of the globe; only 1 adjacent pair needs
		// to merge to its LCA to fit within `max_cells = 12`, leaving 11 points at leaf level 28.
		let pts: Vec<Point<f64>> = (0..13)
			.map(|i| {
				let lon = -165.0 + (i as f64) * 26.0;
				let lat = -72.0 + (i as f64) * 11.5;
				Point::new(lon, lat)
			})
			.collect();
		let mp = Geometry::MultiPoint(geo::MultiPoint::new(pts));
		let cells = cover_geometry_for_index(&mp, &idx_cfg);
		assert_eq!(cells.len(), 12);
		assert!(
			cells.iter().filter(|c| c.level() == MAX_LEVEL).count() >= 11,
			"expected at least 11 exact leaf cells, got {cells:?}"
		);
		assert!(!cells.contains(&SpatialCellId::root()));
	}

	#[test]
	fn test_scan_ops_bounded_by_max_spatial_scan_ops() {
		let high_budget_cfg = CoverConfig {
			min_level: 0,
			max_level: 20,
			max_cells: 80,
		};
		let pts: Vec<Point<f64>> = (0..60)
			.map(|i| Point::new(-170.0 + (i as f64) * 5.5, -80.0 + ((i * 13) % 160) as f64))
			.collect();
		let mp = Geometry::MultiPoint(geo::MultiPoint::new(pts));
		let ops = plan_geometry_scans(&mp, &high_budget_cfg, true);
		assert!(
			ops.len() <= MAX_SPATIAL_SCAN_OPS,
			"expected <= {MAX_SPATIAL_SCAN_OPS} scan ops, got {}",
			ops.len()
		);

		let polar_ops = plan_cap_scans(Point::new(0.0, 89.5), 250_000.0, &high_budget_cfg, true);
		assert!(
			polar_ops.len() <= MAX_SPATIAL_SCAN_OPS,
			"expected <= {MAX_SPATIAL_SCAN_OPS} polar cap scan ops, got {}",
			polar_ops.len()
		);

		// Tighter per-branch budgets (e.g. for N-way OR unions) bound scan_ops to `max_ops`
		// while preserving full superset coverage.
		for max_ops in [1usize, 4, 6, 12] {
			let bounded_geom_ops =
				plan_geometry_scans_with_max_ops(&mp, &high_budget_cfg, true, max_ops);
			assert!(!bounded_geom_ops.is_empty());
			assert!(
				bounded_geom_ops.len() <= max_ops,
				"expected <= {max_ops} geom scan ops, got {}",
				bounded_geom_ops.len()
			);
			let bounded_cap_ops = plan_cap_scans_with_max_ops(
				Point::new(0.0, 89.5),
				250_000.0,
				&high_budget_cfg,
				true,
				max_ops,
			);
			assert!(!bounded_cap_ops.is_empty());
			assert!(
				bounded_cap_ops.len() <= max_ops,
				"expected <= {max_ops} cap scan ops, got {}",
				bounded_cap_ops.len()
			);
		}

		// A tiny box straddling the level-2 quadrant corner `(90°, 45°)` with `include_ancestors
		// = true` lifts cell depth uniformly rather than LCA-merging across the quadrant boundary
		// into a level-1 cell (~25% of the globe).
		let quadrant_box = Geometry::Polygon(Polygon::new(
			LineString::from(vec![
				(89.9999, 44.9999),
				(90.0001, 44.9999),
				(90.0001, 45.0001),
				(89.9999, 45.0001),
				(89.9999, 44.9999),
			]),
			vec![],
		));
		let default_qry_cfg = CoverConfig::default_query();
		for (max_ops, max_area) in [(48usize, 1e-6), (24, 1e-3), (12, 0.02)] {
			let (ops, area) =
				plan_geometry_scans_with_area(&quadrant_box, &default_qry_cfg, true, max_ops);
			assert!(!ops.is_empty() && ops.len() <= max_ops);
			assert!(!ops.iter().copied().any(SpatialScanOp::covers_root));
			assert!(
				area < max_area,
				"quadrant-straddling box at max_ops={max_ops} must lift depth uniformly without collapsing to level-1 (area={area}, max_area={max_area})"
			);
		}
	}

	#[test]
	fn test_randomized_superset_invariant_zero_false_negatives() {
		// Deterministic xorshift64 PRNG for reproducible property-based testing across
		// Points, LineStrings, Polygons, MultiPoints, MultiPolygons, GeometryCollections,
		// and spherical caps (including near-pole and near-antimeridian coordinates).
		struct Rng(u64);
		impl Rng {
			fn next_u64(&mut self) -> u64 {
				self.0 ^= self.0 << 13;
				self.0 ^= self.0 >> 7;
				self.0 ^= self.0 << 17;
				self.0
			}
			fn next_f64(&mut self, min: f64, max: f64) -> f64 {
				let u = (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64);
				min + u * (max - min)
			}
		}

		let matches_ops = |cells: &[SpatialCellId], ops: &[SpatialScanOp]| -> bool {
			cells.iter().any(|c| {
				ops.iter().any(|op| match op {
					SpatialScanOp::Exact(id) => *id == c.0,
					SpatialScanOp::Range {
						min,
						max,
					} => c.0 >= *min && c.0 <= *max,
				})
			})
		};

		let idx_cfg = CoverConfig::default_index();
		let qry_cfg = CoverConfig::default_query();
		let mut rng = Rng(0x5EED_CAFE_1234_5678);

		for trial in 0..200 {
			// Generate a base anchor point, including boundary cases on early trials
			let (base_lon, base_lat) = match trial {
				0 => (179.999, 0.0),
				1 => (-179.999, 0.0),
				2 => (0.0, 89.999),
				3 => (0.0, -89.999),
				4 => (0.0, 0.0),
				_ => (rng.next_f64(-179.5, 179.5), rng.next_f64(-89.5, 89.5)),
			};
			let span = rng.next_f64(0.001, 2.0);
			let lon0 = (base_lon - span).clamp(-180.0, 180.0);
			let lon1 = (base_lon + span).clamp(-180.0, 180.0);
			let lat0 = (base_lat - span).clamp(-90.0, 90.0);
			let lat1 = (base_lat + span).clamp(-90.0, 90.0);

			let raw_poly = Polygon::new(
				LineString::from(vec![
					(lon0, lat0),
					(lon1, lat0),
					(lon1, lat1),
					(lon0, lat1),
					(lon0, lat0),
				]),
				vec![],
			);
			let query_poly = Geometry::Polygon(raw_poly.clone());
			let poly_ops_all = plan_geometry_scans(&query_poly, &qry_cfg, true);
			let poly_ops_pts = plan_geometry_scans(&query_poly, &qry_cfg, false);

			// 1. A point inside the query box must be matched by both general and point-only plans
			let inside_pt = Point::new(rng.next_f64(lon0, lon1), rng.next_f64(lat0, lat1));
			let pt_geom = Geometry::Point(inside_pt);
			if raw_poly.intersects(&inside_pt) {
				let pt_cells = cover_geometry_for_index(&pt_geom, &idx_cfg);
				assert!(
					matches_ops(&pt_cells, &poly_ops_all),
					"trial {trial}: general polygon scan missed intersecting point {inside_pt:?}"
				);
				assert!(
					matches_ops(&pt_cells, &poly_ops_pts),
					"trial {trial}: point-only polygon scan missed intersecting point {inside_pt:?}"
				);
			}

			// 2. A LineString crossing the query box must be matched by the general plan
			let raw_ls = LineString::from(vec![
				(inside_pt.x(), inside_pt.y()),
				(
					(inside_pt.x() + span * 1.5).clamp(-180.0, 180.0),
					(inside_pt.y() + span * 1.5).clamp(-90.0, 90.0),
				),
			]);
			let ls_geom = Geometry::Line(raw_ls.clone());
			if raw_poly.intersects(&raw_ls) {
				let ls_cells = cover_geometry_for_index(&ls_geom, &idx_cfg);
				assert!(
					matches_ops(&ls_cells, &poly_ops_all),
					"trial {trial}: polygon scan missed intersecting linestring"
				);
			}

			// 3. An overlapping Polygon must be matched by the general plan
			let raw_sub_poly = Polygon::new(
				LineString::from(vec![
					(inside_pt.x(), inside_pt.y()),
					((inside_pt.x() + span).clamp(-180.0, 180.0), inside_pt.y()),
					(
						(inside_pt.x() + span).clamp(-180.0, 180.0),
						(inside_pt.y() + span).clamp(-90.0, 90.0),
					),
					(inside_pt.x(), (inside_pt.y() + span).clamp(-90.0, 90.0)),
					(inside_pt.x(), inside_pt.y()),
				]),
				vec![],
			);
			let sub_poly = Geometry::Polygon(raw_sub_poly.clone());
			if raw_poly.intersects(&raw_sub_poly) {
				let sub_cells = cover_geometry_for_index(&sub_poly, &idx_cfg);
				assert!(
					matches_ops(&sub_cells, &poly_ops_all),
					"trial {trial}: polygon scan missed intersecting polygon"
				);
			}

			// 4. Spherical cap (Haversine radius) superset invariant
			let center = Point::new(base_lon, base_lat);
			let target_dist = Haversine.distance(center, inside_pt);
			let radius = target_dist + 1.0;
			let cap_ops_all = plan_cap_scans(center, radius, &qry_cfg, true);
			let cap_ops_pts = plan_cap_scans(center, radius, &qry_cfg, false);
			let pt_cells = cover_geometry_for_index(&pt_geom, &idx_cfg);
			assert!(
				matches_ops(&pt_cells, &cap_ops_all),
				"trial {trial}: general radius scan missed point within radius ({target_dist} <= {radius})"
			);
			assert!(
				matches_ops(&pt_cells, &cap_ops_pts),
				"trial {trial}: point-only radius scan missed point within radius ({target_dist} <= {radius})"
			);
		}
	}

	#[test]
	fn test_non_finite_and_out_of_domain_geometries() {
		let idx_cfg = CoverConfig::default_index();
		let qry_cfg = CoverConfig::default_query();

		// 1. Non-finite points emit no index entries and no query scan ops
		for bad_pt in [
			Point::new(f64::NAN, 0.0),
			Point::new(0.0, f64::NAN),
			Point::new(f64::INFINITY, 10.0),
			Point::new(-10.0, f64::NEG_INFINITY),
		] {
			let geom = Geometry::Point(bad_pt);
			assert!(cover_geometry_for_index(&geom, &idx_cfg).is_empty());
			assert!(plan_geometry_scans(&geom, &qry_cfg, true).is_empty());
		}

		// 2. Non-Point geometries containing any NaN or ±inf coordinate emit `[root()]` on the
		//    write path (so any finite query with ancestor probes visits them for residual filter
		//    evaluation) and decline index acceleration (`Vec::new()`) on the query path.
		let valid_pt = Point::new(10.0, 20.0);
		let mixed_mp =
			Geometry::MultiPoint(geo::MultiPoint::new(vec![valid_pt, Point::new(f64::NAN, 0.0)]));
		let inf_line = Geometry::Line(LineString::from(vec![(0.0, 0.0), (f64::INFINITY, 0.0)]));
		let inf_poly = Geometry::Polygon(Polygon::new(
			LineString::from(vec![
				(0.0, 0.0),
				(f64::INFINITY, 0.0),
				(10.0, 10.0),
				(0.0, 10.0),
				(0.0, 0.0),
			]),
			vec![],
		));
		let nan_poly = Geometry::Polygon(Polygon::new(
			LineString::from(vec![
				(0.0, 0.0),
				(10.0, 0.0),
				(f64::NAN, 10.0),
				(0.0, 10.0),
				(0.0, 0.0),
			]),
			vec![],
		));
		let nan_mp = Geometry::MultiPoint(geo::MultiPoint::new(vec![Point::new(f64::NAN, 0.0)]));
		for non_finite_geom in [&mixed_mp, &inf_line, &inf_poly, &nan_poly, &nan_mp] {
			assert_eq!(
				cover_geometry_for_index(non_finite_geom, &idx_cfg),
				vec![SpatialCellId::root()],
				"expected root cell on write path for {non_finite_geom:?}"
			);
			assert!(
				plan_geometry_scans(non_finite_geom, &qry_cfg, true).is_empty(),
				"expected empty scan ops on query path for {non_finite_geom:?}"
			);
			// Any finite query with ancestor probes (`include_ancestors = true`) includes `root()`,
			// matching the stored `[SpatialCellId::root()]` cell even at distant coordinates.
			let probe_ops =
				plan_geometry_scans(&Geometry::Point(Point::new(-100.0, 0.0)), &qry_cfg, true);
			assert!(probe_ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == SpatialCellId::root().0,
				SpatialScanOp::Range {
					min,
					max,
				} => SpatialCellId::root().0 >= *min && SpatialCellId::root().0 <= *max,
			}));
		}

		// 3. Out-of-domain finite coordinates emit both the clamped cell (for planar Geo
		//    predicates) and the spherical-normalized cell (for Haversine geo::distance predicates)
		let out_pt = Geometry::Point(Point::new(200.0, 45.0));
		let clamped_pt = Geometry::Point(Point::new(180.0, 45.0));
		let spherical_pt = Geometry::Point(Point::new(-160.0, 45.0));
		let out_cells = cover_geometry_for_index(&out_pt, &idx_cfg);
		let out_ops = plan_geometry_scans(&out_pt, &qry_cfg, true);
		let clamped_cell = SpatialCellId::from_point(180.0, 45.0);
		let spherical_cell = SpatialCellId::from_point(-160.0, 45.0);
		assert!(out_cells.contains(&clamped_cell));
		assert!(out_cells.contains(&spherical_cell));
		assert_eq!(out_ops, plan_geometry_scans(&clamped_pt, &qry_cfg, true));

		// Spherical cap on (-160.0, 45.0) covers the indexed cells of (200.0, 45.0), and vice versa
		let cap_ops_canonical = plan_cap_scans(Point::new(-160.0, 45.0), 100.0, &qry_cfg, true);
		assert!(out_cells.iter().any(|c| cap_ops_canonical.iter().any(|op| match op {
			SpatialScanOp::Exact(id) => *id == c.0,
			SpatialScanOp::Range {
				min,
				max,
			} => c.0 >= *min && c.0 <= *max,
		})));
		let cap_ops_out = plan_cap_scans(Point::new(200.0, 45.0), 100.0, &qry_cfg, true);
		let canonical_cells = cover_geometry_for_index(&spherical_pt, &idx_cfg);
		assert!(canonical_cells.iter().any(|c| cap_ops_out.iter().any(|op| match op {
			SpatialScanOp::Exact(id) => *id == c.0,
			SpatialScanOp::Range {
				min,
				max,
			} => c.0 >= *min && c.0 <= *max,
		})));

		// Full-globe radius short-circuits directly to the root cell
		assert_eq!(
			cover_cap(Point::new(0.0, 0.0), 25_000_000.0, &qry_cfg),
			vec![SpatialCellId::root()]
		);

		// 4. Polygon with out-of-domain finite vertices still covers interior points via bbox
		//    fallback
		let out_poly = Geometry::Polygon(Polygon::new(
			LineString::from(vec![
				(170.0, 10.0),
				(190.0, 10.0),
				(190.0, 20.0),
				(170.0, 20.0),
				(170.0, 10.0),
			]),
			vec![],
		));
		let poly_ops = plan_geometry_scans(&out_poly, &qry_cfg, true);
		let interior_leaf = SpatialCellId::from_point(175.0, 15.0);
		assert!(poly_ops.iter().any(|op| match op {
			SpatialScanOp::Exact(id) => *id == interior_leaf.0,
			SpatialScanOp::Range {
				min,
				max,
			} => interior_leaf.0 >= *min && interior_leaf.0 <= *max,
		}));

		// 4b. High-vertex Polygon (2,500 vertices) uses chunked segment culling cleanly without
		//     falling back to a global bounding box, and extreme-vertex Line / MultiLine /
		//     Collection geometries (> MAX_EXACT_GEOMETRY_VERTICES) cover every vertex tightly
		//     without collapsing to a single global bounding box.
		let op_covers_leaf = |ops: &[SpatialScanOp], leaf: SpatialCellId| -> bool {
			ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == leaf.0,
				SpatialScanOp::Range {
					min,
					max,
				} => leaf.0 >= *min && leaf.0 <= *max,
			})
		};

		let mut dense_ring: Vec<(f64, f64)> = Vec::with_capacity(2_505);
		for i in 0..=2_500 {
			let t = (i as f64) / 2_500.0;
			dense_ring.push((t * 2.0, t * 0.2));
		}
		dense_ring.push((2.0, 0.3));
		dense_ring.push((0.0, 0.1));
		dense_ring.push((0.0, 0.0));
		let dense_poly = Geometry::Polygon(Polygon::new(LineString::from(dense_ring), vec![]));
		let dense_ops = plan_geometry_scans(&dense_poly, &qry_cfg, false);
		let far_corner = SpatialCellId::from_point(0.0, 0.29);
		assert!(
			!op_covers_leaf(&dense_ops, far_corner),
			"2,500-vertex diagonal polygon should not collapse to its global bounding box"
		);

		// MultiPoint vertices do not count toward MAX_EXACT_GEOMETRY_VERTICES.
		let big_mp = Geometry::MultiPoint(geo::MultiPoint::new(
			(0..20_000).map(|i| Point::new((i as f64) * 0.001, 5.0)).collect(),
		));
		let mp_and_poly = Geometry::Collection(vec![big_mp, dense_poly]);
		assert!(!geometry_exceeds_exact_vertex_limit(&mp_and_poly));

		let huge_line_coords: Vec<(f64, f64)> = (0..20_000)
			.map(|i| {
				let t = (i as f64) / 20_000.0;
				(10.0 + t, 20.0 + t)
			})
			.collect();
		let huge_line = Geometry::Line(LineString::from(huge_line_coords.clone()));
		assert!(geometry_exceeds_exact_vertex_limit(&huge_line));
		let huge_line_ops = plan_geometry_scans(&huge_line, &qry_cfg, true);
		for &(x, y) in &huge_line_coords {
			assert!(
				op_covers_leaf(&huge_line_ops, SpatialCellId::from_point(x, y)),
				"huge_line covering missed vertex ({x}, {y})"
			);
		}
		// Off-diagonal corner of huge_line's global bounding box must NOT be covered because
		// huge_line is covered by 64-segment chunk rects rather than one global bbox.
		assert!(
			!op_covers_leaf(&huge_line_ops, SpatialCellId::from_point(10.0, 20.99)),
			"huge_line should use per-chunk rects rather than a single global bounding box"
		);

		let huge_ml_lines: Vec<LineString<f64>> = (0..40)
			.map(|j| {
				LineString::from(
					(0..500).map(|i| (j as f64 * 0.1 + i as f64 * 0.001, 10.0)).collect::<Vec<_>>(),
				)
			})
			.collect();
		let huge_ml = Geometry::MultiLine(geo::MultiLineString::new(huge_ml_lines.clone()));
		assert!(geometry_exceeds_exact_vertex_limit(&huge_ml));
		let huge_ml_ops = plan_geometry_scans(&huge_ml, &qry_cfg, true);
		for l in &huge_ml_lines {
			for c in &l.0 {
				assert!(
					op_covers_leaf(&huge_ml_ops, SpatialCellId::from_point(c.x, c.y)),
					"huge_ml covering missed vertex ({}, {})",
					c.x,
					c.y
				);
			}
		}

		let huge_col = Geometry::Collection(vec![huge_line, huge_ml]);
		assert!(geometry_exceeds_exact_vertex_limit(&huge_col));
		let huge_col_ops = plan_geometry_scans(&huge_col, &qry_cfg, true);
		for &(x, y) in &[huge_line_coords[0], huge_line_coords[10_000], huge_line_coords[19_999]] {
			assert!(op_covers_leaf(&huge_col_ops, SpatialCellId::from_point(x, y)));
		}
		for l in &huge_ml_lines {
			let c = l.0[0];
			assert!(op_covers_leaf(&huge_col_ops, SpatialCellId::from_point(c.x, c.y)));
		}

		// Collection of [London Point, Sydney 16,400-vertex LineString] keeps London and Sydney
		// cells localized instead of collapsing to a continent-spanning global bounding box.
		let london_pt = Point::new(-0.1278, 51.5074);
		let sydney_coords: Vec<(f64, f64)> = (0..16_400)
			.map(|i| {
				let t = (i as f64) / 16_400.0;
				(151.20 + t * 0.05, -33.86 + t * 0.05)
			})
			.collect();
		let london_sydney_col = Geometry::Collection(vec![
			Geometry::Point(london_pt),
			Geometry::Line(LineString::from(sydney_coords.clone())),
		]);
		assert!(geometry_exceeds_exact_vertex_limit(&london_sydney_col));
		let ls_ops = plan_geometry_scans(&london_sydney_col, &qry_cfg, false);
		assert!(op_covers_leaf(&ls_ops, SpatialCellId::from_point(london_pt.x(), london_pt.y())));
		for &(x, y) in &[sydney_coords[0], sydney_coords[8_200], sydney_coords[16_399]] {
			assert!(op_covers_leaf(&ls_ops, SpatialCellId::from_point(x, y)));
		}
		// Cairo (31.23, 30.04) sits inside the global bbox [-0.12..151.25, -33.86..51.50]
		// halfway between London and Sydney, and must NOT be covered.
		assert!(
			!op_covers_leaf(&ls_ops, SpatialCellId::from_point(31.23, 30.04)),
			"London + Sydney collection must not collapse to a global bounding box"
		);

		// Collection of 4,096 small squares (20,480 vertices) and Collection of 8,192 2-vertex
		// lines (16,384 vertices) cover every member cleanly in a single pass.
		let squares_col = Geometry::Collection(
			(0..4_096)
				.map(|i| {
					let bx = (i % 64) as f64 * 0.01;
					let by = (i / 64) as f64 * 0.01;
					Geometry::Polygon(Polygon::new(
						LineString::from(vec![
							(bx, by),
							(bx + 0.005, by),
							(bx + 0.005, by + 0.005),
							(bx, by + 0.005),
							(bx, by),
						]),
						vec![],
					))
				})
				.collect(),
		);
		let squares_ops = plan_geometry_scans(&squares_col, &qry_cfg, false);
		assert!(op_covers_leaf(&squares_ops, SpatialCellId::from_point(0.002, 0.002)));
		assert!(op_covers_leaf(&squares_ops, SpatialCellId::from_point(0.632, 0.632)));

		let lines_col = Geometry::Collection(
			(0..8_192)
				.map(|i| {
					let bx = (i % 128) as f64 * 0.01;
					let by = (i / 128) as f64 * 0.01;
					Geometry::Line(LineString::from(vec![(bx, by), (bx + 0.005, by + 0.005)]))
				})
				.collect(),
		);
		assert!(!geometry_exceeds_exact_vertex_limit(&lines_col));
		let lines_ops = plan_geometry_scans(&lines_col, &qry_cfg, false);
		assert!(op_covers_leaf(&lines_ops, SpatialCellId::from_point(0.002, 0.002)));
		assert!(op_covers_leaf(&lines_ops, SpatialCellId::from_point(1.272, 0.632)));

		// 5. Degenerate 1-vertex LineString (both in-domain and out-of-domain) covers its vertex
		for (lon, expected_lon) in [(12.34, 12.34), (210.0, 180.0)] {
			let degen_line = Geometry::Line(LineString::from(vec![(lon, 45.0)]));
			let ops = plan_geometry_scans(&degen_line, &qry_cfg, true);
			let leaf = SpatialCellId::from_point(expected_lon, 45.0);
			assert!(ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == leaf.0,
				SpatialScanOp::Range {
					min,
					max,
				} => leaf.0 >= *min && leaf.0 <= *max,
			}));
		}

		// 6. Empty geometries emit the root cell on the write path and nothing on the query path
		for empty_geom in [
			Geometry::Collection(vec![]),
			Geometry::MultiPoint(geo::MultiPoint::new(vec![])),
			Geometry::MultiLine(geo::MultiLineString::new(vec![])),
			Geometry::MultiPolygon(geo::MultiPolygon::new(vec![])),
		] {
			assert_eq!(
				cover_geometry_for_index(&empty_geom, &idx_cfg),
				vec![SpatialCellId::root()]
			);
			assert!(plan_geometry_scans(&empty_geom, &qry_cfg, true).is_empty());
		}

		// 7. Large GeometryCollection with 30 items coarsens to <= config.max_cells while still
		//    covering every member; on the write path, points inside a Collection use exact
		//    MAX_LEVEL leaf cells (plus spherical-normalized leaf cell when out-of-domain).
		let single_pt_col = Geometry::Collection(vec![Geometry::Point(Point::new(2.35, 48.85))]);
		assert_eq!(
			cover_geometry_for_index(&single_pt_col, &idx_cfg),
			vec![SpatialCellId::from_point(2.35, 48.85)],
			"Collection([Point]) on write path should emit 1 exact leaf cell at MAX_LEVEL"
		);
		let small_poly = Geometry::Polygon(Polygon::new(
			LineString::from(vec![
				(0.0, 0.0),
				(0.0001, 0.0),
				(0.0001, 0.0001),
				(0.0, 0.0001),
				(0.0, 0.0),
			]),
			vec![],
		));
		let poly_and_pt_col = Geometry::Collection(vec![
			small_poly,
			Geometry::Point(Point::new(2.35, 48.85)),
			Geometry::Point(Point::new(200.0, 45.0)),
		]);
		let poly_and_pt_cells = cover_geometry_for_index(&poly_and_pt_col, &idx_cfg);
		assert!(poly_and_pt_cells.contains(&SpatialCellId::from_point(2.35, 48.85)));
		assert!(poly_and_pt_cells.contains(&SpatialCellId::from_point(180.0, 45.0)));
		assert!(poly_and_pt_cells.contains(&SpatialCellId::from_point(-160.0, 45.0)));

		// 7b. Polygon whose interior ring lies outside its exterior ring's bounding box covers
		//     both rings on the write and query paths so `geo::Intersects` matches segments
		//     touching the stray interior ring.
		let stray_hole_poly = Geometry::Polygon(Polygon::new(
			LineString::from(vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
			vec![LineString::from(vec![
				(10.0, 10.0),
				(11.0, 10.0),
				(11.0, 11.0),
				(10.0, 11.0),
				(10.0, 10.0),
			])],
		));
		let stray_line = Geometry::Line(LineString::from(vec![(9.5, 10.5), (10.5, 10.5)]));
		let stray_poly_idx = cover_geometry_for_index(&stray_hole_poly, &idx_cfg);
		let stray_line_ops = plan_geometry_scans(&stray_line, &qry_cfg, true);
		assert!(
			stray_poly_idx.iter().any(|c| stray_line_ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == c.0,
				SpatialScanOp::Range {
					min,
					max,
				} => c.0 >= *min && c.0 <= *max,
			})),
			"indexed stray-hole polygon missed query line touching interior ring"
		);
		let stray_line_idx = cover_geometry_for_index(&stray_line, &idx_cfg);
		let stray_poly_ops = plan_geometry_scans(&stray_hole_poly, &qry_cfg, true);
		assert!(
			stray_line_idx.iter().any(|c| stray_poly_ops.iter().any(|op| match op {
				SpatialScanOp::Exact(id) => *id == c.0,
				SpatialScanOp::Range {
					min,
					max,
				} => c.0 >= *min && c.0 <= *max,
			})),
			"query stray-hole polygon missed indexed line touching interior ring"
		);

		let items: Vec<Geometry> = (0..30)
			.map(|i| {
				let lon = -140.0 + (i as f64) * 9.0;
				let lat = -60.0 + (i as f64) * 4.0;
				Geometry::Point(Point::new(lon, lat))
			})
			.collect();
		let col = Geometry::Collection(items.clone());
		let col_cells = cover_geometry(&col, &qry_cfg, false);
		assert!(
			col_cells.len() <= qry_cfg.max_cells,
			"expected <= {} cells, got {}",
			qry_cfg.max_cells,
			col_cells.len()
		);
		let col_ops = cells_to_scan_ops(&col_cells, true);
		for item in &items {
			let item_cells = cover_geometry_for_index(item, &idx_cfg);
			assert!(
				item_cells.iter().any(|c| col_ops.iter().any(|op| match op {
					SpatialScanOp::Exact(id) => *id == c.0,
					SpatialScanOp::Range {
						min,
						max,
					} => c.0 >= *min && c.0 <= *max,
				})),
				"collection covering missed member {item:?}"
			);
		}

		// 8. Large MultiPoint write covering is bounded by config.max_cells while still matching
		//    both planar and spherical query scans for every constituent point
		let mut mp_pts: Vec<Point<f64>> = (0..300)
			.map(|i| {
				let lon = -170.0 + (i as f64) * 1.1;
				let lat = -80.0 + ((i * 7) % 160) as f64;
				Point::new(lon, lat)
			})
			.collect();
		mp_pts.push(Point::new(200.0, 10.0));
		let mp_geom = Geometry::MultiPoint(geo::MultiPoint::new(mp_pts.clone()));
		let mp_idx_cells = cover_geometry_for_index(&mp_geom, &idx_cfg);
		assert!(
			mp_idx_cells.len() <= idx_cfg.max_cells,
			"expected MultiPoint index cells <= {}, got {}",
			idx_cfg.max_cells,
			mp_idx_cells.len()
		);
		for pt in &mp_pts {
			let pt_geom = Geometry::Point(*pt);
			let planar_ops = plan_geometry_scans(&pt_geom, &qry_cfg, true);
			assert!(
				mp_idx_cells.iter().any(|c| planar_ops.iter().any(|op| match op {
					SpatialScanOp::Exact(id) => *id == c.0,
					SpatialScanOp::Range {
						min,
						max,
					} => c.0 >= *min && c.0 <= *max,
				})),
				"coarsened MultiPoint missed planar query for {pt:?}"
			);
			let cap_ops = plan_cap_scans(*pt, 100.0, &qry_cfg, true);
			assert!(
				mp_idx_cells.iter().any(|c| cap_ops.iter().any(|op| match op {
					SpatialScanOp::Exact(id) => *id == c.0,
					SpatialScanOp::Range {
						min,
						max,
					} => c.0 >= *min && c.0 <= *max,
				})),
				"coarsened MultiPoint missed cap query for {pt:?}"
			);
		}
	}

	#[test]
	fn test_nonconvex_self_intersecting_extreme_caps_and_key_order() {
		use crate::val::{IndexFormat, Number, Value};

		let idx_cfg = CoverConfig::default_index();
		let qry_cfg = CoverConfig::default_query();
		let matches_ops = |cells: &[SpatialCellId], ops: &[SpatialScanOp]| -> bool {
			cells.iter().any(|c| {
				ops.iter().any(|op| match op {
					SpatialScanOp::Exact(id) => *id == c.0,
					SpatialScanOp::Range {
						min,
						max,
					} => c.0 >= *min && c.0 <= *max,
				})
			})
		};

		// 1. Non-convex L-shaped polygon and polygon with interior hole
		let l_poly = Geometry::Polygon(Polygon::new(
			LineString::from(vec![
				(0.0, 0.0),
				(2.0, 0.0),
				(2.0, 1.0),
				(1.0, 1.0),
				(1.0, 2.0),
				(0.0, 2.0),
				(0.0, 0.0),
			]),
			vec![],
		));
		let l_ops = plan_geometry_scans(&l_poly, &qry_cfg, false);
		for pt in [Point::new(0.5, 0.5), Point::new(1.5, 0.5), Point::new(0.5, 1.5)] {
			let pt_cells = cover_geometry_for_index(&Geometry::Point(pt), &idx_cfg);
			assert!(matches_ops(&pt_cells, &l_ops), "L-polygon missed {pt:?}");
		}

		// 2. Self-intersecting bowtie polygon
		let bowtie = Geometry::Polygon(Polygon::new(
			LineString::from(vec![
				(10.0, 10.0),
				(12.0, 12.0),
				(12.0, 10.0),
				(10.0, 12.0),
				(10.0, 10.0),
			]),
			vec![],
		));
		let bowtie_ops = plan_geometry_scans(&bowtie, &qry_cfg, false);
		for pt in [Point::new(11.0, 11.0), Point::new(10.2, 11.0), Point::new(11.8, 11.0)] {
			let pt_cells = cover_geometry_for_index(&Geometry::Point(pt), &idx_cfg);
			assert!(matches_ops(&pt_cells, &bowtie_ops), "bowtie missed {pt:?}");
		}

		// 3. MultiPolygon and MultiLine queries near antimeridian
		let mp = Geometry::MultiPolygon(geo::MultiPolygon::new(vec![
			Polygon::new(
				LineString::from(vec![
					(179.0, -1.0),
					(180.0, -1.0),
					(180.0, 1.0),
					(179.0, 1.0),
					(179.0, -1.0),
				]),
				vec![],
			),
			Polygon::new(
				LineString::from(vec![
					(-180.0, -1.0),
					(-179.0, -1.0),
					(-179.0, 1.0),
					(-180.0, 1.0),
					(-180.0, -1.0),
				]),
				vec![],
			),
		]));
		let mp_ops = plan_geometry_scans(&mp, &qry_cfg, false);
		for pt in [Point::new(179.5, 0.0), Point::new(-179.5, 0.0)] {
			let pt_cells = cover_geometry_for_index(&Geometry::Point(pt), &idx_cfg);
			assert!(matches_ops(&pt_cells, &mp_ops), "antimeridian MultiPolygon missed {pt:?}");
		}

		let ml = Geometry::MultiLine(geo::MultiLineString::new(vec![
			LineString::from(vec![(179.0, 0.0), (180.0, 0.0)]),
			LineString::from(vec![(-180.0, 0.0), (-179.0, 0.0)]),
		]));
		let ml_ops = plan_geometry_scans(&ml, &qry_cfg, true);
		for pt in [Point::new(179.5, 0.0), Point::new(-179.5, 0.0)] {
			let pt_cells = cover_geometry_for_index(&Geometry::Point(pt), &idx_cfg);
			assert!(matches_ops(&pt_cells, &ml_ops), "antimeridian MultiLine missed {pt:?}");
		}

		// 4. Extreme spherical caps: poles (lat = ±90), antimeridian (lon = 180), and > half-globe
		for (center, radius, probe) in [
			(Point::new(0.0, 90.0), 50_000.0, Point::new(135.0, 89.8)),
			(Point::new(0.0, -90.0), 50_000.0, Point::new(-45.0, -89.8)),
			(Point::new(180.0, 0.0), 50_000.0, Point::new(-179.8, 0.0)),
			(Point::new(0.0, 0.0), 15_000_000.0, Point::new(120.0, 30.0)),
		] {
			let ops = plan_cap_scans(center, radius, &qry_cfg, false);
			assert!(ops.len() <= MAX_SPATIAL_SCAN_OPS);
			let pt_cells = cover_geometry_for_index(&Geometry::Point(probe), &idx_cfg);
			assert!(
				matches_ops(&pt_cells, &ops),
				"cap({center:?}, {radius}) missed probe {probe:?}"
			);
		}

		// 5. Key encoding order: cell-id ordering is preserved through storekey `IndexFormat`
		let mut sample_cells = vec![
			SpatialCellId::root(),
			SpatialCellId::from_ij_level(0, 0, 1),
			SpatialCellId::from_ij_level(1, 1, 1),
			SpatialCellId::from_point(-180.0, -90.0),
			SpatialCellId::from_point(0.0, 0.0),
			SpatialCellId::from_point(180.0, 90.0),
		];
		for c in sample_cells.clone() {
			sample_cells.push(c.range_min());
			sample_cells.push(c.range_max());
		}
		sample_cells.sort();
		sample_cells.dedup();
		for pair in sample_cells.windows(2) {
			assert!(pair[0].0 < pair[1].0);
			let k0 = storekey::encode_vec_format::<IndexFormat, _>(&Value::Number(Number::Int(
				pair[0].0,
			)))
			.unwrap();
			let k1 = storekey::encode_vec_format::<IndexFormat, _>(&Value::Number(Number::Int(
				pair[1].0,
			)))
			.unwrap();
			assert!(
				k0 < k1,
				"key encoding reversed order for {:?} ({k0:?}) vs {:?} ({k1:?})",
				pair[0],
				pair[1]
			);
		}
	}

	#[test]
	fn test_frozen_covering_golden_format() {
		// Guards the persisted spatial index format. Any change to the Hilbert state tables,
		// `CoverConfig::default_index()`, `BOUNDARY_EPS`, or `coarsen_to_budget` that alters
		// these vectors requires bumping `SPATIAL_COVERING_FORMAT_VERSION` in `surrealdb-catalog`.
		assert_eq!(crate::catalog::SPATIAL_COVERING_FORMAT_VERSION, 5);
		let cfg = CoverConfig::default_index();

		let pt = Geometry::Point(Point::new(-0.1278, 51.5074));
		let pt_ids: Vec<i64> =
			cover_geometry_for_index(&pt, &cfg).into_iter().map(|c| c.0).collect();

		let poly = Geometry::Polygon(Polygon::new(
			LineString::from(vec![
				(-0.2, 51.4),
				(0.0, 51.4),
				(0.0, 51.6),
				(-0.2, 51.6),
				(-0.2, 51.4),
			]),
			vec![],
		));
		let poly_ids: Vec<i64> =
			cover_geometry_for_index(&poly, &cfg).into_iter().map(|c| c.0).collect();

		let line = Geometry::Line(LineString::from(vec![(179.9, -0.1), (180.0, 0.1)]));
		let line_ids: Vec<i64> =
			cover_geometry_for_index(&line, &cfg).into_iter().map(|c| c.0).collect();

		let mpt = Geometry::MultiPoint(geo::MultiPoint(
			(0..15)
				.map(|i| Point::new(-0.20 + (i as f64) * 0.02, 51.45 + (i as f64) * 0.01))
				.collect(),
		));
		let mpt_ids: Vec<i64> =
			cover_geometry_for_index(&mpt, &cfg).into_iter().map(|c| c.0).collect();

		let cap_ops = plan_cap_scans(
			Point::new(-0.1278, 51.5074),
			5_000.0,
			&CoverConfig::default_query(),
			false,
		);

		assert_eq!(pt_ids, vec![1_656_177_213_895_382_648]);
		assert_eq!(
			poly_ids,
			vec![
				1_656_176_910_171_897_856,
				1_656_177_185_049_804_800,
				1_656_177_459_927_711_744,
				1_656_177_691_855_945_728,
				1_656_180_251_656_454_144,
				1_656_180_483_584_688_128,
				1_656_180_758_462_595_072,
				1_656_181_033_340_502_016,
				1_802_583_609_329_057_792,
				1_802_583_755_357_945_856,
				1_802_587_328_770_736_128,
				1_802_587_468_357_173_248,
			]
		);
		assert_eq!(
			line_ids,
			vec![
				2_017_611_628_039_634_944,
				2_017_612_538_572_701_696,
				2_017_612_555_752_570_880,
				2_017_612_572_932_440_064,
				2_017_612_607_292_178_432,
				2_017_612_622_324_563_968,
				2_017_612_676_011_655_168,
				2_017_612_693_191_524_352,
				2_017_612_830_630_477_824,
				2_017_612_854_252_797_952,
				2_017_612_864_990_216_192,
				2_017_613_157_047_992_320,
			]
		);
		assert_eq!(
			mpt_ids,
			vec![
				1_656_177_124_920_262_656,
				1_656_177_159_280_001_024,
				1_656_180_552_052_377_480,
				1_656_180_566_657_338_664,
				1_656_180_698_333_052_928,
				1_656_180_726_343_402_280,
				1_656_180_734_600_456_584,
				1_802_587_397_742_832_376,
				1_802_587_406_032_830_760,
				1_802_587_434_453_959_080,
				1_802_587_443_512_479_144,
				1_802_587_454_829_757_224,
			]
		);
		assert_eq!(
			cap_ops,
			vec![
				SpatialScanOp::Range {
					min: 1_656_177_150_690_066_440,
					max: 1_656_177_173_238_644_728,
				},
				SpatialScanOp::Range {
					min: 1_656_177_180_754_837_512,
					max: 1_656_177_253_769_281_528,
				},
				SpatialScanOp::Range {
					min: 1_656_177_254_843_023_368,
					max: 1_656_177_255_916_765_176,
				},
				SpatialScanOp::Range {
					min: 1_656_177_266_654_183_432,
					max: 1_656_177_288_129_019_896,
				},
				SpatialScanOp::Range {
					min: 1_656_177_391_208_235_016,
					max: 1_656_177_425_567_973_368,
				},
				SpatialScanOp::Range {
					min: 1_656_180_517_944_426_504,
					max: 1_656_180_539_419_262_968,
				},
				SpatialScanOp::Range {
					min: 1_656_180_689_743_118_344,
					max: 1_656_180_792_822_333_432,
				},
			]
		);
	}
}
