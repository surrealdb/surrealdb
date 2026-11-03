// Builds the pinned issue overview for surrealdb/surrealdb: a circular Voronoi
// treemap of the open issues (issue type, then status, then one equal cell per
// issue) in a light and a dark version, and the issue's title and body.
//
// Usage: node generate.mjs <issues.json> <out-dir> <image-base-url> <version> [overview-issue]
//
// <issues.json> is an array of { number, issueType: { name }, labels: { nodes: [{ name }] } },
// as the workflow's GraphQL query returns them. The out-dir receives
// issues-light.svg, issues-dark.svg, title.txt and body.md. The overview issue
// itself, when its number is given, is left out of the counts.

import { readFileSync, writeFileSync } from "node:fs";
import { hierarchy } from "d3-hierarchy";
import { polygonArea, polygonCentroid } from "d3-polygon";
import { voronoiTreemap } from "d3-voronoi-treemap";
import seedrandom from "seedrandom";

const [input, outDir, imageBase, version, overview] = process.argv.slice(2);
if (!version) {
	console.error("usage: node generate.mjs <issues.json> <out-dir> <image-base-url> <version> [overview-issue]");
	process.exit(2);
}

const REPO = "surrealdb/surrealdb";
const PARTIAL = "partially complete";
const WORKAROUND = "has workaround";

const issues = JSON.parse(readFileSync(input, "utf8"))
	.filter((n) => String(n.number) !== overview)
	.map((n) => ({
	number: n.number,
	type: n.issueType?.name ?? "None",
	labels: n.labels.nodes.map((l) => l.name),
}));

// Each status group: the type region it belongs to, its legend name, the
// caption under its count in the chart (so that "171" reads as "171 feature
// requests not started" without the legend; the later entries wrap the same
// words for a narrow group), how it is matched, its issue
// search, and its colours. The first group that matches an issue holds
// it, so the order sets precedence. Colours follow the SurrealDB brand: the
// chart red for bugs, the Lilac ramp for feature requests and the Obsidian
// greys for the rest.
const GROUPS = [
	{ region: "Bugs", name: "Partially resolved", caption: [["bugs", "partly resolved"], ["bugs", "partly", "resolved"]], match: (i) => i.type === "Bug" && i.labels.includes(PARTIAL), query: `type:Bug label:"${PARTIAL}"`, light: "#f2a3b3", dark: "#8f2a40" },
	{ region: "Bugs", name: "Not yet resolved", caption: [["bugs", "not yet resolved"], ["bugs", "not yet", "resolved"]], match: (i) => i.type === "Bug", query: `type:Bug -label:"${PARTIAL}"`, light: "#d13757", dark: "#ff5c7a" },
	{ region: "Feature requests", name: "Partially complete", caption: [["feature requests", "partly complete"], ["feature", "requests", "partly", "complete"]], match: (i) => i.type === "Feature" && i.labels.includes(PARTIAL), query: `type:Feature label:"${PARTIAL}"`, light: "#b432de", dark: "#d255fe" },
	{ region: "Feature requests", name: "Has workaround", caption: [["feature requests", "with a workaround"], ["feature", "requests", "with a", "workaround"]], match: (i) => i.type === "Feature" && i.labels.includes(WORKAROUND), query: `type:Feature label:"${WORKAROUND}" -label:"${PARTIAL}"`, light: "#e39cff", dark: "#930fba" },
	{ region: "Feature requests", name: "Not started", caption: [["feature requests", "not started"], ["feature", "requests", "not started"]], match: (i) => i.type === "Feature", query: `type:Feature -label:"${WORKAROUND}" -label:"${PARTIAL}"`, light: "#f3d7ff", dark: "#4e0064" },
	{ region: "Other", name: "Improvements", caption: [["improvements"], ["improve-", "ments"]], match: (i) => i.type === "Improvement", query: "type:Improvement", light: "#a7abb6", dark: "#777a87" },
	{ region: "Other", name: "Other types", caption: [["other issues"]], match: () => true, query: "-type:Bug -type:Feature -type:Improvement", light: "#dadfe7", dark: "#4b4b5b" },
];
const REGIONS = [
	{ name: "Bugs", query: "type:Bug" },
	{ name: "Feature requests", query: "type:Feature" },
	{ name: "Other", query: "-type:Bug -type:Feature" },
];

const members = GROUPS.map(() => []);
for (const issue of issues) members[GROUPS.findIndex((g) => g.match(issue))].push(issue);

const tree = {
	name: "Open issues",
	children: REGIONS.map((region) => ({
		name: region.name,
		children: GROUPS.map((g, i) => ({ ...g, children: members[i].map((m) => ({ name: `#${m.number}`, weight: 1 })) })).filter(
			(g) => g.region === region.name && g.children.length > 0,
		),
	})),
};

// Layout: the same seed gives the same layout for the same issues. The image
// widens to each side only as far as that side's callouts reach.
const SIZE = 640;
const R = SIZE / 2 - 4;
const circle = Array.from({ length: 96 }, (_, k) => {
	const a = (2 * Math.PI * k) / 96;
	return [SIZE / 2 + R * Math.cos(a), SIZE / 2 + R * Math.sin(a)];
});
const root = hierarchy(tree).sum((d) => d.weight ?? 0);
voronoiTreemap().clip(circle).prng(seedrandom("surrealdb")).maxIterationCount(200)(root);

// Relative brightness of a #rrggbb colour, to pick black or white text on it.
const luminance = (hex) => {
	const [r, g, b] = [1, 3, 5].map((i) => Number.parseInt(hex.slice(i, i + 2), 16) / 255);
	return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};
// Width of a convex polygon along the horizontal line at height y.
const width = (p, y) => {
	const xs = [];
	for (let i = 0; i < p.length; i++) {
		const [x1, y1] = p[i];
		const [x2, y2] = p[(i + 1) % p.length];
		if ((y1 <= y && y < y2) || (y2 <= y && y < y1)) xs.push(x1 + ((y - y1) / (y2 - y1)) * (x2 - x1));
	}
	return xs.length < 2 ? 0 : Math.max(...xs) - Math.min(...xs);
};
const path = (p) => `M${p.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join("L")}Z`;

function render(theme) {
	const ink = theme === "light" ? "#061b31" : "#f5f7fa";
	const sub = theme === "light" ? "#525f7a" : "#8f99ac";
	// The lines between cells take GitHub's page colour, so the cells read as cut out of the page.
	const edge = theme === "light" ? "#ffffff" : "#0d1117";
	const out = [];
	for (const leaf of root.leaves()) {
		out.push(`<path d="${path(leaf.polygon)}" fill="${leaf.parent.data[theme]}" stroke="${edge}" stroke-width="0.6"/>`);
	}
	for (const node of root.descendants().filter((n) => n.depth === 2)) {
		out.push(`<path d="${path(node.polygon)}" fill="none" stroke="${edge}" stroke-width="2.5"/>`);
	}
	for (const node of root.descendants().filter((n) => n.depth === 1)) {
		out.push(`<path d="${path(node.polygon)}" fill="none" stroke="${edge}" stroke-width="6"/>`);
	}
	// Label each status group: its count, and under it the caption, using the
	// first wording whose every line fits across the group where it is drawn.
	// A group that no wording fits gets a callout outside the circle instead.
	const callouts = [];
	let left = 0;
	let right = SIZE;
	for (const node of root.descendants().filter((n) => n.depth === 2)) {
		const area = Math.abs(polygonArea(node.polygon));
		const [x, y0] = polygonCentroid(node.polygon);
		const big = Math.max(16, Math.min(44, Math.sqrt(area) / 5));
		const top = (lines) => y0 - (lines.length * 18) / 2;
		const fits = (lines) =>
			width(node.polygon, top(lines) + big * 0.25 - big * 0.6) > String(node.value).length * big * 0.6 + 10 &&
			lines.every((line, k) => width(node.polygon, top(lines) + big * 0.25 + 18 * (k + 1) - 5) > line.length * 7.8 + 10);
		const shown = node.data.caption.find(fits);
		if (!shown) {
			callouts.push({ node, x, y: y0 });
			continue;
		}
		const y = top(shown);
		const text = luminance(node.data[theme]) < 0.55 ? "#ffffff" : "#061b31";
		out.push(`<text x="${x.toFixed(1)}" y="${(y + big * 0.25).toFixed(1)}" font-size="${big.toFixed(0)}" font-weight="600" fill="${text}" text-anchor="middle">${node.value}</text>`);
		shown.forEach((line, k) => {
			out.push(`<text x="${x.toFixed(1)}" y="${(y + big * 0.25 + 18 * (k + 1)).toFixed(1)}" font-size="14" fill="${text}" text-anchor="middle">${line}</text>`);
		});
	}
	// Each callout leaves the circle on the line from its centre through the
	// group, then runs level to its text; callouts on one side are kept 44px
	// apart so their text never overlaps.
	const C = SIZE / 2;
	for (const side of [-1, 1]) {
		const mine = callouts
			.filter((c) => (c.x < C ? -1 : 1) === side)
			.map((c) => {
				const d = Math.hypot(c.x - C, c.y - C) || 1;
				return { ...c, ex: C + ((c.x - C) / d) * (R + 22), ey: C + ((c.y - C) / d) * (R + 22) };
			})
			.sort((a, b) => a.ey - b.ey);
		let last = -Infinity;
		for (const c of mine) {
			c.ey = Math.max(c.ey, last + 44);
			last = c.ey;
			const hx = c.ex + side * 18;
			const tx = hx + side * 6;
			const anchor = side < 0 ? "end" : "start";
			const [first, ...rest] = c.node.data.caption[0];
			// Rough text width: the count at 20px, the rest at 14px.
			const reach = Math.max(String(c.node.value).length * 12 + 4 + first.length * 7.6, rest.join(" ").length * 7.6);
			left = Math.min(left, side < 0 ? tx - reach - 8 : 0);
			right = Math.max(right, side > 0 ? tx + reach + 8 : SIZE);
			out.push(`<polyline points="${hx.toFixed(1)},${c.ey.toFixed(1)} ${c.ex.toFixed(1)},${c.ey.toFixed(1)} ${c.x.toFixed(1)},${c.y.toFixed(1)}" fill="none" stroke="${ink}" stroke-width="1.25" marker-end="url(#arrow-${theme})"/>`);
			out.push(`<text x="${tx.toFixed(1)}" y="${(c.ey - 3).toFixed(1)}" font-size="14" fill="${ink}" text-anchor="${anchor}"><tspan font-size="20" font-weight="600">${c.node.value}</tspan> ${first}</text>`);
			if (rest.length > 0) {
				out.push(`<text x="${tx.toFixed(1)}" y="${(c.ey + 15).toFixed(1)}" font-size="14" fill="${ink}" text-anchor="${anchor}">${rest.join(" ")}</text>`);
			}
		}
	}
	// Legend: one line per region with its total and its groups.
	let ly = SIZE + 24;
	for (const region of root.children) {
		out.push(`<text x="${left + 16}" y="${ly}" font-size="15" font-weight="600" fill="${ink}">${region.data.name}: ${region.value}</text>`);
		let lx = left + 16;
		ly += 22;
		for (const g of region.children) {
			const label = `${g.data.name} ${g.value}`;
			out.push(`<rect x="${lx}" y="${ly - 11}" width="12" height="12" rx="2" fill="${g.data[theme]}"/>`);
			out.push(`<text x="${lx + 18}" y="${ly}" font-size="13" fill="${sub}">${label}</text>`);
			lx += 30 + label.length * 7.2;
		}
		ly += 28;
	}
	const w = Math.ceil(right - left);
	const arrow = `<defs><marker id="arrow-${theme}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0L10,5L0,10Z" fill="${ink}"/></marker></defs>`;
	return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${Math.floor(left)} 0 ${w} ${ly}" width="${w}" height="${ly}" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"><title>${root.value} open issues in ${REPO} by type and status</title>${arrow}${out.join("")}</svg>\n`;
}

let imageWidth = SIZE;
for (const theme of ["light", "dark"]) {
	const svg = render(theme);
	imageWidth = Number(svg.match(/ width="(\d+)"/)[1]);
	writeFileSync(`${outDir}/issues-${theme}.svg`, svg);
}

// Title and body of the pinned issue.
const count = (name) => members[GROUPS.findIndex((g) => g.name === name)].length;
const total = (region) => root.children.find((r) => r.data.name === region)?.value ?? 0;
const search = (query) => `https://github.com/${REPO}/issues?q=${encodeURIComponent(`is:issue is:open ${query}`)}`;
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;

const bugs = total("Bugs");
const features = total("Feature requests");
const begun = count("Partially complete") + count("Has workaround");
writeFileSync(
	`${outDir}/title.txt`,
	`Open issues: ${plural(bugs, "bug", "bugs")} (${count("Partially resolved")} partly resolved), ${plural(features, "feature request", "feature requests")} (${begun} partly done or with a workaround), ${total("Other")} other\n`,
);

const rows = REGIONS.flatMap((region) => [
	`| **[${region.name}](${search(region.query)})** | **${total(region.name)}** |`,
	...GROUPS.filter((g) => g.region === region.name).map((g) => `| [${g.name}](${search(g.query)}) | ${count(g.name)} |`),
]);
const today = new Date().toISOString().slice(0, 10);
writeFileSync(
	`${outDir}/body.md`,
	`<!-- Written by .github/workflows/issue-overview.yml. Edits to this issue are overwritten on the next run. -->

This issue gives an overview of the ${root.value} open issues in this repository, by type and by how far each has come. Each cell in the chart is one issue.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="${imageBase}/issues-dark.svg?v=${version}">
  <img alt="${root.value} open issues: ${bugs} bugs, ${features} feature requests and ${total("Other")} others, each split by status" src="${imageBase}/issues-light.svg?v=${version}" width="${imageWidth}">
</picture>

| Issues | Open |
| --- | ---: |
${rows.join("\n")}

- **Partially resolved** and **Partially complete** issues have the \`${PARTIAL}\` label: part of what they report or ask for has been fixed or built since they were opened.
- **Has workaround** issues have the \`${WORKAROUND}\` label: the feature is not built yet, but the same result can be reached another way today.

Updated every Monday and Friday. Last updated ${today}.
`,
);

console.log(readFileSync(`${outDir}/title.txt`, "utf8").trim());
