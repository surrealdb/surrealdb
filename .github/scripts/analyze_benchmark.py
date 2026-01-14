#!/usr/bin/env python3
"""
Analyze CRUD benchmark results and generate performance reports.

This script parses crud-bench JSON output and generates markdown reports.

Requires Python 3.9+ for secure path validation using pathlib.is_relative_to()
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


# Ensure Python 3.9+ for secure path validation
if sys.version_info < (3, 9):
	print("Error: Python 3.9 or higher is required for secure path validation", file=sys.stderr)
	sys.exit(1)


class BenchmarkAnalyzer:
	"""Analyzes benchmark results and generates reports."""

	def __init__(self, results_dir: Path):
		# Resolve to absolute path to prevent directory traversal
		self.results_dir = results_dir.resolve()
		self.current_results = {}

	@staticmethod
	def _validate_output_path(file_path: Path) -> Path:
		"""
		Validate and sanitize an output file path to prevent path traversal attacks.

		Returns the resolved absolute path if safe, raises ValueError otherwise.
		"""
		# Resolve to absolute path
		resolved = file_path.resolve()

		# Get the current working directory
		cwd = Path.cwd().resolve()

		# Ensure the output path is within or relative to the current working directory
		# This prevents writing to sensitive system directories
		try:
			resolved.relative_to(cwd)
		except ValueError:
			# If we can't make it relative to cwd, it's outside our workspace
			raise ValueError(
				f"Output path {file_path} is outside the current working directory. "
				f"This is not allowed for security reasons."
			)

		# Ensure parent directory exists or can be created
		resolved.parent.mkdir(parents=True, exist_ok=True)

		return resolved

	@staticmethod
	def _validate_and_resolve_input_path(file_path: Path, base_dir: Path) -> Optional[Path]:
		"""
		Validate and resolve an input file path to prevent path traversal attacks.

		Returns the resolved path if it's within base_dir, None otherwise.
		This is the security boundary for reading user-controlled paths.
		"""
		try:
			# Resolve both paths to absolute canonical form
			resolved_file = file_path.resolve(strict=True)
			resolved_base = base_dir.resolve(strict=True)

			# Verify the resolved path is within the allowed base directory
			if not resolved_file.is_relative_to(resolved_base):
				return None

			return resolved_file
		except (OSError, ValueError):
			return None

	@staticmethod
	def _extract_config_name(json_path: Path) -> str:
		"""
		Extract configuration name from benchmark result filename.

		Expected format: result-<config>.json (e.g., result-memory.json)
		Returns: <config> (e.g., 'memory', 'rocksdb', 'embedded')
		"""
		return json_path.stem.replace('result-', '')

	def load_current_results(self) -> Dict:
		"""
		Load current benchmark results from JSON files.

		Security Note: This method reads files from the user-specified results_dir.
		The validation ensures files are ONLY read from within this directory, preventing
		path traversal attacks via symlinks or relative paths. The user explicitly controls
		which directory to read from via the --results-dir CLI argument.
		"""
		results = {}
		json_files = list(self.results_dir.glob("*.json"))
		print(f"Found {len(json_files)} JSON files in {self.results_dir}", file=sys.stderr)

		for json_file in json_files:
			try:
				# Validate and resolve path - returns None if outside results_dir
				safe_path = self._validate_and_resolve_input_path(json_file, self.results_dir)
				if safe_path is None:
					print(f"Warning: Skipping {json_file} (outside results directory)", file=sys.stderr)
					continue

				# safe_path is guaranteed to be within self.results_dir
				with open(safe_path, 'r') as f:
					data = json.load(f)
					config_name = self._extract_config_name(json_file)
					print(f"Loading {config_name}...", file=sys.stderr)
					parsed = self._parse_benchmark_data(data)
					results[config_name] = parsed
			except Exception as e:
				print(f"Warning: Failed to parse {json_file}: {e}", file=sys.stderr)
				import traceback
				traceback.print_exc(file=sys.stderr)

		print(f"Loaded {len(results)} configurations with benchmark data", file=sys.stderr)
		return results

	def _parse_benchmark_data(self, data: Dict) -> Dict:
		"""Parse crud-bench JSON output and extract key metrics."""
		metrics = {
			'create': {},
			'read': {},
			'update': {},
			'delete': {},
			'scans': {},
			'metadata': {}
		}

		# Extract metadata
		if 'metadata' in data:
			metrics['metadata'] = data['metadata']

		# crud-bench uses plural keys (creates, reads, updates, deletes)
		# Map plural to singular for consistency with our analysis
		operation_map = {
			'creates': 'create',
			'reads': 'read',
			'updates': 'update',
			'deletes': 'delete'
		}

		for plural_key, singular_key in operation_map.items():
			if plural_key in data:
				op_data = data[plural_key]

				# crud-bench stores latencies in microseconds, we convert to nanoseconds
				# crud-bench uses 'ops' for throughput, 'q50/q95/q99' for percentiles, 'mean' for average
				elapsed_data = op_data.get('elapsed', {})
				total_time_ms = (elapsed_data.get('secs', 0) * 1000) + (elapsed_data.get('nanos', 0) / 1_000_000)

				metrics[singular_key] = {
					'throughput': op_data.get('ops', 0),  # crud-bench uses 'ops' for operations per second
					'total_time_ms': total_time_ms,
					'avg_time_ns': op_data.get('mean', 0) * 1000,  # Convert µs to ns
					'p50_ns': op_data.get('q50', 0) * 1000,  # Convert µs to ns
					'p95_ns': op_data.get('q95', 0) * 1000,  # Convert µs to ns
					'p99_ns': op_data.get('q99', 0) * 1000,  # Convert µs to ns
					'samples': op_data.get('samples', 0)
				}
				print(f"  {singular_key}: {self.format_throughput(metrics[singular_key]['throughput'])}, "
				      f"p50={self.format_latency(metrics[singular_key]['p50_ns'])}, "
				      f"samples={metrics[singular_key]['samples']}", file=sys.stderr)

		# Extract scan metrics
		if 'scans' in data:
			metrics['scans'] = data['scans']

		return metrics

	def format_throughput(self, throughput: float) -> str:
		"""Format throughput value for display."""
		if throughput >= 1_000_000:
			return f"{throughput / 1_000_000:.1f}M ops/s"
		elif throughput >= 1_000:
			return f"{throughput / 1_000:.1f}k ops/s"
		else:
			return f"{throughput:.0f} ops/s"

	def format_latency(self, latency_ns: float) -> str:
		"""Format latency value for display."""
		if latency_ns >= 1_000_000_000:
			return f"{latency_ns / 1_000_000_000:.2f}s"
		elif latency_ns >= 1_000_000:
			return f"{latency_ns / 1_000_000:.2f}ms"
		elif latency_ns >= 1_000:
			return f"{latency_ns / 1_000:.2f}μs"
		else:
			return f"{latency_ns:.0f}ns"

	def generate_report(self) -> str:
		"""Generate markdown report."""
		report = []
		report.append("## 🔍 CRUD Benchmark Results\n")

		# Summary table
		report.append("### Summary\n")
		report.append("| Configuration | Operation | Throughput | P50 Latency | P95 Latency | P99 Latency | Samples |")
		report.append("|--------------|-----------|------------|-------------|-------------|-------------|---------|")

		# Collect all results
		all_results = []
		for config, metrics in sorted(self.current_results.items()):
			for operation in ['create', 'read', 'update', 'delete']:
				if operation not in metrics or not metrics[operation]:
					continue

				op_data = metrics[operation]
				# Skip if no samples were collected (benchmark didn't run for this operation)
				if not op_data.get('samples'):
					continue

				all_results.append({
					'config': config,
					'operation': operation,
					'data': op_data
				})

		# Generate table rows
		for item in all_results:
			config = item['config']
			operation = item['operation'].capitalize()
			data = item['data']

			throughput = self.format_throughput(data['throughput'])
			p50 = self.format_latency(data['p50_ns'])
			p95 = self.format_latency(data['p95_ns'])
			p99 = self.format_latency(data['p99_ns'])
			samples = data['samples']

			report.append(f"| {config} | {operation} | {throughput} | {p50} | {p95} | {p99} | {samples} |")

		# Detailed metrics
		report.append("\n<details>")
		report.append("<summary>📊 Detailed Metrics</summary>\n")

		for config, metrics in sorted(self.current_results.items()):
			report.append(f"\n#### {config}\n")

			for operation in ['create', 'read', 'update', 'delete']:
				# Skip if operation wasn't initialized (not in metrics) or has no data (empty dict)
				if operation not in metrics or not metrics[operation]:
					continue

				op_data = metrics[operation]
				# Skip if no samples were collected (benchmark didn't run for this operation)
				if not op_data.get('samples'):
					continue

				report.append(f"\n**{operation.capitalize()}**")
				report.append(f"- Throughput: {self.format_throughput(op_data['throughput'])}")
				report.append(f"- Average Latency: {self.format_latency(op_data['avg_time_ns'])}")
				report.append(f"- Latency P50: {self.format_latency(op_data['p50_ns'])}")
				report.append(f"- Latency P95: {self.format_latency(op_data['p95_ns'])}")
				report.append(f"- Latency P99: {self.format_latency(op_data['p99_ns'])}")
				report.append(f"- Total Time: {op_data['total_time_ms']:.0f}ms")
				report.append(f"- Samples: {op_data['samples']}")

		report.append("\n</details>")

		# Methodology
		report.append("\n<details>")
		report.append("<summary>ℹ️ Methodology</summary>\n")
		report.append("\n- **Benchmark parameters:** 10,000 samples, 12 clients, 48 threads")
		report.append("- **Benchmarking tool:** [crud-bench](https://github.com/surrealdb/crud-bench)")
		report.append("- **Metrics:** Throughput (ops/s), Latency percentiles (P50/P95/P99)")
		report.append("</details>")

		return "\n".join(report)

	def run(self, output_file: Path, json_output_file: Optional[Path] = None):
		"""Run the full analysis and generate reports."""
		# Validate output paths to prevent path traversal attacks
		safe_output_file = self._validate_output_path(output_file)
		safe_json_output_file = self._validate_output_path(json_output_file) if json_output_file else None

		# Load current results
		self.current_results = self.load_current_results()

		if not self.current_results:
			print("No benchmark results found", file=sys.stderr)
			with open(safe_output_file, 'w') as f:
				f.write("## 🔍 CRUD Benchmark Results\n\n")
				f.write("⚠️ No benchmark results found. Benchmarks may have failed.\n")
			return

		# Generate report
		report = self.generate_report()

		# Write markdown report
		with open(safe_output_file, 'w') as f:
			f.write(report)

		print(f"Report written to {safe_output_file}")

		# Write JSON output
		if safe_json_output_file:
			output_data = {
				'timestamp': datetime.now().isoformat(),
				'results': self.current_results
			}
			with open(safe_json_output_file, 'w') as f:
				json.dump(output_data, f, indent=2)
			print(f"JSON output written to {safe_json_output_file}")


def main():
	parser = argparse.ArgumentParser(
		description="Analyze CRUD benchmark results"
	)
	parser.add_argument(
		"--results-dir",
		type=Path,
		required=True,
		help="Directory containing current benchmark result JSON files"
	)
	parser.add_argument(
		"--output",
		type=Path,
		required=True,
		help="Output markdown report file"
	)
	parser.add_argument(
		"--json-output",
		type=Path,
		help="Output JSON file with detailed analysis"
	)

	args = parser.parse_args()

	analyzer = BenchmarkAnalyzer(results_dir=args.results_dir)

	try:
		analyzer.run(args.output, args.json_output)
	except Exception as e:
		print(f"Error during analysis: {e}", file=sys.stderr)
		import traceback
		traceback.print_exc()
		sys.exit(1)


if __name__ == "__main__":
	main()
