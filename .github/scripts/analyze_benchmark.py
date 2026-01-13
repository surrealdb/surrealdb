#!/usr/bin/env python3
"""
Analyze CRUD benchmark results and generate performance regression reports.

This script parses crud-bench JSON output, compares against historical baselines,
and generates markdown reports with statistical analysis.

Requires Python 3.9+ for secure path validation using pathlib.is_relative_to()
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import statistics

# Ensure Python 3.9+ for secure path validation
if sys.version_info < (3, 9):
	print("Error: Python 3.9 or higher is required for secure path validation", file=sys.stderr)
	sys.exit(1)

try:
	import numpy as np
	from scipy import stats
	HAS_SCIPY = True
except ImportError:
	print("Warning: scipy not available, using basic statistics", file=sys.stderr)
	HAS_SCIPY = False


class BenchmarkAnalyzer:
	"""Analyzes benchmark results and detects performance regressions."""

	def __init__(self, results_dir: Path, historical_days: int = 30, regression_threshold: float = 0.15):
		# Resolve to absolute path to prevent directory traversal
		self.results_dir = results_dir.resolve()
		self.historical_days = historical_days
		self.regression_threshold = regression_threshold
		self.current_results = {}
		self.historical_results = []

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
		for json_file in self.results_dir.glob("*.json"):
			try:
				# Resolve to absolute path and verify it's within results_dir
				# This prevents path traversal and symlink attacks
				resolved_file = json_file.resolve(strict=True)

				# Use is_relative_to for secure path validation (Python 3.9+)
				# Only read files that are confirmed to be within results_dir
				if not resolved_file.is_relative_to(self.results_dir):
					print(f"Warning: Skipping {json_file} (outside results directory)", file=sys.stderr)
					continue

				# SAFE: resolved_file has been validated to be within self.results_dir
				with open(resolved_file, 'r') as f:
					data = json.load(f)
					config_name = self._extract_config_name(json_file)
					results[config_name] = self._parse_benchmark_data(data)
			except Exception as e:
				print(f"Warning: Failed to parse {json_file}: {e}", file=sys.stderr)
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

		# Extract operation metrics
		operations = ['create', 'read', 'update', 'delete']
		for op in operations:
			if op in data:
				op_data = data[op]
				metrics[op] = {
					'throughput': op_data.get('throughput', 0),
					'total_time_ms': op_data.get('total_time_ms', 0),
					'avg_time_ns': op_data.get('avg_time_ns', 0),
					'p50_ns': op_data.get('p50_ns', 0),
					'p95_ns': op_data.get('p95_ns', 0),
					'p99_ns': op_data.get('p99_ns', 0),
					'samples': op_data.get('samples', 0)
				}

		# Extract scan metrics
		if 'scans' in data:
			metrics['scans'] = data['scans']

		return metrics

	def load_historical_results(self, benchmark_results_dir: Path) -> List[Dict]:
		"""
		Load historical benchmark results from the benchmark-results branch.

		Security Note: Reads files from the benchmark-results Git branch directory.
		Validates all paths are within benchmark_results_dir to prevent path traversal.
		"""
		historical = []
		# Resolve to absolute path to prevent directory traversal
		benchmark_results_dir = benchmark_results_dir.resolve()

		if not benchmark_results_dir.exists():
			return historical

		cutoff_date = datetime.now() - timedelta(days=self.historical_days)

		for date_dir in sorted(benchmark_results_dir.glob("*"), reverse=True):
			if not date_dir.is_dir():
				continue

			try:
				# Validate date_dir is within benchmark_results_dir
				resolved_date_dir = date_dir.resolve(strict=True)

				# Use is_relative_to for secure path validation (Python 3.9+)
				if not resolved_date_dir.is_relative_to(benchmark_results_dir):
					print(f"Warning: Skipping {date_dir} (outside results directory)", file=sys.stderr)
					continue

				dir_date = datetime.strptime(date_dir.name, "%Y-%m-%d")
				if dir_date < cutoff_date:
					break

				for json_file in date_dir.glob("*.json"):
					# Validate json_file is within date_dir
					resolved_file = json_file.resolve(strict=True)

					# Use is_relative_to for secure path validation (Python 3.9+)
					# Only read files that are confirmed to be within the date directory
					if not resolved_file.is_relative_to(resolved_date_dir):
						print(f"Warning: Skipping {json_file} (outside date directory)", file=sys.stderr)
						continue

					# SAFE: resolved_file has been validated to be within resolved_date_dir
					with open(resolved_file, 'r') as f:
						data = json.load(f)
						config_name = self._extract_config_name(json_file)
						historical.append({
							'date': dir_date,
							'config': config_name,
							'metrics': self._parse_benchmark_data(data)
						})
			except Exception as e:
				print(f"Warning: Failed to parse historical data from {date_dir}: {e}", file=sys.stderr)

		return historical

	def calculate_baseline(self, config: str, operation: str, metric: str) -> Optional[Tuple[float, float]]:
		"""Calculate baseline median and stddev for a specific metric."""
		values = []
		for result in self.historical_results:
			if result['config'] == config:
				op_data = result['metrics'].get(operation, {})
				if metric in op_data:
					values.append(op_data[metric])

		if not values:
			return None

		median = statistics.median(values)
		if len(values) > 1:
			stddev = statistics.stdev(values)
		else:
			stddev = 0

		return median, stddev

	def detect_regressions(self) -> Dict:
		"""Detect performance regressions by comparing current vs historical."""
		regressions = {
			'detected': [],
			'improvements': [],
			'stable': []
		}

		for config, metrics in self.current_results.items():
			for operation in ['create', 'read', 'update', 'delete']:
				if operation not in metrics:
					continue

				current_throughput = metrics[operation].get('throughput', 0)
				if current_throughput == 0:
					continue

				baseline = self.calculate_baseline(config, operation, 'throughput')
				if not baseline:
					regressions['stable'].append({
						'config': config,
						'operation': operation,
						'status': 'no_baseline',
						'current_throughput': current_throughput
					})
					continue

				baseline_throughput, stddev = baseline
				if baseline_throughput == 0:
					continue

				change_pct = ((current_throughput - baseline_throughput) / baseline_throughput) * 100

				# Detect regression (performance degradation)
				if change_pct < -(self.regression_threshold * 100):
					significance = self._test_significance(
						current_throughput,
						baseline_throughput,
						stddev
					)
					regressions['detected'].append({
						'config': config,
						'operation': operation,
						'current_throughput': current_throughput,
						'baseline_throughput': baseline_throughput,
						'change_pct': change_pct,
						'significant': significance
					})
				# Detect improvement
				elif change_pct > (self.regression_threshold * 100):
					regressions['improvements'].append({
						'config': config,
						'operation': operation,
						'current_throughput': current_throughput,
						'baseline_throughput': baseline_throughput,
						'change_pct': change_pct
					})
				else:
					regressions['stable'].append({
						'config': config,
						'operation': operation,
						'current_throughput': current_throughput,
						'baseline_throughput': baseline_throughput,
						'change_pct': change_pct
					})

		return regressions

	def _test_significance(self, current: float, baseline: float, stddev: float) -> bool:
		"""Test if the difference is statistically significant."""
		if stddev == 0:
			return abs(current - baseline) > 0

		# Simple z-test for significance
		z_score = abs(current - baseline) / stddev
		return z_score > 1.96  # 95% confidence level

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

	def generate_report(self, regressions: Dict) -> str:
		"""Generate markdown report."""
		report = []
		report.append("## 🔍 CRUD Benchmark Results\n")

		# Overall status
		if regressions['detected']:
			report.append("### ⚠️ Performance Regressions Detected\n")
		else:
			report.append("### ✅ No Performance Regressions Detected\n")

		# Summary table
		report.append("### Summary\n")
		report.append("| Configuration | Operation | Status | Current | Baseline | Change |")
		report.append("|--------------|-----------|--------|---------|----------|--------|")

		# Sort by config and operation
		all_results = []
		for item in regressions['detected']:
			all_results.append(('⚠️', item))
		for item in regressions['stable']:
			if item.get('baseline_throughput'):
				all_results.append(('✅', item))
			else:
				all_results.append(('ℹ️', item))
		for item in regressions['improvements']:
			all_results.append(('🚀', item))

		for status, item in all_results:
			config = item['config']
			operation = item['operation'].capitalize()
			current = self.format_throughput(item['current_throughput'])

			if item.get('baseline_throughput'):
				baseline = self.format_throughput(item['baseline_throughput'])
				change_pct = item['change_pct']
				if change_pct > 0:
					change = f"+{change_pct:.1f}%"
				else:
					change = f"{change_pct:.1f}%"
			else:
				baseline = "N/A"
				change = "N/A"

			report.append(f"| {config} | {operation} | {status} | {current} | {baseline} | {change} |")

		# Detailed regression warnings
		if regressions['detected']:
			report.append("\n### ⚠️ Performance Warnings\n")
			for item in regressions['detected']:
				config = item['config']
				operation = item['operation'].capitalize()
				change_pct = abs(item['change_pct'])
				current = self.format_throughput(item['current_throughput'])
				baseline = self.format_throughput(item['baseline_throughput'])

				sig_marker = " (statistically significant)" if item.get('significant') else ""
				report.append(
					f"- **{config} - {operation}:** {change_pct:.1f}% slower than {self.historical_days}-day median "
					f"({current} vs {baseline}){sig_marker}"
				)

		# Improvements
		if regressions['improvements']:
			report.append("\n### 🚀 Performance Improvements\n")
			for item in regressions['improvements']:
				config = item['config']
				operation = item['operation'].capitalize()
				change_pct = item['change_pct']
				current = self.format_throughput(item['current_throughput'])
				baseline = self.format_throughput(item['baseline_throughput'])

				report.append(
					f"- **{config} - {operation}:** {change_pct:.1f}% faster than {self.historical_days}-day median "
					f"({current} vs {baseline})"
				)

		# Detailed metrics
		report.append("\n<details>")
		report.append("<summary>📊 Detailed Metrics</summary>\n")

		for config, metrics in self.current_results.items():
			report.append(f"\n#### {config}\n")

			for operation in ['create', 'read', 'update', 'delete']:
				if operation not in metrics or not metrics[operation].get('throughput'):
					continue

				op_data = metrics[operation]
				report.append(f"\n**{operation.capitalize()}**")
				report.append(f"- Throughput: {self.format_throughput(op_data['throughput'])}")
				report.append(f"- Latency P50: {self.format_latency(op_data['p50_ns'])}")
				report.append(f"- Latency P95: {self.format_latency(op_data['p95_ns'])}")
				report.append(f"- Latency P99: {self.format_latency(op_data['p99_ns'])}")
				report.append(f"- Samples: {op_data['samples']}")

		report.append("\n</details>")

		# Methodology
		report.append("\n<details>")
		report.append("<summary>ℹ️ Methodology</summary>\n")
		report.append(f"\n- **Baseline:** Rolling {self.historical_days}-day median")
		report.append(f"- **Regression threshold:** {self.regression_threshold * 100}% performance degradation")
		report.append("- **Significance test:** Z-test at 95% confidence level")
		report.append("- **Benchmark parameters:** 10,000 samples, 12 clients, 48 threads")
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

		# Load historical results
		historical_dir = Path("results")
		if historical_dir.exists():
			self.historical_results = self.load_historical_results(historical_dir)
			print(f"Loaded {len(self.historical_results)} historical data points", file=sys.stderr)
		else:
			print("No historical data available yet", file=sys.stderr)

		# Detect regressions
		regressions = self.detect_regressions()

		# Generate report
		report = self.generate_report(regressions)

		# Write markdown report
		with open(safe_output_file, 'w') as f:
			f.write(report)

		print(f"Report written to {safe_output_file}")

		# Write JSON output
		if safe_json_output_file:
			output_data = {
				'timestamp': datetime.now().isoformat(),
				'current_results': self.current_results,
				'regressions': regressions
			}
			with open(safe_json_output_file, 'w') as f:
				json.dump(output_data, f, indent=2)
			print(f"JSON output written to {safe_json_output_file}")


def main():
	parser = argparse.ArgumentParser(
		description="Analyze CRUD benchmark results and detect performance regressions"
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
	parser.add_argument(
		"--historical-days",
		type=int,
		default=30,
		help="Number of days of historical data to compare against (default: 30)"
	)
	parser.add_argument(
		"--regression-threshold",
		type=float,
		default=0.15,
		help="Regression threshold as a decimal (default: 0.15 = 15%%)"
	)

	args = parser.parse_args()

	analyzer = BenchmarkAnalyzer(
		results_dir=args.results_dir,
		historical_days=args.historical_days,
		regression_threshold=args.regression_threshold
	)

	try:
		analyzer.run(args.output, args.json_output)
	except Exception as e:
		print(f"Error during analysis: {e}", file=sys.stderr)
		import traceback
		traceback.print_exc()
		sys.exit(1)


if __name__ == "__main__":
	main()

