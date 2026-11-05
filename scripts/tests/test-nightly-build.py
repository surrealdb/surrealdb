#!/usr/bin/env python3
"""Compile tiny crates to verify flag precedence, cross-target cfg and child Cargo.

Requires the pinned nightly, wasm32-unknown-unknown and cargo-make on PATH.
Run: python3 scripts/tests/test-nightly-build.py
"""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / "scripts/cargo-nightly"


class NightlyBuildTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="nightly-build-")
        self.addCleanup(self.temp.cleanup)
        self.project = Path(self.temp.name)
        (self.project / "rust-toolchain.nightly").write_text((ROOT / "rust-toolchain.nightly").read_text())
        (self.project / "src").mkdir()
        (self.project / ".cargo").mkdir()
        (self.project / "Cargo.toml").write_text(
            '[package]\nname="nightly-probe"\nversion="0.0.0"\nedition="2024"\n'
        )
        (self.project / "src/lib.rs").write_text(
            '#![no_std]\n#[cfg(not(probe_preserved))]\n'
            'compile_error!("caller flags were lost");\npub fn answer() -> u32 { 42 }\n'
        )
        self.env = dict(os.environ, RUSTC_WRAPPER="", CARGO_INCREMENTAL="0",
                        CARGO_TARGET_DIR=str(self.project / "target"))
        for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTUP_TOOLCHAIN",
                    "SURREAL_RUSTC_THREADS", "SURREAL_NIGHTLY_RUSTFLAGS"):
            self.env.pop(key, None)

    def cargo(self, *args, success=True):
        p = subprocess.run([str(WRAPPER), *args], cwd=self.project, env=self.env,
                           text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.assertEqual(p.returncode == 0, success, p.stdout)
        return p.stdout

    def test_target_flags_are_preserved(self):
        (self.project / ".cargo/config.toml").write_text(
            '[target.\'cfg(all())\']\nrustflags=["--cfg", "probe_preserved"]\n'
        )
        output = self.cargo("build", "--offline", "-vv")
        self.assertIn("-Zthreads=8", output)

    def test_explicit_rustflags_are_preserved(self):
        self.env["RUSTFLAGS"] = "--cfg probe_preserved"
        self.env["SURREAL_RUSTC_THREADS"] = "2"
        self.assertIn("-Zthreads=2", self.cargo("build", "--offline", "-vv"))

    def test_encoded_flags_take_precedence(self):
        self.env["CARGO_ENCODED_RUSTFLAGS"] = "--cfg\x1fprobe_preserved"
        self.env["RUSTFLAGS"] = "--cfg this_must_not_replace_encoded_flags"
        self.assertIn("-Zthreads=8", self.cargo("build", "--offline", "-vv"))

    def test_wasm_target_cfg_survives(self):
        (self.project / ".cargo/config.toml").write_text(
            '[target.wasm32-unknown-unknown]\n'
            'rustflags=["--cfg", "probe_preserved", "--cfg", \'getrandom_backend="wasm_js"\']\n'
        )
        (self.project / "src/lib.rs").write_text(
            '#![no_std]\n#[cfg(not(getrandom_backend="wasm_js"))]\n'
            'compile_error!("wasm getrandom backend was lost");\n'
        )
        self.assertIn("-Zthreads=8", self.cargo("build", "--offline", "-vv",
                                              "--target", "wasm32-unknown-unknown"))

    def test_cargo_make_child_inherits_toolchain_and_flags(self):
        # Use the real project's makefiles; only the leaf command is a probe.
        (self.project / "Makefile.toml").write_text(
            f'extend=[{{path="{ROOT / "Makefile.toml"}"}}]\n'
            '[tasks.probe]\ncommand="cargo"\nargs=["build", "--offline", "-vv"]\n'
        )
        self.env["RUSTFLAGS"] = "--cfg probe_preserved"
        self.assertIn("-Zthreads=8", self.cargo("make", "probe"))

    def test_ci_configuration_reaches_child_cargo(self):
        scripts = self.project / "scripts"
        scripts.mkdir()
        for name in ("nightly-env.sh", "configure-nightly-ci"):
            shutil.copy(ROOT / "scripts" / name, scripts / name)
        (self.project / ".cargo/config.toml").write_text(
            '[target.\'cfg(all())\']\nrustflags=["--cfg", "probe_preserved"]\n'
        )
        # Use a different cfg selector: CI appends cfg(all()) to existing config.
        config = self.project / ".cargo/config.toml"
        config.write_text(config.read_text().replace("cfg(all())", "cfg(unix)"))
        env_file = self.project / "github-env"
        output_file = self.project / "github-output"
        self.env.update(GITHUB_ENV=str(env_file), GITHUB_OUTPUT=str(output_file))
        subprocess.run([str(scripts / "configure-nightly-ci")], cwd=self.project,
                       env=self.env, check=True, stdout=subprocess.PIPE)
        for line in env_file.read_text().splitlines():
            key, value = line.split("=", 1)
            self.env[key] = value
        p = subprocess.run(["cargo", "build", "--offline", "-vv"], cwd=self.project,
                           env=self.env, text=True, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)
        self.assertEqual(p.returncode, 0, p.stdout)
        self.assertIn("-Zthreads=8", p.stdout)

    def test_invalid_threads_fail_before_cargo(self):
        self.env["SURREAL_RUSTC_THREADS"] = "0"
        self.assertIn("positive integer", self.cargo("build", success=False))

    def test_cranelift_rejects_test_and_coverage(self):
        for cmd in ("test", "llvm-cov"):
            self.assertIn("LLVM for tests", self.cargo("--cranelift", cmd, success=False))


if __name__ == "__main__":
    unittest.main()
