#!/usr/bin/env python3
"""
Automated test suite runner with intelligent retry logic for flaky integration tests.

This script:
1. Runs the full test suite initially (with normal cleanup)
2. Identifies failed tests and retries them with configurable strategy
3. Manages test namespaces intelligently (cleanup on success, keep failed for debugging by default)
4. Provides detailed logging and comprehensive reporting

Usage: ./scripts/auto-retry-tests.py --parallel 4 --attempts-serial 3 --attempts-parallel 2 --venv ./venv
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class TestConstants:
    """Constants used throughout the test runner."""

    MIN_RUNTIME_THRESHOLD = 30.0  # Filter out quick failures (seconds)
    MAX_RUNTIME_HISTORY = 50  # Keep only recent runs
    MAX_TEST_NAME_LENGTH = 100  # Maximum test name length for filenames
    HASH_SUFFIX_LENGTH = 8  # Length of MD5 hash suffix
    DEFAULT_PARALLEL_WORKERS = 2  # Default number of parallel workers

    # Regex patterns
    NAMESPACE_PATTERN = r"Creating namespace: (kuttl-test-[^\s]+)"
    FAILED_TEST_PATTERN = r"--- FAIL: kuttl/harness/([^\s]+)"


@dataclass
class RuntimeHistory:
    """Tracks runtime history for a specific test."""

    test_name: str
    runtimes: List[float]  # List of all recorded runtimes in seconds
    last_updated: str  # ISO timestamp of last update

    def add_runtime(self, duration: float) -> None:
        """Add a new runtime measurement."""
        # Only track runtimes longer than threshold to filter out quick failures
        if duration >= TestConstants.MIN_RUNTIME_THRESHOLD:
            self.runtimes.append(duration)
            self.last_updated = datetime.now().isoformat()
            # Keep only the most recent runs to avoid infinite growth
            if len(self.runtimes) > TestConstants.MAX_RUNTIME_HISTORY:
                self.runtimes = self.runtimes[-TestConstants.MAX_RUNTIME_HISTORY :]

    def get_average_runtime(self) -> float:
        """Get the average runtime."""
        return statistics.mean(self.runtimes) if self.runtimes else 0.0

    def get_median_runtime(self) -> float:
        """Get the median runtime."""
        return statistics.median(self.runtimes) if self.runtimes else 0.0

    def get_runtime_range(self) -> Tuple[float, float]:
        """Get the min and max runtime."""
        if not self.runtimes:
            return 0.0, 0.0
        return min(self.runtimes), max(self.runtimes)

    def estimate_remaining_time(self) -> float:
        """Estimate remaining time based on average."""
        return self.get_average_runtime()


@dataclass
class TestResult:
    """Represents the result of a single test run."""

    test_name: str
    attempt: int
    attempt_type: str  # 'initial', 'parallel', 'serial'
    success: bool
    namespace: Optional[str] = None
    duration: float = 0.0
    log_file: Optional[str] = None
    failed_step: Optional[str] = None


@dataclass
class TestSummary:
    """Summary of all attempts for a single test."""

    test_name: str
    initial_result: TestResult
    retry_results: List[TestResult]
    final_status: str  # 'passed', 'failed', 'flaky'
    total_attempts: int
    final_namespace: Optional[str] = None


class FileOperationError(Exception):
    """Exception raised for file operation errors."""

    pass


class StateManager:
    """Manages test run state persistence and runtime history."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.state_file = output_dir / "test_run_state.json"
        self.runtime_file = output_dir / "runtime_history.json"
        self.runtime_history: Dict[str, RuntimeHistory] = {}

    @contextmanager
    def safe_file_operation(self, file_path: Path, mode: str = "r"):
        """Context manager for safe file operations."""
        try:
            with open(file_path, mode) as file_handle:
                yield file_handle
        except FileNotFoundError:
            if mode == "r":  # Reading non-existent file is sometimes expected
                raise FileOperationError(f"File not found: {file_path}")
            raise
        except PermissionError:
            raise FileOperationError(f"Permission denied: {file_path}")
        except Exception as exception:
            raise FileOperationError(f"Unexpected error with {file_path}: {exception}")

    def safe_json_operation(
        self, operation: str, file_path: Path, data: dict = None
    ) -> any:
        """Safely handle JSON file operations with consistent error handling."""
        try:
            if operation == "load":
                with self.safe_file_operation(file_path, "r") as file_handle:
                    return json.load(file_handle)
            elif operation == "save":
                with self.safe_file_operation(file_path, "w") as file_handle:
                    json.dump(data, file_handle, indent=2)
                return True
            else:
                raise ValueError(f"Unknown operation: {operation}")
        except FileOperationError as exception:
            if operation == "load" and "not found" in str(exception):
                return None  # File not found is acceptable for loading
            print(f"⚠️ Failed to {operation} JSON file {file_path}: {exception}")
            return None if operation == "load" else False

    def load_runtime_history(self) -> None:
        """Load runtime history from JSON file."""
        history_data = self.safe_json_operation("load", self.runtime_file)
        if not history_data:
            return

        loaded_count = 0
        for test_name, data in history_data.items():
            # Filter out short runtimes from existing data
            filtered_runtimes = [
                runtime
                for runtime in data["runtimes"]
                if runtime >= TestConstants.MIN_RUNTIME_THRESHOLD
            ]

            if filtered_runtimes:  # Only keep tests that have valid runtime data
                self.runtime_history[test_name] = RuntimeHistory(
                    test_name=data["test_name"],
                    runtimes=filtered_runtimes,
                    last_updated=data["last_updated"],
                )
                loaded_count += 1

        if loaded_count > 0:
            print(f"📈 Loaded runtime history for {loaded_count} tests")
            # Save cleaned up data back to file
            self.save_runtime_history()

    def save_runtime_history(self) -> bool:
        """Save runtime history to JSON file."""
        history_data = {
            test_name: asdict(history)
            for test_name, history in self.runtime_history.items()
        }
        return self.safe_json_operation("save", self.runtime_file, history_data)

    def update_runtime_history(self, test_name: str, duration: float) -> None:
        """Update runtime history for a test."""
        if test_name not in self.runtime_history:
            self.runtime_history[test_name] = RuntimeHistory(
                test_name=test_name,
                runtimes=[],
                last_updated=datetime.now().isoformat(),
            )

        self.runtime_history[test_name].add_runtime(duration)
        self.save_runtime_history()

    def get_estimated_runtime(self, test_name: str) -> float:
        """Get estimated runtime for a test based on history."""
        if test_name in self.runtime_history:
            return self.runtime_history[test_name].get_average_runtime()
        return 0.0

    def build_state_data(self, runner) -> dict:
        """Build state data dictionary for persistence."""
        return {
            "start_time": runner.start_time.isoformat(),
            "failed_tests": runner.failed_tests,
            "parallel_attempts_done": runner.parallel_attempts_done,
            "serial_attempts_done": runner.serial_attempts_done,
            "initial_result_log": runner.initial_result_log,
            "configuration": self.build_configuration_dict(runner.args),
            "test_summaries": {
                name: asdict(summary) for name, summary in runner.test_summaries.items()
            },
            "all_results": [asdict(result) for result in runner.results],
        }

    def build_configuration_dict(self, args) -> dict:
        """Build configuration dictionary from arguments."""
        return {
            "parallel": args.parallel,
            "attempts_parallel": args.attempts_parallel,
            "attempts_serial": args.attempts_serial,
            "delete_failed_namespaces": args.delete_failed_namespaces,
            "venv": args.venv,
            "extra_args": args.extra_args,
            "output_dir": str(self.output_dir),
        }

    def save_state(self, runner) -> bool:
        """Save current state to JSON file for resuming later."""
        state_data = self.build_state_data(runner)
        success = self.safe_json_operation("save", self.state_file, state_data)
        if success:
            print(f"📁 State saved to: {self.state_file}")
        return success

    def load_state(self, state_file: Path, runner) -> bool:
        """Load previous state from JSON file for resuming."""
        state_data = self.safe_json_operation("load", state_file)
        if not state_data:
            return False

        try:
            # Restore basic state
            runner.start_time = datetime.fromisoformat(state_data["start_time"])
            runner.failed_tests = state_data["failed_tests"]
            runner.parallel_attempts_done = state_data["parallel_attempts_done"]
            runner.serial_attempts_done = state_data["serial_attempts_done"]
            runner.initial_result_log = state_data.get("initial_result_log")

            # Restore test summaries
            for name, summary_dict in state_data["test_summaries"].items():
                # Convert dict back to TestResult objects
                initial_result = TestResult(**summary_dict["initial_result"])
                retry_results = [
                    TestResult(**result) for result in summary_dict["retry_results"]
                ]

                summary = TestSummary(
                    test_name=summary_dict["test_name"],
                    initial_result=initial_result,
                    retry_results=retry_results,
                    final_status=summary_dict["final_status"],
                    total_attempts=summary_dict["total_attempts"],
                    final_namespace=summary_dict.get("final_namespace"),
                )
                runner.test_summaries[name] = summary

            # Restore all results
            runner.results = [
                TestResult(**result) for result in state_data["all_results"]
            ]

            print(f"📂 Loaded state from: {state_file}")
            print(
                f"   Original start time: {runner.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            print(f"   Failed tests: {len(runner.failed_tests)}")
            print(f"   Parallel attempts done: {runner.parallel_attempts_done}")
            print(f"   Completed tests: {len(runner.test_summaries)}")

            return True

        except Exception as exception:
            print(f"❌ Failed to restore state: {exception}")
            return False

    def find_latest_state_file(self) -> Optional[Path]:
        """Find the most recent state file in the output directory."""
        state_files = list(self.output_dir.glob("**/test_run_state.json"))
        if not state_files:
            return None

        # Return the most recently modified state file
        return max(state_files, key=lambda file_path: file_path.stat().st_mtime)

    def load_failed_tests_from_state(
        self, state_file: Optional[Path] = None
    ) -> List[str]:
        """Load failed test list from a state file."""
        if state_file is None:
            state_file = self.find_latest_state_file()
            if state_file is None:
                print("❌ No state file found in output directory")
                return []

        state_data = self.safe_json_operation("load", state_file)
        if not state_data:
            return []

        failed_tests = state_data.get("failed_tests", [])
        print(f"📂 Loaded {len(failed_tests)} failed tests from: {state_file}")
        return failed_tests


class TestExecutor:
    """Handles test execution and command building."""

    def __init__(self, args, state_manager: StateManager):
        self.args = args
        self.state_manager = state_manager

    @staticmethod
    def format_duration(seconds: float) -> str:
        """Format duration in a human-readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.1f}m"
        else:
            return f"{seconds / 3600:.1f}h"

    @staticmethod
    def shorten_test_name(test_name: str, max_length: int = 60) -> str:
        """Shorten test name for display purposes."""
        if len(test_name) <= max_length:
            return test_name

        # Try to keep meaningful parts
        if "," in test_name:
            parts = test_name.split(",")
            # Keep first and last parts, truncate middle
            if len(parts) > 2:
                return f"{parts[0]}...{parts[-1]}"

        # Simple truncation with ellipsis
        return test_name[: max_length - 3] + "..."

    @staticmethod
    def create_safe_log_filename(
        test_name: str, attempt: int, attempt_type: str
    ) -> str:
        """Create a safe filename for test logs, handling long test names."""
        safe_test_name = test_name.replace("/", "_")

        if len(safe_test_name) > TestConstants.MAX_TEST_NAME_LENGTH:
            # Keep first part and add hash of full name
            hash_suffix = hashlib.md5(test_name.encode()).hexdigest()[
                : TestConstants.HASH_SUFFIX_LENGTH
            ]
            safe_test_name = (
                safe_test_name[: TestConstants.MAX_TEST_NAME_LENGTH - 9]
                + "_"
                + hash_suffix
            )

        return f"{safe_test_name}_attempt_{attempt}_{attempt_type}.txt"

    def build_test_command(
        self, test_name: str = None, skip_delete: bool = False, work_dir: str = None
    ) -> List[str]:
        """Build the command arguments for running tests."""
        command_args = ["scripts/run-tests"]

        # Add configured arguments
        if self.args.parallel and not test_name:  # Only use parallel for full suite
            command_args.extend(["--parallel", str(self.args.parallel)])

        if skip_delete:
            command_args.extend(["--skip-delete"])

        if test_name:
            command_args.extend(["--test", test_name])

        # Add unique work directory to prevent parallel interference
        # beku deletes the work dir at the start of a test so if a test was already running and then
        # another starts the new one would delete (and recreate) the work directory.
        # This does lead to failures.
        if work_dir:
            command_args.extend(["--work-dir", work_dir])

        # Add any extra arguments passed through
        if self.args.extra_args:
            command_args.extend(self.args.extra_args)

        return command_args

    def setup_test_environment(self) -> Optional[dict]:
        """Set up environment with virtualenv if specified."""
        if not self.args.venv:
            return None

        venv_bin = Path(self.args.venv) / "bin"
        if not venv_bin.exists():
            return None

        environment_vars = dict(os.environ)
        environment_vars["PATH"] = f"{venv_bin}:{environment_vars['PATH']}"
        environment_vars["VIRTUAL_ENV"] = str(self.args.venv)
        return environment_vars

    @staticmethod
    def extract_test_namespace_from_log_file(log_file: Path) -> Optional[str]:
        """Extract namespace from test log file."""
        try:
            with open(log_file, "r") as file_handle:
                content = file_handle.read()

            # Look for: "Creating namespace: kuttl-test-xyz"
            match = re.search(TestConstants.NAMESPACE_PATTERN, content)
            return match.group(1) if match else None
        except Exception:
            return None

    @staticmethod
    def extract_failed_step(log_file: Path) -> Optional[str]:
        """Extract the failed step from the log."""
        try:
            with open(log_file, "r") as file_handle:
                content = file_handle.read()

            # Look for step failure pattern: "failed in step <step-name>"
            step_match = re.search(r"failed in step ([\w\-]+)", content)
            if step_match:
                return f"failed in step {step_match.group(1)}"

            return "Could not find failed step"
        except Exception:
            return "Could not find failed step"

    @staticmethod
    def parse_failed_tests_from_log(log_file: Path) -> List[str]:
        """Parse failed tests from the initial test run log."""
        try:
            with open(log_file, "r") as file_handle:
                content = file_handle.read()

            # Look for lines that match the pattern: "--- FAIL: kuttl/harness/TEST_NAME"
            matches = re.findall(TestConstants.FAILED_TEST_PATTERN, content)
            return matches
        except Exception as exception:
            print(f"Error parsing failed tests: {exception}")
            return []

    @staticmethod
    def delete_test_namespace(namespace: str) -> bool:
        """Delete a test namespace."""
        if not namespace:
            return False

        try:
            command_args = ["kubectl", "delete", "namespace", namespace]
            result = subprocess.run(command_args, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"  ✓ Deleted namespace: {namespace}")
                return True
            else:
                print(f"  ✗ Failed to delete namespace {namespace}: {result.stderr}")
                return False
        except Exception as exception:
            print(f"  ✗ Error deleting namespace {namespace}: {exception}")
            return False

    def run_single_test_suite(
        self,
        output_dir: Path,
        test_name: str = None,
        skip_delete: bool = False,
        attempt: int = 1,
        attempt_type: str = "initial",
    ) -> TestResult:
        """Run a single test or the full test suite."""
        # Create unique work directory to prevent parallel test interference
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[
            :-3
        ]  # microseconds to milliseconds
        if test_name:
            # Create a unique work directory based on test name and timestamp
            safe_test_name = test_name.replace("/", "_").replace(",", "_")
            work_dir = (
                f"tests/_work_{safe_test_name}_{attempt}_{attempt_type}_{timestamp}"
            )
        else:
            work_dir = f"tests/_work_full_suite_{attempt}_{attempt_type}_{timestamp}"

        command_args = self.build_test_command(test_name, skip_delete, work_dir)

        # Set up log file
        if test_name:
            log_filename = self.create_safe_log_filename(
                test_name, attempt, attempt_type
            )
            log_dir = output_dir / "retries"
        else:
            # Add date to full suite log filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"full_suite_{attempt_type}_{timestamp}.txt"
            log_dir = output_dir

        log_file = log_dir / log_filename

        if test_name:
            short_name = self.shorten_test_name(test_name)
            # Get runtime estimate for the test
            estimated = self.state_manager.get_estimated_runtime(test_name)
            if estimated > 0:
                estimate_text = f" ⏰ Estimated: {self.format_duration(estimated)}"
            else:
                estimate_text = ""
            print(
                f"Running {short_name} (attempt {attempt}/{attempt_type})...{estimate_text}"
            )

        # Set up environment with virtualenv if specified
        environment_vars = self.setup_test_environment()

        # Run the test
        start_time = time.time()

        with open(log_file, "w") as file_handle:
            # Log the exact command being executed
            file_handle.write(f"Command: {' '.join(command_args)}\n")
            file_handle.write(f"Working directory: {os.getcwd()}\n")
            file_handle.write("=" * 80 + "\n\n")
            file_handle.flush()

            result = subprocess.run(
                command_args,
                stdout=file_handle,
                stderr=subprocess.STDOUT,
                env=environment_vars,
            )

        duration = time.time() - start_time
        success = result.returncode == 0

        # Extract namespace and error info from log
        namespace = self.extract_test_namespace_from_log_file(log_file)
        failed_step = None if success else self.extract_failed_step(log_file)

        test_result = TestResult(
            test_name=test_name or "full_suite",
            attempt=attempt,
            attempt_type=attempt_type,
            success=success,
            namespace=namespace,
            duration=duration,
            log_file=str(log_file),
            failed_step=failed_step,
        )

        # Update runtime history for individual tests
        if test_name and test_name != "full_suite":
            self.state_manager.update_runtime_history(test_name, duration)

            # Print completion with runtime info
            status_icon = "✅" if success else "❌"
            print(f"  {status_icon} Completed in {self.format_duration(duration)}")

            # Show runtime comparison if we have history
            if test_name in self.state_manager.runtime_history:
                history = self.state_manager.runtime_history[test_name]
                avg_runtime = history.get_average_runtime()
                if len(history.runtimes) > 1:
                    if duration > avg_runtime * 1.2:
                        print(
                            f"    ⚠️  Slower than average ({self.format_duration(avg_runtime)})"
                        )
                    elif duration < avg_runtime * 0.8:
                        print(
                            f"    ⚡ Faster than average ({self.format_duration(avg_runtime)})"
                        )
                    else:
                        print(f"    📊 Average: {self.format_duration(avg_runtime)}")

        # If this was a skip_delete attempt and test passed, clean up the namespace
        # We only want to keep failed namespaces
        if test_name and skip_delete and success and namespace:
            self.delete_test_namespace(namespace)
            print(f"  🧹 Test passed, cleaning up namespace: {namespace}")

        # Clean up the unique work directory after test completion
        if work_dir and Path(work_dir).exists():
            try:
                shutil.rmtree(work_dir)
            except Exception as exception:
                print(f"  ⚠️ Failed to clean up work directory {work_dir}: {exception}")

        return test_result


class ReportGenerator:
    """Handles test reporting and output generation."""

    def __init__(self, output_dir: Path, state_manager: StateManager):
        self.output_dir = output_dir
        self.state_manager = state_manager

    @staticmethod
    def format_duration(seconds: float) -> str:
        """Format duration in a human-readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.1f}m"
        else:
            return f"{seconds / 3600:.1f}h"

    def print_runtime_summary(self, test_names: List[str], title: str) -> None:
        """Print a summary of runtime estimates for a list of tests."""
        print(f"\n📊 {title}")
        print("-" * 46)

        total_estimated = 0.0
        tests_with_history = 0

        for test_name in test_names:
            estimated = self.state_manager.get_estimated_runtime(test_name)
            if estimated > 0:
                tests_with_history += 1
                total_estimated += estimated

                # Get runtime statistics
                history = self.state_manager.runtime_history[test_name]
                min_time, max_time = history.get_runtime_range()
                avg_time = history.get_average_runtime()

                print(f"  {test_name}")
                print(
                    f"    Est: {self.format_duration(estimated)} | "
                    f"Avg: {self.format_duration(avg_time)} | "
                    f"Range: {self.format_duration(min_time)}-{self.format_duration(max_time)} | "
                    f"Runs: {len(history.runtimes)}"
                )
            else:
                print(f"  {test_name} - No runtime history")

        if tests_with_history > 0:
            print(f"\nTotal estimated time: {self.format_duration(total_estimated)}")
            print(f"Tests with history: {tests_with_history}/{len(test_names)}")
        print()

    def generate_comprehensive_report(self, runner, start_time: datetime) -> str:
        """Generate a comprehensive test report."""
        end_time = datetime.now()
        total_duration = end_time - start_time

        # Categorize tests
        passed_tests = [
            s for s in runner.test_summaries.values() if s.final_status == "passed"
        ]
        flaky_tests = [
            s for s in runner.test_summaries.values() if s.final_status == "flaky"
        ]
        failed_tests = [
            s for s in runner.test_summaries.values() if s.final_status == "failed"
        ]

        report = [
            "=" * 80,
            "AUTOMATED TEST SUITE REPORT",
            "=" * 46,
            f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Ended: {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Duration: {total_duration}",
            "",
            "SUMMARY",
            "-" * 46,
            f"Total Tests: {len(runner.test_summaries)}",
            f"Passed: {len(passed_tests)}",
            f"Flaky (eventually passed): {len(flaky_tests)}",
            f"Failed: {len(failed_tests)}",
        ]

        # Summary stats
        success_rate = (
            (len(passed_tests) + len(flaky_tests)) / max(1, len(runner.test_summaries))
        ) * 100
        report.append(f"Success Rate: {success_rate:.1f}%")
        report.append("")

        # Flaky tests (important to highlight)
        if flaky_tests:
            report.append("FLAKY TESTS (Failed initially but passed on retry)")
            report.append("-" * 60)
            for summary in flaky_tests:
                successful_attempt = next(
                    (r for r in summary.retry_results if r.success), None
                )
                if successful_attempt:
                    report.append(f"  ✓ {summary.test_name}")
                    report.append(
                        f"    Passed on attempt {successful_attempt.attempt} ({successful_attempt.attempt_type})"
                    )
                    report.append(f"    Total attempts: {summary.total_attempts}")

                    # Add runtime information
                    if summary.test_name in self.state_manager.runtime_history:
                        history = self.state_manager.runtime_history[summary.test_name]
                        avg_runtime = history.get_average_runtime()
                        runtime_info = f"    Runtime: {self.format_duration(successful_attempt.duration)} (avg: {self.format_duration(avg_runtime)})"
                        report.append(runtime_info)
                    else:
                        report.append(
                            f"    Runtime: {self.format_duration(successful_attempt.duration)}"
                        )
                    report.append("")

        # Failed tests
        if failed_tests:
            report.append("FAILED TESTS")
            report.append("-" * 46)
            for summary in failed_tests:
                report.append(f"  ✗ {summary.test_name}")
                report.append(f"    Total attempts: {summary.total_attempts}")
                if summary.final_namespace:
                    report.append(
                        f"    Namespace kept for debugging: {summary.final_namespace}"
                    )

                # Add runtime information
                if summary.test_name in self.state_manager.runtime_history:
                    history = self.state_manager.runtime_history[summary.test_name]
                    avg_runtime = history.get_average_runtime()
                    runtime_info = f"    Average runtime: {self.format_duration(avg_runtime)} (from {len(history.runtimes)} runs)"
                    report.append(runtime_info)

                if summary.retry_results:
                    last_error = summary.retry_results[-1].failed_step
                    if last_error:
                        report.append(f"    Last error: {last_error[:100]}...")
                report.append("")

        # Runtime statistics
        self._add_runtime_statistics_to_report(report)

        # Configuration used
        report.append("CONFIGURATION")
        report.append("-" * 46)
        report.append(f"Parallel: {runner.args.parallel}")
        report.append(f"Parallel retry attempts: {runner.args.attempts_parallel}")
        report.append(f"Serial retry attempts: {runner.args.attempts_serial}")
        report.append(
            f"Delete failed namespaces: {runner.args.delete_failed_namespaces}"
        )
        report.append(f"Virtualenv: {runner.args.venv or 'None'}")
        report.append("")

        return "\n".join(report)

    def _add_runtime_statistics_to_report(self, report: List[str]) -> None:
        """Add runtime statistics section to the report."""
        if not self.state_manager.runtime_history:
            return

        report.append("RUNTIME STATISTICS")
        report.append("-" * 46)

        # Overall statistics
        all_runtimes = []
        for history in self.state_manager.runtime_history.values():
            all_runtimes.extend(history.runtimes)

        if all_runtimes:
            avg_overall = statistics.mean(all_runtimes)
            median_overall = statistics.median(all_runtimes)
            min_overall = min(all_runtimes)
            max_overall = max(all_runtimes)

            report.append(f"Total test runs recorded: {len(all_runtimes)}")
            report.append(
                f"Overall average runtime: {self.format_duration(avg_overall)}"
            )
            report.append(
                f"Overall median runtime: {self.format_duration(median_overall)}"
            )
            report.append(f"Fastest test run: {self.format_duration(min_overall)}")
            report.append(f"Slowest test run: {self.format_duration(max_overall)}")

            # Top 3 slowest and fastest tests
            test_averages = [
                (name, history.get_average_runtime())
                for name, history in self.state_manager.runtime_history.items()
            ]
            test_averages.sort(key=lambda x: x[1], reverse=True)

            if test_averages:
                report.append("")
                report.append("Slowest tests (by average):")
                for name, avg_time in test_averages[:3]:
                    report.append(f"  {name}: {self.format_duration(avg_time)}")

                report.append("")
                report.append("Fastest tests (by average):")
                for name, avg_time in test_averages[-3:]:
                    report.append(f"  {name}: {self.format_duration(avg_time)}")

        report.append("")

    def save_detailed_json_report(self, runner, start_time: datetime) -> None:
        """Save detailed results as JSON for further analysis."""
        report_data = {
            "start_time": start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "configuration": self.state_manager.build_configuration_dict(runner.args),
            "test_summaries": {
                name: asdict(summary) for name, summary in runner.test_summaries.items()
            },
            "all_results": [asdict(result) for result in runner.results],
            "runtime_history": {
                name: asdict(history)
                for name, history in self.state_manager.runtime_history.items()
            },
        }

        json_file = self.output_dir / "detailed_report.json"
        success = self.state_manager.safe_json_operation("save", json_file, report_data)
        if success:
            print(f"Detailed JSON report saved to: {json_file}")

    def generate_and_save_final_report(self, runner, start_time: datetime) -> None:
        """Generate and save the final report."""
        print("\n📊 Generating final report...")

        # Generate and print report
        report = self.generate_comprehensive_report(runner, start_time)
        print(report)

        # Save text report
        report_file = self.output_dir / "test_report.txt"
        try:
            with open(report_file, "w") as file_handle:
                file_handle.write(report)
            print(f"Text report saved to: {report_file}")
        except Exception as exception:
            print(f"⚠️ Failed to save text report: {exception}")

        # Save JSON report
        self.save_detailed_json_report(runner, start_time)


class AutoRetryTestRunner:
    """Main test runner with intelligent retry logic."""

    def __init__(self, args):
        self.args = args
        self.results: List[TestResult] = []
        self.test_summaries: Dict[str, TestSummary] = {}
        self.start_time = datetime.now()
        self.output_dir = Path(args.output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Create subdirectories for organization
        (self.output_dir / "retries").mkdir(exist_ok=True)

        # State management
        self.failed_tests: List[str] = []
        self.parallel_attempts_done = 0
        self.serial_attempts_done: Dict[str, int] = {}
        self.initial_result_log: Optional[str] = None

        # Initialize components
        self.state_manager = StateManager(self.output_dir)
        self.test_executor = TestExecutor(args, self.state_manager)
        self.report_generator = ReportGenerator(self.output_dir, self.state_manager)

        # Load runtime history
        self.state_manager.load_runtime_history()

    def retry_tests_in_parallel(
        self, test_names: List[str], max_attempts: int, serial_tests_follow: bool = True
    ) -> Dict[str, List[TestResult]]:
        """Retry multiple tests in parallel, with multiple attempts per test."""
        if max_attempts <= 0 or not test_names:
            return {test_name: [] for test_name in test_names}

        print(
            f"\n=== Parallel retries for {len(test_names)} tests (up to {max_attempts} attempts each) ==="
        )

        all_results = {test_name: [] for test_name in test_names}

        for attempt in range(1, max_attempts + 1):
            print(f"\n--- Parallel attempt {attempt} ---")

            # Only retry tests that haven't passed yet
            tests_to_retry = [
                test_name
                for test_name in test_names
                if not any(result.success for result in all_results[test_name])
            ]

            if not tests_to_retry:
                print("All tests have passed, no more retries needed!")
                break

            # Determine if this is the last attempt and no serial tests follow
            is_last_attempt = attempt == max_attempts
            use_skip_delete = (
                not self.args.delete_failed_namespaces
                and not serial_tests_follow
                and is_last_attempt
            )

            # Run failed tests in parallel, respecting the parallel limit
            max_parallel = min(len(tests_to_retry), self.args.parallel)
            print(
                f"Retrying {len(tests_to_retry)} tests in parallel (max {max_parallel} at once)..."
            )

            # Execute tests in parallel
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                future_to_test = {
                    executor.submit(
                        self.test_executor.run_single_test_suite,
                        self.output_dir,
                        test_name,
                        use_skip_delete,
                        attempt,
                        "parallel",
                    ): test_name
                    for test_name in tests_to_retry
                }

                for future in as_completed(future_to_test):
                    test_name = future_to_test[future]
                    try:
                        result = future.result()
                        all_results[test_name].append(result)
                        self.results.append(result)

                        if result.success:
                            print(f"  ✓ {test_name} PASSED (attempt {attempt})")
                        else:
                            print(f"  ✗ {test_name} FAILED (attempt {attempt})")

                    except Exception as exception:
                        print(f"  ✗ {test_name} ERROR (attempt {attempt}): {exception}")

            # Update parallel attempts counter
            self.parallel_attempts_done = attempt

            # Note: Namespace cleanup handled per-test based on skip_delete flag

        return all_results

    def retry_test_serially(
        self,
        test_name: str,
        max_attempts: int,
        current_test: int = 1,
        total_tests: int = 1,
    ) -> List[TestResult]:
        """Retry a test multiple times serially."""
        if max_attempts <= 0:
            return []

        print(
            f"\n=== Serial retries for {test_name} (up to {max_attempts} attempts) - Test {current_test}/{total_tests} failing ==="
        )

        results = []
        for attempt in range(1, max_attempts + 1):
            # Only use skip-delete on the last attempt
            is_last_attempt = attempt == max_attempts
            use_skip_delete = not self.args.delete_failed_namespaces and is_last_attempt

            result = self.test_executor.run_single_test_suite(
                self.output_dir,
                test_name=test_name,
                skip_delete=use_skip_delete,
                attempt=attempt,
                attempt_type="serial",
            )
            results.append(result)
            self.results.append(result)

            # Save state after each test completion
            self.state_manager.save_state(self)

            if result.success:
                print(f"  ✓ Serial attempt {attempt} PASSED")
                # Update serial attempts counter
                self.serial_attempts_done[test_name] = attempt
                break
            else:
                print(f"  ✗ Serial attempt {attempt} FAILED")
                # Update serial attempts counter
                self.serial_attempts_done[test_name] = attempt

        return results

    def create_test_summary(
        self,
        test_name: str,
        initial_result: TestResult,
        retry_results: List[TestResult],
    ) -> TestSummary:
        """Process and summarize all results for a single test."""
        # Determine final status
        if initial_result.success:
            final_status = "passed"
        elif any(result.success for result in retry_results):
            final_status = "flaky"
        else:
            final_status = "failed"

        # Find the last namespace for failed tests (only if keeping failed namespaces)
        final_namespace = None
        if final_status == "failed" and not self.args.delete_failed_namespaces:
            # Keep the last failed attempt's namespace
            for result in reversed(retry_results):
                if result.namespace:
                    final_namespace = result.namespace
                    break

        summary = TestSummary(
            test_name=test_name,
            initial_result=initial_result,
            retry_results=retry_results,
            final_status=final_status,
            total_attempts=1 + len(retry_results),
            final_namespace=final_namespace,
        )

        self.test_summaries[test_name] = summary
        return summary

    def initialize_run_mode(self) -> bool:
        """Initialize the test run based on mode (resume, rerun, or fresh)."""
        # Check if we're resuming from a previous state
        if self.failed_tests:  # This will be populated if we loaded state
            print("Resuming from previous state...")
            print(f"Found {len(self.failed_tests)} failed tests to continue with:")
            for i, test in enumerate(self.failed_tests, 1):
                print(f"  {i}. {test}")
            return True

        # Check if we're rerunning only failed tests
        elif self.args.rerun_failed:
            print("Rerunning only previously failed tests...")
            return self._load_failed_tests_for_rerun()

        else:
            # Run initial test suite
            return self._run_initial_test_suite()

    def _load_failed_tests_for_rerun(self) -> bool:
        """Load failed tests for rerun mode."""
        # Load failed tests from the specified state file
        failed_tests = self.state_manager.load_failed_tests_from_state(
            Path(self.args.rerun_failed)
        )

        if not failed_tests:
            print("❌ No failed tests found to rerun")
            return False

        print(f"Found {len(failed_tests)} failed tests to rerun:")
        for i, test in enumerate(failed_tests, 1):
            print(f"  {i}. {test}")

        # Store failed tests for the retry process
        self.failed_tests = failed_tests
        return True

    def _run_initial_test_suite(self) -> bool:
        """Run the initial test suite."""
        print("\nStep 1: Running initial full test suite...")
        initial_result = self.test_executor.run_single_test_suite(
            self.output_dir, attempt_type="initial"
        )

        if initial_result.success:
            print("  All tests passed on initial run!")
            self.report_generator.generate_and_save_final_report(self, self.start_time)
            return False  # No need to continue

        # Parse failed tests
        print("\nStep 2: Parsing failed tests...")
        failed_tests = self.test_executor.parse_failed_tests_from_log(
            Path(initial_result.log_file)
        )

        if not failed_tests:
            print("  No failed tests found in output but run-tests exited with code 1")
            print(
                "  This indicates an infrastructure or setup issue that prevents tests from running"
            )
            print(
                "  Check the log file for connection errors, missing dependencies, or cluster issues"
            )
            print(f"  Log file: {initial_result.log_file}")
            return False

        print(f"  Found {len(failed_tests)} failed tests:")
        for i, test in enumerate(failed_tests, 1):
            print(f"    {i}. {test}")

        # Store failed tests and initial result log for state persistence
        self.failed_tests = failed_tests
        self.initial_result_log = initial_result.log_file
        return True

    def execute_parallel_retries(self) -> Dict[str, List[TestResult]]:
        """Execute parallel retry attempts for failed tests."""
        all_parallel_results = {}

        if (
            self.args.attempts_parallel > 0
            and self.parallel_attempts_done < self.args.attempts_parallel
        ):
            # Determine which tests still need parallel retries
            tests_needing_parallel = []
            for test_name in self.failed_tests:
                # Check if this test has already passed in previous attempts
                if test_name in self.test_summaries:
                    summary = self.test_summaries[test_name]
                    if summary.final_status in ["passed", "flaky"]:
                        continue  # Skip tests that already passed

                tests_needing_parallel.append(test_name)

            if tests_needing_parallel:
                remaining_parallel_attempts = (
                    self.args.attempts_parallel - self.parallel_attempts_done
                )

                # Determine if serial tests will follow after parallel attempts
                serial_tests_will_follow = self.args.attempts_serial > 0
                all_parallel_results = self.retry_tests_in_parallel(
                    tests_needing_parallel,
                    remaining_parallel_attempts,
                    serial_tests_will_follow,
                )
            else:
                print("All tests have completed their parallel retry attempts.")

        return all_parallel_results

    def execute_serial_retries(
        self, all_parallel_results: Dict[str, List[TestResult]]
    ) -> Dict[str, List[TestResult]]:
        """Execute serial retry attempts for tests that failed parallel retries."""
        # Determine tests that still need serial retries
        tests_still_failing = []
        for test_name in self.failed_tests:
            # Check if test has already passed
            if test_name in self.test_summaries:
                summary = self.test_summaries[test_name]
                if summary.final_status in ["passed", "flaky"]:
                    continue  # Skip tests that already passed

            # Check if test failed all parallel attempts
            parallel_results = all_parallel_results.get(test_name, [])
            if not any(result.success for result in parallel_results):
                tests_still_failing.append(test_name)

        print(
            f"\n{len(tests_still_failing)} tests still failing after parallel retries, starting serial retries..."
        )

        all_serial_results = {}
        if self.args.attempts_serial > 0 and tests_still_failing:
            for i, test_name in enumerate(tests_still_failing, 1):
                # Check how many serial attempts have already been done for this test
                attempts_done = self.serial_attempts_done.get(test_name, 0)
                remaining_attempts = self.args.attempts_serial - attempts_done

                if remaining_attempts > 0:
                    serial_results = self.retry_test_serially(
                        test_name,
                        remaining_attempts,
                        current_test=i,
                        total_tests=len(tests_still_failing),
                    )
                    all_serial_results[test_name] = serial_results
                else:
                    print(
                        f"Test {test_name} has already completed all serial attempts ({attempts_done}/{self.args.attempts_serial})"
                    )

        return all_serial_results

    def process_all_test_results(
        self, all_parallel_results: dict, all_serial_results: dict
    ) -> None:
        """Process and summarize results for all tests."""
        for test_name in self.failed_tests:
            # Skip tests that have already been processed (when resuming)
            if test_name in self.test_summaries:
                continue

            # Create initial result for this test (marked as failed)
            test_initial_result = TestResult(
                test_name=test_name,
                attempt=1,
                attempt_type="initial",
                success=False,
                duration=0.0,
                log_file=self.initial_result_log or "unknown",
            )

            # Combine all retry results
            retry_results = []
            retry_results.extend(all_parallel_results.get(test_name, []))
            retry_results.extend(all_serial_results.get(test_name, []))

            # Process results for this test
            self.create_test_summary(test_name, test_initial_result, retry_results)

    def execute_test_retries(self) -> bool:
        """Execute the test retry strategy."""
        print("\nStep 3: Retrying failed tests...")

        # Execute parallel retries
        all_parallel_results = self.execute_parallel_retries()

        # Execute serial retries
        all_serial_results = self.execute_serial_retries(all_parallel_results)

        # Process results for all tests
        self.process_all_test_results(all_parallel_results, all_serial_results)

        return True

    def finalize_and_report(self) -> int:
        """Generate final report and return appropriate exit code."""
        print("\nStep 4: Generating final report...")

        self.report_generator.generate_and_save_final_report(self, self.start_time)

        # Return appropriate exit code
        failed_count = len(
            [s for s in self.test_summaries.values() if s.final_status == "failed"]
        )
        return 0 if failed_count == 0 else 1

    def run(self) -> int:
        """Main execution method."""
        print("Starting Automated Test Suite with Retry Logic")
        print("=" * 46)

        # Initialize based on run mode
        if not self.initialize_run_mode():
            return 0  # Early exit (e.g., all tests passed initially)

        # Execute test retries
        if not self.execute_test_retries():
            return 1

        # Generate final report and return exit code
        return self.finalize_and_report()


def main():
    parser = argparse.ArgumentParser(
        description="Automated test suite runner with intelligent retry logic for flaky integration tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Basic test runner arguments
    parser.add_argument(
        "--parallel",
        type=int,
        default=TestConstants.DEFAULT_PARALLEL_WORKERS,
        help=f"Number of parallel tests for initial and later parallel run s (default: {TestConstants.DEFAULT_PARALLEL_WORKERS})",
    )
    parser.add_argument(
        "--venv", type=str, help="Path to virtualenv directory (e.g., ./venv)"
    )
    parser.add_argument(
        "--extra-args",
        nargs="*",
        default=[],
        help="Extra arguments to pass to scripts/run-tests",
    )

    # Retry strategy arguments
    parser.add_argument(
        "--attempts-parallel",
        type=int,
        default=1,
        help="Number of parallel retry attempts (default: 1)",
    )
    parser.add_argument(
        "--attempts-serial",
        type=int,
        default=2,
        help="Number of serial retry attempts (default: 2)",
    )

    # Namespace management arguments
    parser.add_argument(
        "--delete-failed-namespaces",
        action="store_true",
        help="Delete namespaces of failed tests (default: keep them for debugging)",
    )

    # Output arguments
    parser.add_argument(
        "--output-dir",
        type=str,
        default="test-results",
        help="Directory to save test results and logs (default: test-results)",
    )

    # Resume functionality
    parser.add_argument(
        "--resume",
        type=str,
        metavar="STATE_FILE",
        help="Resume from a previous state file (e.g., test-results/test_run_state.json)",
    )

    # Rerun functionality
    parser.add_argument(
        "--rerun-failed",
        type=str,
        metavar="STATE_FILE",
        help="Rerun only the previously failed tests from a state file (skips initial full suite run)",
    )

    args = parser.parse_args()

    # Validate argument combinations
    if args.resume and args.rerun_failed:
        print("❌ Cannot use --resume and --rerun-failed together")
        return 1

    # Create and run the test runner
    runner = AutoRetryTestRunner(args)

    # Handle resume functionality
    if args.resume:
        state_file = Path(args.resume)
        if not state_file.exists():
            print(f"❌ State file not found: {state_file}")
            return 1

        if not runner.state_manager.load_state(state_file, runner):
            print(f"❌ Failed to load state from: {state_file}")
            return 1

    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
