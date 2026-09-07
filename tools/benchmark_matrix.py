#!/usr/bin/env python3
"""Run reproducible, pressure-aware Prescient benchmark matrices.

The controller treats every requested process as required work. Reduced probes are
diagnostic only: they never replace a required process or enter final aggregates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


STATE_VERSION = 2
EVENTS = (
    "cycles:uk",
    "instructions:uk",
    "cycles:u",
    "instructions:u",
    "task-clock",
    "context-switches",
    "cpu-migrations",
)
INT_FIELDS = (
    "bytes",
    "producers",
    "consumers",
    "messages",
    "samples",
    "warmups",
    "delivered",
    "capacity",
    "aggregate_capacity",
    "batch",
    "producer_batch",
)
FLOAT_FIELDS = ("median_mps", "median_gbps", "min_mps", "max_mps")


class ConfigurationError(ValueError):
    """The benchmark specification is internally inconsistent."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if temporary.exists():
            temporary.unlink()


def command_version(command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
        )
        return completed.stdout.strip()
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"unavailable: {error}"


def read_cpu_pressure(path: Path) -> dict[str, float | int]:
    text = path.read_text(encoding="ascii")
    line = next((item for item in text.splitlines() if item.startswith("some ")), None)
    if line is None:
        raise RuntimeError(f"{path} has no CPU 'some' pressure line")
    values: dict[str, float | int] = {}
    for part in line.split()[1:]:
        name, raw = part.split("=", 1)
        values[name] = int(raw) if name == "total" else float(raw)
    for required in ("avg10", "avg60", "avg300", "total"):
        if required not in values:
            raise RuntimeError(f"{path} lacks {required}")
    return values


def pressure_fraction(before: dict[str, Any], after: dict[str, Any], elapsed: float) -> float:
    if elapsed <= 0:
        return math.inf
    delta = int(after["total"]) - int(before["total"])
    if delta < 0:
        return math.inf
    return delta / (elapsed * 1_000_000.0)


def balanced_orders(count: int, rounds: int) -> list[list[int]]:
    if count < 1 or rounds < 1:
        raise ConfigurationError("variant count and rounds must be positive")
    orders: list[list[int]] = []
    base = list(range(count))
    for round_index in range(rounds):
        group, phase = divmod(round_index, 4)
        offset = (group + (count // 2 if phase >= 2 else 0)) % count
        order = base[offset:] + base[:offset]
        if phase % 2 == 1:
            order.reverse()
        orders.append(order)
    return orders


def require_integer(value: Any, name: str, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigurationError(f"{name} must be an integer >= {minimum}")
    return value


def validate_spec(spec: dict[str, Any], spec_path: Path) -> dict[str, Any]:
    required = ("name", "binary", "variants", "payloads", "shapes")
    missing = [name for name in required if name not in spec]
    if missing:
        raise ConfigurationError(f"missing spec fields: {', '.join(missing)}")
    allowed = {
        *required,
        "capacities",
        "batches",
        "samples",
        "warmups",
        "rounds",
        "timeout_seconds",
        "admission",
    }
    unknown = sorted(set(spec) - allowed)
    if unknown:
        raise ConfigurationError(f"unknown spec fields: {', '.join(unknown)}")
    if not isinstance(spec["name"], str) or not spec["name"].strip():
        raise ConfigurationError("name must be a non-empty string")

    binary = Path(spec["binary"])
    if not binary.is_absolute():
        binary = (spec_path.parent / binary).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ConfigurationError(f"binary is not executable: {binary}")
    spec["binary"] = str(binary)

    spec.setdefault("capacities", [4096])
    spec.setdefault("batches", [64])
    spec.setdefault("samples", 5)
    spec.setdefault("warmups", 1)
    spec.setdefault("rounds", 4)
    spec.setdefault("timeout_seconds", 240)
    for field in ("samples", "rounds", "timeout_seconds"):
        spec[field] = require_integer(spec[field], field)
    for field in ("capacities", "batches"):
        values = spec[field]
        if not isinstance(values, list) or not values:
            raise ConfigurationError(f"{field} must be a non-empty list")
        for index, value in enumerate(values):
            require_integer(value, f"{field}[{index}]")
        if len(set(values)) != len(values):
            raise ConfigurationError(f"{field} contains duplicates")
    if spec["warmups"] != 1:
        raise ConfigurationError("payload_bench currently has exactly one warmup")

    variants = spec["variants"]
    if not isinstance(variants, list) or not variants:
        raise ConfigurationError("variants must be a non-empty list")
    variant_ids: set[str] = set()
    for index, variant in enumerate(variants):
        if not isinstance(variant, dict):
            raise ConfigurationError(f"variants[{index}] must be an object")
        for field in ("id", "label", "mode", "producer_batch"):
            if field not in variant:
                raise ConfigurationError(f"variants[{index}] lacks {field}")
        identifier = variant["id"]
        if not isinstance(identifier, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", identifier):
            raise ConfigurationError(f"invalid variant id: {identifier!r}")
        if identifier in variant_ids:
            raise ConfigurationError(f"duplicate variant id: {identifier}")
        variant_ids.add(identifier)
        variant_binary = Path(variant.get("binary", spec["binary"]))
        if not variant_binary.is_absolute():
            variant_binary = (spec_path.parent / variant_binary).resolve()
        if not variant_binary.is_file() or not os.access(variant_binary, os.X_OK):
            raise ConfigurationError(
                f"{identifier}.binary is not executable: {variant_binary}"
            )
        variant["binary"] = str(variant_binary)
        if variant["producer_batch"] not in ("scalar", "batch"):
            raise ConfigurationError(
                f"{identifier}.producer_batch must be 'scalar' or 'batch'"
            )
        capacity_mode = variant.get("capacity_mode", "explicit")
        if capacity_mode not in ("explicit", "automatic"):
            raise ConfigurationError(
                f"{identifier}.capacity_mode must be explicit or automatic"
            )
        side = variant.get("side")
        if side not in (None, "baseline", "candidate"):
            raise ConfigurationError(f"{identifier}.side must be baseline or candidate")
        if "pair" in variant and (
            not isinstance(variant["pair"], str) or not variant["pair"].strip()
        ):
            raise ConfigurationError(f"{identifier}.pair must be a non-empty string")
        for field in ("include_shapes", "include_batches", "include_capacities", "include_payloads"):
            if field in variant and (
                not isinstance(variant[field], list) or not variant[field]
            ):
                raise ConfigurationError(f"{identifier}.{field} must be a non-empty list")

    payloads = spec["payloads"]
    if not isinstance(payloads, list) or not payloads:
        raise ConfigurationError("payloads must be a non-empty list")
    seen_payloads: set[int] = set()
    for index, payload in enumerate(payloads):
        if not isinstance(payload, dict):
            raise ConfigurationError(f"payloads[{index}] must be an object")
        size = require_integer(payload.get("bytes"), f"payloads[{index}].bytes")
        require_integer(
            payload.get("messages_per_sample"),
            f"payloads[{index}].messages_per_sample",
            minimum=2,
        )
        if size in seen_payloads:
            raise ConfigurationError(f"duplicate payload size: {size}")
        seen_payloads.add(size)
        if "include_capacities" in payload:
            selected = payload["include_capacities"]
            if (
                not isinstance(selected, list)
                or not selected
                or any(capacity not in spec["capacities"] for capacity in selected)
                or len(set(selected)) != len(selected)
            ):
                raise ConfigurationError(
                    f"payloads[{index}].include_capacities must select configured capacities"
                )

    shapes = spec["shapes"]
    if not isinstance(shapes, list) or not shapes:
        raise ConfigurationError("shapes must be a non-empty list")
    shape_ids: set[str] = set()
    for index, shape in enumerate(shapes):
        if not isinstance(shape, dict):
            raise ConfigurationError(f"shapes[{index}] must be an object")
        for field in ("id", "producers", "consumers", "cpus"):
            if field not in shape:
                raise ConfigurationError(f"shapes[{index}] lacks {field}")
        identifier = shape["id"]
        if not isinstance(identifier, str) or not re.fullmatch(
            r"[A-Za-z0-9_.-]+", identifier
        ):
            raise ConfigurationError(f"invalid shape id: {identifier!r}")
        if identifier in shape_ids:
            raise ConfigurationError(f"duplicate shape id: {identifier}")
        shape_ids.add(identifier)
        producers = require_integer(shape["producers"], f"{identifier}.producers")
        consumers = require_integer(shape["consumers"], f"{identifier}.consumers")
        cpus = shape["cpus"]
        if (
            not isinstance(cpus, list)
            or len(cpus) != producers + consumers
            or any(isinstance(cpu, bool) or not isinstance(cpu, int) or cpu < 0 for cpu in cpus)
            or len(set(cpus)) != len(cpus)
        ):
            raise ConfigurationError(
                f"{identifier}.cpus must contain {producers + consumers} distinct CPU ids"
            )

    for payload in payloads:
        total = payload["messages_per_sample"]
        for shape in shapes:
            producers = shape["producers"]
            if total % producers != 0 or total // producers < 2:
                raise ConfigurationError(
                    f"payload {payload['bytes']} messages_per_sample must divide exactly "
                    f"across {shape['id']} producers"
                )

    configured_batches = set(spec["batches"])
    configured_capacities = set(spec["capacities"])
    configured_payloads = {payload["bytes"] for payload in payloads}
    for variant in variants:
        include_shapes = variant.get("include_shapes", list(shape_ids))
        if (
            any(not isinstance(shape, str) for shape in include_shapes)
            or len(set(include_shapes)) != len(include_shapes)
            or any(shape not in shape_ids for shape in include_shapes)
        ):
            raise ConfigurationError(f"{variant['id']}.include_shapes names an unknown shape")
        include_batches = variant.get("include_batches", spec["batches"])
        if (
            any(isinstance(batch, bool) or not isinstance(batch, int) for batch in include_batches)
            or len(set(include_batches)) != len(include_batches)
            or any(batch not in configured_batches for batch in include_batches)
        ):
            raise ConfigurationError(
                f"{variant['id']}.include_batches must select configured batches"
            )
        include_capacities = variant.get("include_capacities", spec["capacities"])
        if (
            any(
                isinstance(capacity, bool) or not isinstance(capacity, int)
                for capacity in include_capacities
            )
            or len(set(include_capacities)) != len(include_capacities)
            or any(capacity not in configured_capacities for capacity in include_capacities)
        ):
            raise ConfigurationError(
                f"{variant['id']}.include_capacities must select configured capacities"
            )
        include_payloads = variant.get("include_payloads", list(configured_payloads))
        if (
            any(isinstance(size, bool) or not isinstance(size, int) for size in include_payloads)
            or len(set(include_payloads)) != len(include_payloads)
            or any(size not in configured_payloads for size in include_payloads)
        ):
            raise ConfigurationError(
                f"{variant['id']}.include_payloads must select configured payload sizes"
            )
    admission = spec.setdefault("admission", {})
    defaults = {
        "avg10_max": 2.0,
        "run_psi_warn": 0.05,
        "poll_seconds": 2.0,
        "wait_before_probe_seconds": 30.0,
        "max_wait_seconds": 600.0,
        "retry_cooldown_seconds": 5.0,
        "max_retries": 2,
        "probe_avg10_max": 10.0,
        "probe_scale": 0.05,
        "probe_samples": 1,
        "probe_min_per_producer": 1000,
    }
    unknown_admission = sorted(set(admission) - set(defaults))
    if unknown_admission:
        raise ConfigurationError(
            f"unknown admission fields: {', '.join(unknown_admission)}"
        )
    for name, default in defaults.items():
        admission.setdefault(name, default)
    for name in (
        "avg10_max",
        "run_psi_warn",
        "poll_seconds",
        "wait_before_probe_seconds",
        "max_wait_seconds",
        "retry_cooldown_seconds",
        "probe_avg10_max",
    ):
        value = admission[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            raise ConfigurationError(f"admission.{name} must be non-negative")
        admission[name] = float(value)
    if not 0 < admission["probe_scale"] < 1:
        raise ConfigurationError("admission.probe_scale must be between zero and one")
    admission["max_retries"] = require_integer(
        admission["max_retries"], "admission.max_retries", minimum=0
    )
    admission["probe_samples"] = require_integer(
        admission["probe_samples"], "admission.probe_samples"
    )
    admission["probe_min_per_producer"] = require_integer(
        admission["probe_min_per_producer"], "admission.probe_min_per_producer"
    )
    if admission["probe_avg10_max"] < admission["avg10_max"]:
        raise ConfigurationError("probe_avg10_max cannot be below avg10_max")
    if admission["max_wait_seconds"] < admission["wait_before_probe_seconds"]:
        raise ConfigurationError("max_wait_seconds cannot be below wait_before_probe_seconds")
    if admission["poll_seconds"] <= 0:
        raise ConfigurationError("admission.poll_seconds must be greater than zero")

    pairs: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for variant in variants:
        if "pair" in variant:
            if "side" not in variant:
                raise ConfigurationError(f"{variant['id']} has pair without side")
            pairs[variant["pair"]][variant["side"]] += 1
    for pair, sides in pairs.items():
        if sides != {"baseline": 1, "candidate": 1}:
            raise ConfigurationError(f"pair {pair!r} needs one baseline and one candidate")
    return spec


def variant_applies(
    variant: dict[str, Any], payload_bytes: int, shape_id: str, capacity: int, batch: int
) -> bool:
    return (
        payload_bytes in variant.get("include_payloads", [payload_bytes])
        and shape_id in variant.get("include_shapes", [shape_id])
        and capacity in variant.get("include_capacities", [capacity])
        and batch in variant.get("include_batches", [batch])
    )


def configured_producer_batch(variant: dict[str, Any], batch: int) -> int:
    return 1 if variant["producer_batch"] == "scalar" else batch


def payload_capacities(spec: dict[str, Any], payload: dict[str, Any]) -> list[int]:
    return payload.get("include_capacities", spec["capacities"])


def binary_hashes(spec: dict[str, Any]) -> dict[str, str]:
    paths = sorted({variant["binary"] for variant in spec["variants"]})
    return {path: sha256_file(Path(path)) for path in paths}


def make_jobs(spec: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for payload in spec["payloads"]:
        for shape in spec["shapes"]:
            for capacity in payload_capacities(spec, payload):
                for batch in spec["batches"]:
                    variants = [
                        variant
                        for variant in spec["variants"]
                        if variant_applies(variant, payload["bytes"], shape["id"], capacity, batch)
                    ]
                    if not variants:
                        continue
                    orders = balanced_orders(len(variants), spec["rounds"])
                    for round_index, order in enumerate(orders):
                        for position, variant_index in enumerate(order):
                            variant = variants[variant_index]
                            identifier = (
                                f"b{payload['bytes']}-{shape['id']}-c{capacity}-n{batch}-"
                                f"r{round_index:03d}-p{position:03d}-{variant['id']}"
                            )
                            jobs.append(
                                {
                                    "id": identifier,
                                    "payload_bytes": payload["bytes"],
                                    "messages_per_sample": payload["messages_per_sample"],
                                    "per_producer": payload["messages_per_sample"]
                                    // shape["producers"],
                                    "shape_id": shape["id"],
                                    "producers": shape["producers"],
                                    "consumers": shape["consumers"],
                                    "cpus": shape["cpus"],
                                    "capacity": capacity,
                                    "batch": batch,
                                    "variant_id": variant["id"],
                                    "round": round_index,
                                    "position": position,
                                }
                            )
    return jobs


def parse_header(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in line.split(";"):
        if "=" not in part:
            raise ValueError(f"malformed header field: {part!r}")
        name, value = part.split("=", 1)
        if not name or name in fields:
            raise ValueError(f"duplicate or empty header field: {name!r}")
        fields[name] = value
    return fields


def parse_payload_output(
    raw: str,
    spec: dict[str, Any],
    job: dict[str, Any],
    *,
    per_producer: int,
    samples: int,
) -> tuple[dict[str, Any] | None, list[str]]:
    reasons: list[str] = []
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    headers = [line for line in lines if line.startswith("impl=")]
    if len(headers) != 1:
        return None, [f"expected one impl header, found {len(headers)}"]
    try:
        header = parse_header(headers[0])
    except ValueError as error:
        return None, [str(error)]

    variant = next(item for item in spec["variants"] if item["id"] == job["variant_id"])
    messages = job["producers"] * per_producer
    delivered = messages * (samples + spec["warmups"])
    expected = {
        "impl": variant["mode"],
        "bytes": job["payload_bytes"],
        "producers": job["producers"],
        "consumers": job["consumers"],
        "messages": messages,
        "samples": samples,
        "warmups": spec["warmups"],
        "delivered": delivered,
        "capacity": job["capacity"],
        "capacity_mode": variant.get("capacity_mode", "explicit"),
        "aggregate_capacity": job["capacity"] * job["producers"],
        "batch": job["batch"],
        "producer_batch": configured_producer_batch(variant, job["batch"]),
        "worker_cpus": ",".join(str(cpu) for cpu in job["cpus"]),
        "worker_metrics": "off",
    }
    for name, wanted in expected.items():
        if header.get(name) != str(wanted):
            reasons.append(f"header {name}={header.get(name)!r}, expected {wanted!r}")
    for name in INT_FIELDS:
        try:
            if int(header[name]) < 0:
                raise ValueError
        except (KeyError, ValueError):
            reasons.append(f"invalid integer header {name}")
    for name in FLOAT_FIELDS:
        try:
            value = float(header[name])
            if not math.isfinite(value) or value <= 0:
                raise ValueError
        except (KeyError, ValueError):
            reasons.append(f"invalid positive float header {name}")
    try:
        sample_values = [float(value) for value in header["sample_mps"].split(",")]
        if len(sample_values) != samples or any(
            not math.isfinite(value) or value <= 0 for value in sample_values
        ):
            raise ValueError
        reported_median = float(header["median_mps"])
        if not math.isclose(
            statistics.median(sample_values), reported_median, rel_tol=1e-6, abs_tol=1e-6
        ):
            reasons.append("reported median_mps does not match samples")
        if not math.isclose(
            min(sample_values), float(header["min_mps"]), rel_tol=1e-6, abs_tol=1e-6
        ):
            reasons.append("reported min_mps does not match samples")
        if not math.isclose(
            max(sample_values), float(header["max_mps"]), rel_tol=1e-6, abs_tol=1e-6
        ):
            reasons.append("reported max_mps does not match samples")
    except (KeyError, ValueError):
        sample_values = []
        reasons.append("invalid sample_mps")

    events: dict[str, float] = {}
    coverage: dict[str, float] = {}
    for line in lines:
        fields = line.split(";")
        if len(fields) < 5 or fields[2] not in EVENTS:
            continue
        event = fields[2]
        if event in events:
            reasons.append(f"duplicate perf event {event}")
            continue
        try:
            value = float(fields[0])
            scheduled = float(fields[4])
            if not math.isfinite(value) or value < 0:
                raise ValueError
            if not math.isclose(scheduled, 100.0, abs_tol=1e-9):
                reasons.append(f"perf event {event} scheduled {scheduled}%")
            events[event] = value
            coverage[event] = scheduled
        except ValueError:
            reasons.append(f"invalid perf event {event}")
    missing_events = [event for event in EVENTS if event not in events]
    if missing_events:
        reasons.append(f"missing perf events: {', '.join(missing_events)}")
    if reasons:
        return None, reasons

    if events["instructions:u"] > events["instructions:uk"]:
        reasons.append("user instructions exceed user+kernel instructions")
    if events["cycles:u"] > events["cycles:uk"]:
        reasons.append("user cycles exceed user+kernel cycles")
    if events["cycles:uk"] <= 0 or events["instructions:uk"] <= 0:
        reasons.append("non-positive core counter")
    if reasons:
        return None, reasons

    metrics = {
        "header": header,
        "events": events,
        "coverage": coverage,
        "sample_mps": sample_values,
        "mps": float(header["median_mps"]),
        "ipm": events["instructions:uk"] / delivered,
        "cpm": events["cycles:uk"] / delivered,
        "ipc": events["instructions:uk"] / events["cycles:uk"],
        "user_ipm": events["instructions:u"] / delivered,
        "user_cpm": events["cycles:u"] / delivered,
        "user_ipc": events["instructions:u"] / events["cycles:u"],
        "kernel_ipm": (events["instructions:uk"] - events["instructions:u"]) / delivered,
        "kernel_cpm": (events["cycles:uk"] - events["cycles:u"]) / delivered,
        "cpu_ns_per_message": events["task-clock"] * 1_000_000.0 / delivered,
        "context_switches": int(events["context-switches"]),
        "cpu_migrations": int(events["cpu-migrations"]),
    }
    return metrics, []


def benchmark_command(
    spec: dict[str, Any], job: dict[str, Any], *, per_producer: int, samples: int
) -> list[str]:
    variant = next(item for item in spec["variants"] if item["id"] == job["variant_id"])
    return [
        "perf",
        "stat",
        "--no-big-num",
        "-x",
        ";",
        "-e",
        "{cycles:uk,instructions:uk,cycles:u,instructions:u}",
        "-e",
        "task-clock,context-switches,cpu-migrations",
        "--",
        variant["binary"],
        variant["mode"],
        str(job["payload_bytes"]),
        str(job["producers"]),
        str(job["consumers"]),
        str(per_producer),
        str(job["capacity"]),
        str(job["batch"]),
        str(samples),
        "--capacity-mode",
        variant.get("capacity_mode", "explicit"),
        "--cpus",
        ",".join(str(cpu) for cpu in job["cpus"]),
        "--worker-metrics",
        "off",
    ]


def execute_attempt(
    spec: dict[str, Any],
    job: dict[str, Any],
    pressure_path: Path,
    *,
    kind: str,
    per_producer: int,
    samples: int,
) -> dict[str, Any]:
    command = benchmark_command(spec, job, per_producer=per_producer, samples=samples)
    before = read_cpu_pressure(pressure_path)
    started_at = utc_now()
    started = time.monotonic()
    timed_out = False
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "LC_ALL": "C"},
        start_new_session=True,
    )
    try:
        try:
            stdout, stderr = process.communicate(timeout=spec["timeout_seconds"])
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()
        exit_code: int | None = process.returncode
        separator = "" if not stdout or stdout.endswith("\n") or not stderr else "\n"
        raw = stdout + separator + stderr
    finally:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
    elapsed = time.monotonic() - started
    after = read_cpu_pressure(pressure_path)
    measured_pressure = pressure_fraction(before, after, elapsed)
    metrics, reasons = parse_payload_output(
        raw, spec, job, per_producer=per_producer, samples=samples
    )
    if timed_out:
        reasons.insert(0, f"timed out after {spec['timeout_seconds']} seconds")
    elif exit_code != 0:
        reasons.insert(0, f"process exited {exit_code}")
    warnings = []
    if measured_pressure > spec["admission"]["run_psi_warn"]:
        warnings.append(
            f"run CPU pressure {measured_pressure:.3%} exceeds warning level "
            f"{spec['admission']['run_psi_warn']:.3%}"
        )
    return {
        "sequence": None,
        "kind": kind,
        "job_id": job["id"],
        "started_at": started_at,
        "finished_at": utc_now(),
        "elapsed_seconds": elapsed,
        "command": command,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "per_producer": per_producer,
        "samples": samples,
        "pressure_before": before,
        "pressure_after": after,
        "attempt_pressure": measured_pressure,
        "accepted": kind == "required" and not reasons,
        "reasons": reasons,
        "warnings": warnings,
        "metrics": metrics,
        "stdout": stdout,
        "stderr": stderr,
        "raw": raw,
    }


def median_field(attempts: Iterable[dict[str, Any]], name: str) -> float:
    return statistics.median(attempt["metrics"][name] for attempt in attempts)


def summarize(spec: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    accepted_by_cell: dict[
        tuple[int, str, int, int, str], list[dict[str, Any]]
    ] = defaultdict(list)
    jobs = {job["id"]: job for job in state["jobs"]}
    for attempt in state["attempts"]:
        if not attempt["accepted"]:
            continue
        job = jobs[attempt["job_id"]]
        key = (
            job["payload_bytes"],
            job["shape_id"],
            job["capacity"],
            job["batch"],
            job["variant_id"],
        )
        accepted_by_cell[key].append(attempt)

    cells: list[dict[str, Any]] = []
    for payload in spec["payloads"]:
        for shape in spec["shapes"]:
            for capacity in payload_capacities(spec, payload):
                for batch in spec["batches"]:
                    for variant in spec["variants"]:
                        if not variant_applies(variant, payload["bytes"], shape["id"], capacity, batch):
                            continue
                        key = (
                            payload["bytes"],
                            shape["id"],
                            capacity,
                            batch,
                            variant["id"],
                        )
                        attempts = accepted_by_cell[key]
                        cell: dict[str, Any] = {
                            "payload_bytes": payload["bytes"],
                            "shape_id": shape["id"],
                            "capacity": capacity,
                            "batch": batch,
                            "variant_id": variant["id"],
                            "label": variant["label"],
                            "accepted_processes": len(attempts),
                            "required_processes": spec["rounds"],
                        }
                        if attempts:
                            mps_values = [
                                attempt["metrics"]["mps"] for attempt in attempts
                            ]
                            for name in (
                                "mps",
                                "ipm",
                                "ipc",
                                "cpm",
                                "user_ipm",
                                "user_cpm",
                                "user_ipc",
                                "kernel_ipm",
                                "kernel_cpm",
                                "cpu_ns_per_message",
                                "context_switches",
                                "cpu_migrations",
                            ):
                                cell[name] = median_field(attempts, name)
                            cell["mps_min"] = min(mps_values)
                            cell["mps_max"] = max(mps_values)
                            cell["attempt_pressure_max"] = max(
                                attempt["attempt_pressure"] for attempt in attempts
                            )
                        cells.append(cell)

    leaders: list[dict[str, Any]] = []
    for payload in spec["payloads"]:
        for shape in spec["shapes"]:
            for capacity in payload_capacities(spec, payload):
                for batch in spec["batches"]:
                    complete = [
                        cell
                        for cell in cells
                        if cell["payload_bytes"] == payload["bytes"]
                        and cell["shape_id"] == shape["id"]
                        and cell["capacity"] == capacity
                        and cell["batch"] == batch
                        and cell["accepted_processes"] == spec["rounds"]
                    ]
                    if not complete:
                        continue
                    pareto = [
                        cell
                        for cell in complete
                        if not any(
                            other is not cell
                            and other["ipm"] <= cell["ipm"]
                            and other["ipc"] >= cell["ipc"]
                            and (
                                other["ipm"] < cell["ipm"]
                                or other["ipc"] > cell["ipc"]
                            )
                            for other in complete
                        )
                    ]
                    leaders.append(
                        {
                            "payload_bytes": payload["bytes"],
                            "shape_id": shape["id"],
                            "capacity": capacity,
                            "batch": batch,
                            "throughput": max(complete, key=lambda cell: cell["mps"])[
                                "label"
                            ],
                            "instructions": min(complete, key=lambda cell: cell["ipm"])[
                                "label"
                            ],
                            "ipc": max(complete, key=lambda cell: cell["ipc"])["label"],
                            "cycles": min(complete, key=lambda cell: cell["cpm"])["label"],
                            "two_rule_pareto": [
                                cell["label"]
                                for cell in sorted(pareto, key=lambda cell: cell["ipm"])
                            ],
                        }
                    )

    by_cell = {
        (
            cell["payload_bytes"],
            cell["shape_id"],
            cell["capacity"],
            cell["batch"],
            cell["variant_id"],
        ): cell
        for cell in cells
    }
    comparisons: list[dict[str, Any]] = []
    pair_names = sorted(
        {variant.get("pair") for variant in spec["variants"] if variant.get("pair")}
    )
    for payload in spec["payloads"]:
        for shape in spec["shapes"]:
            for capacity in payload_capacities(spec, payload):
                for batch in spec["batches"]:
                    for pair in pair_names:
                        baseline_variant = next(
                            variant
                            for variant in spec["variants"]
                            if variant.get("pair") == pair
                            and variant.get("side") == "baseline"
                        )
                        candidate_variant = next(
                            variant
                            for variant in spec["variants"]
                            if variant.get("pair") == pair
                            and variant.get("side") == "candidate"
                        )
                        if not (
                            variant_applies(baseline_variant, payload["bytes"], shape["id"], capacity, batch)
                            and variant_applies(candidate_variant, payload["bytes"], shape["id"], capacity, batch)
                        ):
                            continue
                        baseline = by_cell[
                            (
                                payload["bytes"],
                                shape["id"],
                                capacity,
                                batch,
                                baseline_variant["id"],
                            )
                        ]
                        candidate = by_cell[
                            (
                                payload["bytes"],
                                shape["id"],
                                capacity,
                                batch,
                                candidate_variant["id"],
                            )
                        ]
                        comparison: dict[str, Any] = {
                            "payload_bytes": payload["bytes"],
                            "shape_id": shape["id"],
                            "capacity": capacity,
                            "batch": batch,
                            "pair": pair,
                            "baseline": baseline_variant["label"],
                            "candidate": candidate_variant["label"],
                            "complete": (
                                baseline["accepted_processes"] == spec["rounds"]
                                and candidate["accepted_processes"] == spec["rounds"]
                            ),
                        }
                        if comparison["complete"]:
                            comparison.update(
                                {
                                    "throughput_ratio": candidate["mps"]
                                    / baseline["mps"],
                                    "instructions_delta": candidate["ipm"]
                                    / baseline["ipm"]
                                    - 1.0,
                                    "ipc_delta": candidate["ipc"]
                                    / baseline["ipc"]
                                    - 1.0,
                                    "cycles_delta": candidate["cpm"]
                                    / baseline["cpm"]
                                    - 1.0,
                                }
                            )
                        comparisons.append(comparison)

    rejected = [
        attempt
        for attempt in state["attempts"]
        if attempt["kind"] == "required" and not attempt["accepted"]
    ]
    probes = [attempt for attempt in state["attempts"] if attempt["kind"] == "probe"]
    pressure_warnings = [
        attempt for attempt in state["attempts"] if attempt.get("warnings")
    ]
    admissions = [
        event for event in state["admission_events"] if event["kind"] == "admitted"
    ]
    return {
        "generated_at": utc_now(),
        "status": state["status"],
        "spec_sha256": state["spec_sha256"],
        "binaries": state["binaries"],
        "source_checkpoint": state["source_checkpoint"],
        "required_jobs": len(state["jobs"]),
        "accepted_jobs": len(state["accepted"]),
        "remaining_jobs": len(state["pending"]),
        "failed_jobs": len(state["failed"]),
        "rejected_attempts": len(rejected),
        "diagnostic_probes": len(probes),
        "run_pressure_warnings": len(pressure_warnings),
        "admission_wait_events": sum(
            event["kind"] == "waiting" for event in state["admission_events"]
        ),
        "admission_wait_seconds": sum(
            float(event["waited_seconds"]) for event in admissions
        ),
        "pre_run_avg10_max": max(
            (float(event["pressure"]["avg10"]) for event in admissions), default=0.0
        ),
        "cells": cells,
        "leaders": leaders,
        "comparisons": comparisons,
    }


def percent(value: float) -> str:
    return f"{value * 100:+.1f}%"


def render_report(spec: dict[str, Any], state: dict[str, Any], summary: dict[str, Any]) -> str:
    lines = [
        f"# {spec['name']} benchmark matrix",
        "",
        f"Status: **{summary['status']}**  ",
        f"Generated: {summary['generated_at']}  ",
        f"Source checkpoint: `{summary['source_checkpoint']}`  ",
    ]
    for binary, digest in summary["binaries"].items():
        lines.append(f"Binary `{binary}` SHA256: `{digest}`  ")
    lines.extend(
        [
            f"Spec SHA256: `{summary['spec_sha256']}`",
            "",
            "## Progress",
            "",
            f"- Required jobs accepted: {summary['accepted_jobs']}/{summary['required_jobs']}.",
            f"- Required attempts rejected: {summary['rejected_attempts']}.",
            f"- Diagnostic reduced-work probes: {summary['diagnostic_probes']}.",
            f"- Runs above the reporting threshold: {summary['run_pressure_warnings']}.",
            f"- Failed jobs: {summary['failed_jobs']}; remaining queue: {summary['remaining_jobs']}.",
            f"- Required pre-run CPU PSI avg10 ceiling: {spec['admission']['avg10_max']:.2f}%; "
            f"run-pressure reporting level: {spec['admission']['run_psi_warn']:.2%}.",
            f"- Maximum admitted pre-run avg10: {summary['pre_run_avg10_max']:.2f}%; "
            f"wait events: {summary['admission_wait_events']}; "
            f"cumulative admission wait: {summary['admission_wait_seconds']:.1f}s.",
            "",
            "A diagnostic probe never satisfies a required job or contributes to an aggregate.",
            "",
            "## Accepted results",
            "",
        ]
    )
    for payload in spec["payloads"]:
        for shape in spec["shapes"]:
            for capacity in payload_capacities(spec, payload):
                for batch in spec["batches"]:
                    dimension_cells = [
                        cell
                        for cell in summary["cells"]
                        if cell["payload_bytes"] == payload["bytes"]
                        and cell["shape_id"] == shape["id"]
                        and cell["capacity"] == capacity
                        and cell["batch"] == batch
                    ]
                    if not dimension_cells:
                        continue
                    lines.extend(
                        [
                            (
                                f"### {payload['bytes']} B, {shape['id']}, capacity "
                                f"{capacity}, batch {batch}, work "
                                f"{payload['messages_per_sample']:,} total "
                                f"({payload['messages_per_sample'] // shape['producers']:,}"
                                "/producer)"
                            ),
                            "",
                            "| Variant | n | Mmsg/s [range] | instructions/msg | IPC | cycles/msg | max pressure |",
                            "|---|---:|---:|---:|---:|---:|---:|",
                        ]
                    )
                    for cell in dimension_cells:
                        if "mps" not in cell:
                            result = "—"
                            rest = ("—", "—", "—", "—")
                        else:
                            result = (
                                f"{cell['mps']:.3f} "
                                f"[{cell['mps_min']:.3f}–{cell['mps_max']:.3f}]"
                            )
                            rest = (
                                f"{cell['ipm']:.3f}",
                                f"{cell['ipc']:.3f}",
                                f"{cell['cpm']:.3f}",
                                f"{cell['attempt_pressure_max']:.2%}",
                            )
                        lines.append(
                            f"| {cell['label']} | "
                            f"{cell['accepted_processes']}/{cell['required_processes']} "
                            f"| {result} | {rest[0]} | {rest[1]} | {rest[2]} | "
                            f"{rest[3]} |"
                        )
                    lines.append("")
                    lines.extend(
                        [
                            "CPU accounting medians:",
                            "",
                            "| Variant | CPU ns/msg | user ins/msg | kernel ins/msg | context switches | migrations |",
                            "|---|---:|---:|---:|---:|---:|",
                        ]
                    )
                    for cell in dimension_cells:
                        if "mps" not in cell:
                            accounting = ("—", "—", "—", "—", "—")
                        else:
                            accounting = (
                                f"{cell['cpu_ns_per_message']:.3f}",
                                f"{cell['user_ipm']:.3f}",
                                f"{cell['kernel_ipm']:.3f}",
                                f"{cell['context_switches']:.0f}",
                                f"{cell['cpu_migrations']:.0f}",
                            )
                        lines.append(
                            f"| {cell['label']} | {accounting[0]} | {accounting[1]} | "
                            f"{accounting[2]} | {accounting[3]} | {accounting[4]} |"
                        )
                    lines.append("")

    if summary["leaders"]:
        lines.extend(
            [
                "## Dimension leaders",
                "",
                "The Pareto column retains paths not dominated on both instructions/message and IPC.",
                "",
                "| Payload / shape / capacity / batch | Throughput | Fewest instructions | Highest IPC | Fewest cycles | Two-rule Pareto |",
                "|---|---|---|---|---|---|",
            ]
        )
        for leader in summary["leaders"]:
            lines.append(
                f"| {leader['payload_bytes']} B / {leader['shape_id']} / "
                f"{leader['capacity']} / {leader['batch']} | {leader['throughput']} | "
                f"{leader['instructions']} | {leader['ipc']} | {leader['cycles']} | "
                f"{', '.join(leader['two_rule_pareto'])} |"
            )
        lines.append("")

    if summary["comparisons"]:
        lines.extend(
            [
                "## Paired comparisons",
                "",
                "Candidate deltas are relative to the paired baseline.",
                "",
                "| Payload / shape / capacity / batch | Pair | Candidate ÷ baseline throughput | instructions | IPC | cycles |",
                "|---|---|---:|---:|---:|---:|",
            ]
        )
        for comparison in summary["comparisons"]:
            if comparison["complete"]:
                values = (
                    f"{comparison['throughput_ratio']:.3f}×",
                    percent(comparison["instructions_delta"]),
                    percent(comparison["ipc_delta"]),
                    percent(comparison["cycles_delta"]),
                )
            else:
                values = ("incomplete", "—", "—", "—")
            lines.append(
                f"| {comparison['payload_bytes']} B / {comparison['shape_id']} / "
                f"{comparison['capacity']} / {comparison['batch']} | "
                f"{comparison['pair']} | {values[0]} | {values[1]} | {values[2]} | {values[3]} |"
            )
        lines.append("")

    rejected = [
        attempt
        for attempt in state["attempts"]
        if attempt["kind"] == "required" and not attempt["accepted"]
    ]
    if rejected:
        lines.extend(["## Rejected required attempts", ""])
        for attempt in rejected:
            lines.append(
                f"- Attempt {attempt['sequence']} `{attempt['job_id']}`: "
                + "; ".join(attempt["reasons"])
                + "."
            )
        lines.append("")
    probes = [attempt for attempt in state["attempts"] if attempt["kind"] == "probe"]
    pressure_warnings = [
        attempt for attempt in state["attempts"] if attempt.get("warnings")
    ]
    if probes:
        lines.extend(["## Diagnostic probes", ""])
        for attempt in probes:
            disposition = "valid diagnostic" if not attempt["reasons"] else "; ".join(attempt["reasons"])
            lines.append(
                f"- Attempt {attempt['sequence']} `{attempt['job_id']}` at "
                f"{attempt['per_producer']} messages/producer: {disposition}."
            )
        lines.append("")
    if pressure_warnings:
        lines.extend(["## Run-pressure observations", ""])
        for attempt in pressure_warnings:
            lines.append(
                f"- Attempt {attempt['sequence']} `{attempt['job_id']}`: "
                + "; ".join(attempt["warnings"])
                + "."
            )
        lines.append("")
    if state["pending"]:
        lines.extend(["## Remaining queue", ""])
        for job_id in state["pending"]:
            lines.append(f"- `{job_id}`")
        lines.append("")
    if state.get("stop_reason"):
        lines.extend(["## Stop reason", "", state["stop_reason"], ""])
    return "\n".join(lines)


def checkpoint(output: Path, spec: dict[str, Any], state: dict[str, Any]) -> None:
    summary = summarize(spec, state)
    atomic_write(output / "spec.resolved.json", json_bytes(spec))
    atomic_write(output / "state.json", json_bytes(state))
    atomic_write(output / "summary.json", json_bytes(summary))
    atomic_write(output / "REPORT.md", render_report(spec, state, summary).encode())


def initial_state(
    spec: dict[str, Any], spec_sha256: str, binaries: dict[str, str]
) -> dict[str, Any]:
    jobs = make_jobs(spec)
    return {
        "version": STATE_VERSION,
        "name": spec["name"],
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "status": "IN PROGRESS",
        "stop_reason": None,
        "spec_sha256": spec_sha256,
        "binaries": binaries,
        "source_checkpoint": command_version(["git", "rev-parse", "HEAD"]),
        "platform": platform.platform(),
        "python": sys.version,
        "rustc": command_version(["rustc", "--version", "--verbose"]),
        "perf": command_version(["perf", "--version"]),
        "jobs": jobs,
        "pending": [job["id"] for job in jobs],
        "accepted": {},
        "failed": {},
        "attempt_counts": {},
        "probe_jobs": [],
        "attempts": [],
        "admission_events": [],
    }


class Controller:
    def __init__(
        self,
        spec: dict[str, Any],
        state: dict[str, Any],
        output: Path,
        pressure_path: Path,
        max_jobs: int | None,
    ) -> None:
        self.spec = spec
        self.state = state
        self.output = output
        self.pressure_path = pressure_path
        self.max_jobs = max_jobs
        self.accepted_this_run = 0
        self.interrupted = False
        self.jobs = {job["id"]: job for job in state["jobs"]}

    def save(self) -> None:
        self.state["updated_at"] = utc_now()
        checkpoint(self.output, self.spec, self.state)

    def admission_event(self, kind: str, job_id: str, **fields: Any) -> None:
        self.state["admission_events"].append(
            {"at": utc_now(), "kind": kind, "job_id": job_id, **fields}
        )
        self.save()

    def wait_for_admission(self, job: dict[str, Any]) -> str:
        admission = self.spec["admission"]
        started = time.monotonic()
        announced = False
        probe_available = job["id"] not in self.state["probe_jobs"]
        while not self.interrupted:
            pressure = read_cpu_pressure(self.pressure_path)
            waited = time.monotonic() - started
            if pressure["avg10"] <= admission["avg10_max"]:
                self.admission_event("admitted", job["id"], pressure=pressure, waited_seconds=waited)
                return "required"
            if not announced:
                print(
                    f"wait {job['id']}: CPU PSI avg10 {pressure['avg10']:.2f}% > "
                    f"{admission['avg10_max']:.2f}%",
                    flush=True,
                )
                self.admission_event("waiting", job["id"], pressure=pressure)
                announced = True
            if (
                probe_available
                and waited >= admission["wait_before_probe_seconds"]
                and pressure["avg10"] <= admission["probe_avg10_max"]
            ):
                self.admission_event(
                    "diagnostic-probe-admitted",
                    job["id"],
                    pressure=pressure,
                    waited_seconds=waited,
                )
                return "probe"
            if waited >= admission["max_wait_seconds"]:
                self.admission_event(
                    "admission-timeout", job["id"], pressure=pressure, waited_seconds=waited
                )
                return "timeout"
            if int(waited) > 0 and int(waited) % 30 < admission["poll_seconds"]:
                print(
                    f"still waiting {job['id']}: avg10={pressure['avg10']:.2f}% "
                    f"elapsed={waited:.0f}s",
                    flush=True,
                )
            time.sleep(admission["poll_seconds"])
        return "interrupted"

    def record_attempt(self, attempt: dict[str, Any]) -> None:
        attempt["sequence"] = len(self.state["attempts"]) + 1
        self.state["attempts"].append(attempt)

    def run_probe(self, job: dict[str, Any]) -> None:
        admission = self.spec["admission"]
        per_producer = max(
            admission["probe_min_per_producer"],
            int(job["per_producer"] * admission["probe_scale"]),
        )
        per_producer = min(per_producer, job["per_producer"] - 1)
        print(f"probe {job['id']}: {per_producer} messages/producer", flush=True)
        attempt = execute_attempt(
            self.spec,
            job,
            self.pressure_path,
            kind="probe",
            per_producer=per_producer,
            samples=admission["probe_samples"],
        )
        self.record_attempt(attempt)
        self.state["probe_jobs"].append(job["id"])
        self.save()

    def run_required(self, job: dict[str, Any]) -> None:
        job_id = job["id"]
        attempt_number = self.state["attempt_counts"].get(job_id, 0) + 1
        self.state["attempt_counts"][job_id] = attempt_number
        print(f"run {job_id}: required attempt {attempt_number}", flush=True)
        attempt = execute_attempt(
            self.spec,
            job,
            self.pressure_path,
            kind="required",
            per_producer=job["per_producer"],
            samples=self.spec["samples"],
        )
        self.record_attempt(attempt)
        self.state["pending"].pop(0)
        if attempt["accepted"]:
            self.state["accepted"][job_id] = attempt["sequence"]
            self.accepted_this_run += 1
            print(
                f"accepted {job_id}: {attempt['metrics']['mps']:.3f} Mmsg/s, "
                f"{attempt['metrics']['ipm']:.3f} ins/msg, IPC {attempt['metrics']['ipc']:.3f}, "
                f"pressure {attempt['attempt_pressure']:.2%}",
                flush=True,
            )
        elif attempt_number <= self.spec["admission"]["max_retries"]:
            self.state["pending"].append(job_id)
            print(f"rejected {job_id}: {'; '.join(attempt['reasons'])}; requeued", flush=True)
            time.sleep(self.spec["admission"]["retry_cooldown_seconds"])
        else:
            self.state["failed"][job_id] = {
                "attempts": attempt_number,
                "last_reasons": attempt["reasons"],
            }
            print(f"failed {job_id}: {'; '.join(attempt['reasons'])}", flush=True)
        self.save()

    def run(self) -> int:
        self.state["status"] = "IN PROGRESS"
        self.state["stop_reason"] = None
        self.save()
        while self.state["pending"] and not self.interrupted:
            if self.max_jobs is not None and self.accepted_this_run >= self.max_jobs:
                self.state["status"] = "INCOMPLETE"
                self.state["stop_reason"] = f"Stopped at --max-jobs {self.max_jobs}."
                self.save()
                return 2
            job = self.jobs[self.state["pending"][0]]
            decision = self.wait_for_admission(job)
            if decision == "probe":
                self.run_probe(job)
                continue
            if decision == "required":
                self.run_required(job)
                continue
            if decision == "timeout":
                self.state["status"] = "INCOMPLETE"
                self.state["stop_reason"] = (
                    f"CPU pressure did not enter the required admission gate within "
                    f"{self.spec['admission']['max_wait_seconds']:.0f} seconds."
                )
                self.save()
                return 2
            break
        if self.interrupted:
            self.state["status"] = "INCOMPLETE"
            self.state["stop_reason"] = "Interrupted; checkpoint is resumable."
            self.save()
            return 130
        if self.state["failed"]:
            self.state["status"] = "FAILED"
            self.state["stop_reason"] = "One or more required jobs exhausted their retries."
            self.save()
            return 1
        self.state["status"] = "COMPLETE"
        self.state["stop_reason"] = None
        self.save()
        return 0


def self_test() -> None:
    orders = balanced_orders(6, 4)
    assert orders[0] == [0, 1, 2, 3, 4, 5]
    assert orders[1] == [5, 4, 3, 2, 1, 0]
    assert orders[2] == [3, 4, 5, 0, 1, 2]
    assert orders[3] == [2, 1, 0, 5, 4, 3]
    for variant in range(6):
        assert sum(order.index(variant) for order in orders) == 10

    axis_spec = {
        "variants": [
            {"id": "all", "producer_batch": "scalar"},
            {
                "id": "filtered",
                "producer_batch": "batch",
                "include_shapes": ["one"],
                "include_batches": [64],
            },
        ],
        "payloads": [{"bytes": 8, "messages_per_sample": 10}],
        "shapes": [
            {"id": "one", "producers": 1, "consumers": 1, "cpus": [0, 1]},
            {"id": "two", "producers": 2, "consumers": 1, "cpus": [2, 3, 4]},
        ],
        "capacities": [1024, 4096],
        "batches": [1, 64],
        "rounds": 4,
    }
    jobs = make_jobs(axis_spec)
    assert len(jobs) == 40
    assert len({job["id"] for job in jobs}) == len(jobs)
    assert {job["per_producer"] for job in jobs if job["shape_id"] == "two"} == {5}

    spec = {
        "binary": "/default-binary",
        "warmups": 1,
        "variants": [
            {
                "id": "ring",
                "label": "Ring",
                "mode": "mpmc-ring",
                "producer_batch": "scalar",
                "binary": "/variant-binary",
            }
        ],
    }
    job = {
        "variant_id": "ring",
        "payload_bytes": 8,
        "producers": 1,
        "consumers": 1,
        "cpus": [0, 1],
        "capacity": 4096,
        "batch": 64,
    }
    command = benchmark_command(spec, job, per_producer=10, samples=2)
    assert command[command.index("--") + 1] == "/variant-binary"
    raw = (
        "impl=mpmc-ring;bytes=8;producers=1;consumers=1;messages=10;samples=2;"
        "warmups=1;delivered=30;capacity=4096;capacity_mode=explicit;aggregate_capacity=4096;batch=64;"
        "producer_batch=1;worker_cpus=0,1;pool_blocks_per_producer=0;"
        "pool_payload_bytes=0;median_mps=15.000000;median_gbps=0.120000;"
        "min_mps=10.000000;max_mps=20.000000;sample_mps=10.000000,20.000000;"
        "worker_metrics=off\n"
        "300;;cycles:uk;1;100.00;;\n600;;instructions:uk;1;100.00;;\n"
        "240;;cycles:u;1;100.00;;\n480;;instructions:u;1;100.00;;\n"
        "0.003;msec;task-clock;1;100.00;;\n2;;context-switches;1;100.00;;\n"
        "0;;cpu-migrations;1;100.00;;\n"
    )
    metrics, reasons = parse_payload_output(raw, spec, job, per_producer=10, samples=2)
    assert reasons == []
    assert metrics is not None
    assert metrics["ipm"] == 20
    assert metrics["cpm"] == 10
    assert metrics["ipc"] == 2
    assert metrics["cpu_ns_per_message"] == 100
    malformed, reasons = parse_payload_output(raw.replace("100.00", "99.00", 1), spec, job, per_producer=10, samples=2)
    assert malformed is None
    assert reasons == ["perf event cycles:uk scheduled 99.0%"]
    assert math.isclose(pressure_fraction({"total": 10}, {"total": 510}, 0.01), 0.05)

    invalid_first_variant = {
        "name": "validation regression",
        "binary": sys.executable,
        "variants": [
            {
                "id": "bad",
                "label": "bad",
                "mode": "mpmc-ring",
                "producer_batch": "scalar",
                "include_capacities": [2],
            },
            {
                "id": "good",
                "label": "good",
                "mode": "mpmc-ring",
                "producer_batch": "scalar",
            },
        ],
        "payloads": [{"bytes": 8, "messages_per_sample": 10}],
        "shapes": [{"id": "one", "producers": 1, "consumers": 1, "cpus": [0, 1]}],
        "capacities": [1],
        "batches": [1],
    }
    try:
        validate_spec(invalid_first_variant, Path("self-test.json"))
    except ConfigurationError as error:
        assert "bad.include_capacities" in str(error)
    else:
        raise AssertionError("the first variant's invalid capacity selector was accepted")

    with tempfile.TemporaryDirectory() as temporary:
        destination = Path(temporary) / "atomic.json"
        atomic_write(destination, json_bytes({"ok": True}))
        assert json.loads(destination.read_text()) == {"ok": True}
    print("benchmark_matrix self-test: ok")


def load_configuration(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ConfigurationError(f"invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ConfigurationError("spec root must be an object")
    return validate_spec(value, path), sha256_bytes(raw)


def run_command(arguments: argparse.Namespace) -> int:
    spec_path = arguments.spec.resolve()
    spec, spec_sha256 = load_configuration(spec_path)
    binaries = binary_hashes(spec)
    output = arguments.output.resolve()
    state_path = output / "state.json"
    if arguments.dry_run:
        jobs = make_jobs(spec)
        workloads = [
            {
                "payload_bytes": payload["bytes"],
                "shape_id": shape["id"],
                "messages_per_sample": payload["messages_per_sample"],
                "per_producer": payload["messages_per_sample"] // shape["producers"],
            }
            for payload in spec["payloads"]
            for shape in spec["shapes"]
        ]
        print(
            json.dumps(
                {
                    "name": spec["name"],
                    "binaries": binaries,
                    "spec_sha256": spec_sha256,
                    "required_jobs": len(jobs),
                    "balanced_orders": balanced_orders(len(spec["variants"]), spec["rounds"]),
                    "workloads": workloads,
                    "first_job": jobs[0],
                    "last_job": jobs[-1],
                },
                indent=2,
            )
        )
        return 0
    if arguments.resume:
        if not state_path.is_file():
            raise ConfigurationError(f"no checkpoint to resume at {state_path}")
        state = json.loads(state_path.read_text())
        if state.get("version") != STATE_VERSION:
            raise ConfigurationError("checkpoint version does not match controller")
        if state.get("spec_sha256") != spec_sha256:
            raise ConfigurationError("checkpoint spec hash differs; start a new output directory")
        if state.get("binaries") != binaries:
            raise ConfigurationError("checkpoint binary hashes differ; start a new output directory")
    else:
        if output.exists() and any(output.iterdir()):
            raise ConfigurationError(f"output directory is not empty: {output}; use --resume")
        state = initial_state(spec, spec_sha256, binaries)

    pressure_path = arguments.pressure_path.resolve()
    if shutil.which("perf") is None:
        raise ConfigurationError("perf is not available on PATH")
    read_cpu_pressure(pressure_path)
    controller = Controller(spec, state, output, pressure_path, arguments.max_jobs)

    def interrupt(_signum: int, _frame: Any) -> None:
        controller.interrupted = True

    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)
    return controller.run()


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a balanced Prescient payload benchmark matrix with adaptive CPU-pressure "
            "admission, strict perf validation, resumable checkpoints, and reports."
        ),
        epilog=(
            "Busy-host behavior: wait for the required admission gate; after the configured "
            "delay, optionally run one reduced diagnostic probe; resume waiting for the full "
            "job. Probes never enter accepted aggregates."
        ),
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    run_parser = subparsers.add_parser("run", help="execute or resume a JSON matrix spec")
    run_parser.add_argument("spec", type=Path)
    run_parser.add_argument("--output", type=Path, required=True)
    run_parser.add_argument("--resume", action="store_true")
    run_parser.add_argument("--dry-run", action="store_true")
    run_parser.add_argument("--max-jobs", type=int, help="stop resumably after N accepted jobs")
    run_parser.add_argument(
        "--pressure-path", type=Path, default=Path("/proc/pressure/cpu")
    )
    subparsers.add_parser("self-test", help="run dependency-free controller tests")
    return parser


def main() -> int:
    parser = make_parser()
    arguments = parser.parse_args()
    try:
        if arguments.action == "self-test":
            self_test()
            return 0
        if arguments.max_jobs is not None and arguments.max_jobs < 1:
            raise ConfigurationError("--max-jobs must be positive")
        return run_command(arguments)
    except (ConfigurationError, OSError, RuntimeError) as error:
        print(f"benchmark_matrix: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
