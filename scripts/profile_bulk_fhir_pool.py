#!/usr/bin/env python3
"""Profile Patient, Observation, and Condition resources from bulk FHIR NDJSON."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import urllib.request
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

TARGET_RESOURCE_TYPES = ("Patient", "Observation", "Condition")
DATASET_PRESETS = {
    "small": {
        "branch": "10-patients",
        "archive_name": "10-patients.zip",
        "extract_dir": "sample-bulk-fhir-datasets-10-patients",
    },
    "medium": {
        "branch": "100-patients",
        "archive_name": "100-patients.zip",
        "extract_dir": "sample-bulk-fhir-datasets-100-patients",
    },
    "large": {
        "branch": "1000-patients",
        "archive_name": "1000-patients.zip",
        "extract_dir": "sample-bulk-fhir-datasets-1000-patients",
    },
}


@dataclass
class ProfilingStats:
    resource_counts: Counter[str] = field(default_factory=Counter)
    resource_reference_counts: Counter[str] = field(default_factory=Counter)
    linked_context_counts: Counter[str] = field(default_factory=Counter)
    observation_total: int = 0
    observation_numeric: int = 0
    observation_with_unit: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download or reuse a SMART on FHIR sample bulk dataset, profile Patient / "
            "Observation / Condition resources, and emit quota-planning outputs."
        )
    )
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASET_PRESETS),
        default="medium",
        help="Dataset preset to use when --input-dir is not provided.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        help="Existing extracted dataset directory containing NDJSON files.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Root directory for downloaded archives and extracted datasets.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for generated CSV/Markdown outputs. Defaults to outputs/<dataset>.",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download and extract the preset dataset archive if the input directory is missing.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Redownload and re-extract the preset dataset archive.",
    )
    parser.add_argument(
        "--pilot-size",
        type=int,
        default=100,
        help="Example pilot sample size to use in the markdown recommendation.",
    )
    parser.add_argument(
        "--observation-oversample-factor",
        type=float,
        default=1.10,
        help=(
            "Relative weight multiplier to apply to Observation in the optional "
            "debug-focused quota example."
        ),
    )
    return parser.parse_args()


def ensure_dataset_available(args: argparse.Namespace) -> Path:
    if args.input_dir:
        input_dir = args.input_dir.expanduser().resolve()
        if not input_dir.exists():
            raise SystemExit(f"Input directory does not exist: {input_dir}")
        return input_dir

    preset = DATASET_PRESETS[args.dataset]
    data_dir = args.data_dir.expanduser().resolve()
    raw_dir = data_dir / "raw"
    download_dir = data_dir / "downloads"
    input_dir = raw_dir / preset["extract_dir"]
    archive_path = download_dir / preset["archive_name"]

    if input_dir.exists() and not args.force_download:
        return input_dir

    if not args.download and not input_dir.exists():
        raise SystemExit(
            "Dataset directory is missing. Re-run with --download or provide --input-dir."
        )

    download_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    archive_url = (
        "https://github.com/smart-on-fhir/sample-bulk-fhir-datasets/"
        f"archive/refs/heads/{preset['branch']}.zip"
    )

    if args.force_download or not archive_path.exists():
        urllib.request.urlretrieve(archive_url, archive_path)  # nosec: B310

    with zipfile.ZipFile(archive_path) as zipped:
        zipped.extractall(raw_dir)

    if not input_dir.exists():
        raise SystemExit(f"Expected extracted dataset directory is missing: {input_dir}")

    return input_dir


def discover_ndjson_files(input_dir: Path) -> list[Path]:
    files = sorted(path for path in input_dir.rglob("*.ndjson") if path.is_file())
    if not files:
        raise SystemExit(f"No NDJSON files found under {input_dir}")
    return files


def bool_str(value: bool) -> str:
    return "true" if value else "false"


def pct(part: int, whole: int) -> float:
    if whole <= 0:
        return 0.0
    return 100.0 * part / whole


def pct_str(part: int, whole: int) -> str:
    return f"{pct(part, whole):.4f}"


def is_numeric_scalar(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def has_nonempty_value(value: Any) -> bool:
    return value not in (None, "", [], {})


def quantity_has_numeric_value(quantity: Any) -> bool:
    return isinstance(quantity, dict) and is_numeric_scalar(quantity.get("value"))


def quantity_has_unit(quantity: Any) -> bool:
    if not isinstance(quantity, dict):
        return False
    return any(has_nonempty_value(quantity.get(key)) for key in ("unit", "code"))


def range_has_numeric_value(value_range: Any) -> bool:
    if not isinstance(value_range, dict):
        return False
    for bound_key in ("low", "high"):
        bound = value_range.get(bound_key)
        if isinstance(bound, dict) and is_numeric_scalar(bound.get("value")):
            return True
    return False


def ratio_has_numeric_value(value_ratio: Any) -> bool:
    if not isinstance(value_ratio, dict):
        return False
    for bound_key in ("numerator", "denominator"):
        bound = value_ratio.get(bound_key)
        if isinstance(bound, dict) and is_numeric_scalar(bound.get("value")):
            return True
    return False


def observation_has_value(resource: dict[str, Any]) -> bool:
    for key, value in resource.items():
        if key.startswith("value") and has_nonempty_value(value):
            return True
    for component in resource.get("component", []):
        if not isinstance(component, dict):
            continue
        for key, value in component.items():
            if key.startswith("value") and has_nonempty_value(value):
                return True
    return False


def observation_has_unit(resource: dict[str, Any]) -> bool:
    if quantity_has_unit(resource.get("valueQuantity")):
        return True
    for component in resource.get("component", []):
        if isinstance(component, dict) and quantity_has_unit(component.get("valueQuantity")):
            return True
    return False


def observation_likely_numeric(resource: dict[str, Any]) -> bool:
    if quantity_has_numeric_value(resource.get("valueQuantity")):
        return True
    if is_numeric_scalar(resource.get("valueInteger")):
        return True
    if range_has_numeric_value(resource.get("valueRange")):
        return True
    if ratio_has_numeric_value(resource.get("valueRatio")):
        return True
    for component in resource.get("component", []):
        if not isinstance(component, dict):
            continue
        if quantity_has_numeric_value(component.get("valueQuantity")):
            return True
        if is_numeric_scalar(component.get("valueInteger")):
            return True
        if range_has_numeric_value(component.get("valueRange")):
            return True
        if ratio_has_numeric_value(component.get("valueRatio")):
            return True
    return False


def collect_references(node: Any) -> list[str]:
    references: list[str] = []

    def walk(current: Any) -> None:
        if isinstance(current, dict):
            for key, value in current.items():
                if key == "reference" and isinstance(value, str):
                    references.append(value)
                else:
                    walk(value)
        elif isinstance(current, list):
            for item in current:
                walk(item)

    walk(node)
    return references


def normalize_reference_type(reference: str) -> str | None:
    if not reference or reference.startswith("#"):
        return None
    stripped = reference.strip("/")
    if not stripped:
        return None
    parts = stripped.split("/")
    if len(parts) >= 2 and parts[-2] and parts[-2][0].isalpha():
        return parts[-2]
    if parts[0] and parts[0][0].isalpha():
        return parts[0]
    return None


def reference_types_for(resource: dict[str, Any]) -> set[str]:
    reference_types = set()
    for reference in collect_references(resource):
        normalized = normalize_reference_type(reference)
        if normalized:
            reference_types.add(normalized)
    return reference_types


def guess_needs_linked_context(
    resource_type: str, resource: dict[str, Any], reference_types: set[str]
) -> bool:
    if resource_type == "Patient":
        return bool(resource.get("link"))
    if resource_type == "Condition":
        return bool(reference_types)
    if resource_type == "Observation":
        return bool(
            reference_types
            or resource.get("component")
            or resource.get("hasMember")
            or resource.get("derivedFrom")
            or resource.get("basedOn")
        )
    return bool(reference_types)


def guess_complexity(
    resource_type: str,
    resource: dict[str, Any],
    reference_types: set[str],
    likely_numeric: bool,
    needs_linked_context: bool,
) -> str:
    if resource_type == "Patient":
        if any(resource.get(key) for key in ("link", "contact", "generalPractitioner", "managingOrganization")):
            return "medium"
        return "low"

    if resource_type == "Condition":
        score = 0
        if reference_types - {"Patient", "Encounter"}:
            score += 1
        if any(resource.get(key) for key in ("evidence", "stage", "bodySite", "asserter", "recorder", "partOf")):
            score += 1
        return "high" if score >= 2 else "medium"

    if resource_type == "Observation":
        score = 0
        if not likely_numeric:
            score += 1
        if any(
            resource.get(key)
            for key in ("component", "hasMember", "derivedFrom", "basedOn", "specimen", "performer", "focus", "device", "partOf")
        ):
            score += 1
        if reference_types - {"Patient", "Encounter"}:
            score += 1
        if score >= 2:
            return "high"
        if needs_linked_context:
            return "medium"
        return "low"

    return "medium"


def candidate_id_for(resource_type: str, resource_id: str, source_file: str, line_number: int) -> str:
    if resource_id:
        return f"{resource_type}/{resource_id}"
    return f"{resource_type}/{source_file}#L{line_number}"


def write_resource_counts_summary(stats: ProfilingStats, output_dir: Path) -> None:
    total_target_resources = sum(stats.resource_counts.values())
    output_path = output_dir / "resource_counts_summary.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["resource_type", "total_count", "count_pct_among_target_resources"],
        )
        writer.writeheader()
        for resource_type in TARGET_RESOURCE_TYPES:
            writer.writerow(
                {
                    "resource_type": resource_type,
                    "total_count": stats.resource_counts[resource_type],
                    "count_pct_among_target_resources": pct_str(
                        stats.resource_counts[resource_type], total_target_resources
                    ),
                }
            )


def write_observation_profile_summary(stats: ProfilingStats, output_dir: Path) -> None:
    output_path = output_dir / "observation_profile_summary.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "total_observation_count",
                "numeric_observation_count",
                "numeric_observation_pct",
                "with_unit_count",
                "with_unit_pct",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "total_observation_count": stats.observation_total,
                "numeric_observation_count": stats.observation_numeric,
                "numeric_observation_pct": pct_str(
                    stats.observation_numeric, stats.observation_total
                ),
                "with_unit_count": stats.observation_with_unit,
                "with_unit_pct": pct_str(stats.observation_with_unit, stats.observation_total),
            }
        )


def apportion(total: int, weights: dict[str, float]) -> dict[str, int]:
    if total < 0:
        raise ValueError("total must be non-negative")
    if not weights:
        return {}
    total_weight = sum(weights.values())
    if total_weight <= 0:
        return {key: 0 for key in weights}

    raw = {key: total * weight / total_weight for key, weight in weights.items()}
    quotas = {key: int(math.floor(value)) for key, value in raw.items()}
    remainder = total - sum(quotas.values())

    ranked = sorted(
        weights,
        key=lambda key: (raw[key] - quotas[key], raw[key], key),
        reverse=True,
    )
    for key in ranked[:remainder]:
        quotas[key] += 1
    return quotas


def apportion_with_minima(
    total: int, weights: dict[str, float], minima: dict[str, int]
) -> dict[str, int]:
    minimum_total = sum(minima.values())
    if minimum_total > total:
        raise ValueError("Minimum quotas exceed total pilot size")
    remaining = total - minimum_total
    apportioned = apportion(remaining, weights)
    return {key: minima.get(key, 0) + apportioned.get(key, 0) for key in weights}


def quota_line(quotas: dict[str, int]) -> str:
    return (
        f"Patient {quotas['Patient']}, Observation {quotas['Observation']}, "
        f"Condition {quotas['Condition']}"
    )


def minimum_nonzero_sample_size(count: int, total: int) -> int | None:
    if count <= 0 or total <= 0:
        return None
    return math.ceil(total / count)


def write_pilot_quota_recommendation(
    stats: ProfilingStats,
    output_dir: Path,
    dataset_label: str,
    pilot_size: int,
    observation_oversample_factor: float,
) -> None:
    total_target_resources = sum(stats.resource_counts.values())
    weights = {resource_type: float(stats.resource_counts[resource_type]) for resource_type in TARGET_RESOURCE_TYPES}
    proportional = apportion(pilot_size, weights)
    nonzero_minima = {
        "Patient": 1 if pilot_size >= 20 else 0,
        "Observation": 0,
        "Condition": 1 if pilot_size >= 20 else 0,
    }
    floor_adjusted = apportion_with_minima(pilot_size, weights, nonzero_minima)
    oversampled_weights = dict(weights)
    oversampled_weights["Observation"] *= observation_oversample_factor
    observation_debug = apportion(pilot_size, oversampled_weights)

    patient_threshold = minimum_nonzero_sample_size(
        stats.resource_counts["Patient"], total_target_resources
    )
    condition_threshold = minimum_nonzero_sample_size(
        stats.resource_counts["Condition"], total_target_resources
    )

    output_path = output_dir / "pilot_quota_recommendation.md"
    lines = [
        "# Pilot Quota Recommendation",
        "",
        "## Empirical pool distribution",
        (
            f"Across {total_target_resources:,} retained target resources in the {dataset_label} dataset, "
            f"the empirical distribution is Patient {stats.resource_counts['Patient']:,} "
            f"({pct(stats.resource_counts['Patient'], total_target_resources):.4f}%), "
            f"Observation {stats.resource_counts['Observation']:,} "
            f"({pct(stats.resource_counts['Observation'], total_target_resources):.4f}%), "
            f"and Condition {stats.resource_counts['Condition']:,} "
            f"({pct(stats.resource_counts['Condition'], total_target_resources):.4f}%)."
        ),
        "",
        "## Numeric Observation prevalence",
        (
            f"Observation resources total {stats.observation_total:,}. "
            f"Of those, {stats.observation_numeric:,} are conservatively flagged as likely numeric "
            f"({pct(stats.observation_numeric, stats.observation_total):.4f}%), and "
            f"{stats.observation_with_unit:,} include an explicit unit "
            f"({pct(stats.observation_with_unit, stats.observation_total):.4f}%)."
        ),
        (
            "By simple reference scanning, resources that reference at least one other resource are "
            f"Patient {stats.resource_reference_counts['Patient']:,}/{stats.resource_counts['Patient']:,}, "
            f"Observation {stats.resource_reference_counts['Observation']:,}/{stats.resource_counts['Observation']:,}, "
            f"and Condition {stats.resource_reference_counts['Condition']:,}/{stats.resource_counts['Condition']:,}. "
            "The linked-context heuristic is intentionally stricter for Observation and Condition than for Patient."
        ),
        "",
        "## Recommended sampling plan",
        (
            "Use the observed pool distribution as the default set of strata weights, then convert "
            "those weights to integer quotas with largest-remainder rounding."
        ),
        (
            f"For an example pilot of {pilot_size} total seeds, pure proportional quotas would be: "
            f"{quota_line(proportional)}."
        ),
        (
            "Because Patients are extremely rare in the retained pool, strict proportional sampling "
            f"does not reliably produce a non-zero Patient quota until roughly {patient_threshold:,} "
            "total seeds. If the pilot needs all three resource types represented, use a mild "
            "coverage floor instead of a hard fixed ratio."
        ),
    ]
    if condition_threshold:
        lines.append(
            (
                f"Condition becomes reliably non-zero under pure proportional sampling at roughly "
                f"{condition_threshold:,} total seeds."
            )
        )
    lines.extend(
        [
        (
            f"A practical floor-constrained version at {pilot_size} total seeds is: "
            f"{quota_line(floor_adjusted)}. This preserves the Observation-heavy shape while avoiding "
            "a zero-Patient pilot."
        ),
        "",
        "## Optional Observation oversampling for debugging",
        (
            "If early pipeline debugging benefits from more Observation examples, increase the "
            f"Observation weight by about {observation_oversample_factor:.2f}x, renormalize, and "
            "re-apportion. For the same pilot size, that would produce: "
            f"{quota_line(observation_debug)}."
        ),
        (
            "Because Observations already dominate this source pool, additional Observation "
            "oversampling is best treated as a debug-only option. If the debugging target is numeric "
            "value handling, prefer sampling that extra Observation share from the likely-numeric "
            "subset rather than from all Observations."
        ),
        "",
        "## Interpretation",
        (
            "The empirical source pool is overwhelmingly Observation-heavy, with Condition a distant "
            "second and Patient extremely sparse. That means the default pilot quota should follow the "
            "empirical distribution when representativeness matters, but a mildly adjusted version with "
            "a tiny Patient floor is usually the safer choice for small pilots that must exercise all "
            "resource-specific code paths."
        ),
    ]
    )

    with output_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
        handle.write("\n")


def profile_dataset(input_dir: Path, output_dir: Path) -> ProfilingStats:
    output_dir.mkdir(parents=True, exist_ok=True)
    stats = ProfilingStats()
    candidate_catalog_path = output_dir / "candidate_seed_catalog.csv"

    with candidate_catalog_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "candidate_id",
                "resource_type",
                "resource_id",
                "source_file",
                "json_valid",
                "likely_numeric",
                "has_value",
                "has_unit",
                "needs_linked_context",
                "complexity_guess",
            ],
        )
        writer.writeheader()

        for ndjson_path in discover_ndjson_files(input_dir):
            source_file = ndjson_path.relative_to(input_dir).as_posix()
            with ndjson_path.open("r", encoding="utf-8") as source_handle:
                for line_number, raw_line in enumerate(source_handle, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        resource = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(resource, dict):
                        continue

                    resource_type = resource.get("resourceType")
                    if resource_type not in TARGET_RESOURCE_TYPES:
                        continue

                    resource_id = str(resource.get("id", "") or "")
                    reference_types = reference_types_for(resource)
                    references_another_resource = bool(reference_types)
                    if references_another_resource:
                        stats.resource_reference_counts[resource_type] += 1

                    has_value = False
                    has_unit = False
                    likely_numeric = False
                    if resource_type == "Observation":
                        has_value = observation_has_value(resource)
                        has_unit = observation_has_unit(resource)
                        likely_numeric = observation_likely_numeric(resource)
                        stats.observation_total += 1
                        if has_unit:
                            stats.observation_with_unit += 1
                        if likely_numeric:
                            stats.observation_numeric += 1

                    needs_linked_context = guess_needs_linked_context(
                        resource_type, resource, reference_types
                    )
                    if needs_linked_context:
                        stats.linked_context_counts[resource_type] += 1

                    complexity_guess = guess_complexity(
                        resource_type,
                        resource,
                        reference_types,
                        likely_numeric,
                        needs_linked_context,
                    )

                    stats.resource_counts[resource_type] += 1
                    writer.writerow(
                        {
                            "candidate_id": candidate_id_for(
                                resource_type, resource_id, source_file, line_number
                            ),
                            "resource_type": resource_type,
                            "resource_id": resource_id,
                            "source_file": source_file,
                            "json_valid": bool_str(True),
                            "likely_numeric": bool_str(likely_numeric),
                            "has_value": bool_str(has_value),
                            "has_unit": bool_str(has_unit),
                            "needs_linked_context": bool_str(needs_linked_context),
                            "complexity_guess": complexity_guess,
                        }
                    )

    return stats


def print_run_summary(input_dir: Path, output_dir: Path, stats: ProfilingStats) -> None:
    total_target_resources = sum(stats.resource_counts.values())
    print(f"Input dataset: {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Retained target resources: {total_target_resources:,}")
    for resource_type in TARGET_RESOURCE_TYPES:
        count = stats.resource_counts[resource_type]
        print(f"  {resource_type}: {count:,} ({pct(count, total_target_resources):.4f}%)")
    print(
        "  Observation likely numeric: "
        f"{stats.observation_numeric:,}/{stats.observation_total:,} "
        f"({pct(stats.observation_numeric, stats.observation_total):.4f}%)"
    )
    print(
        "  Observation with unit: "
        f"{stats.observation_with_unit:,}/{stats.observation_total:,} "
        f"({pct(stats.observation_with_unit, stats.observation_total):.4f}%)"
    )


def main() -> int:
    args = parse_args()
    input_dir = ensure_dataset_available(args)
    output_dir = (
        args.output_dir.expanduser().resolve()
        if args.output_dir
        else (Path("outputs") / args.dataset).resolve()
    )

    stats = profile_dataset(input_dir, output_dir)
    write_resource_counts_summary(stats, output_dir)
    write_observation_profile_summary(stats, output_dir)
    write_pilot_quota_recommendation(
        stats,
        output_dir,
        dataset_label=args.dataset,
        pilot_size=args.pilot_size,
        observation_oversample_factor=args.observation_oversample_factor,
    )
    print_run_summary(input_dir, output_dir, stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
