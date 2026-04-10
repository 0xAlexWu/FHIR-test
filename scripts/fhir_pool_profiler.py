#!/usr/bin/env python3
"""Profile SMART sample bulk FHIR datasets for seed-pool planning.

This script can:
1. Download and unpack the pre-generated Large (1,000 patient) dataset.
2. Profile any local dataset directory of NDJSON files.
3. Optionally run ``./generate.sh <N>`` in the upstream repository and then
   profile the generated dataset.

Outputs:
- resource_counts_summary.csv
- observation_profile_summary.csv
- candidate_seed_catalog.csv
- pilot_quota_recommendation.md
- large_dataset_summary.md
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from json import JSONDecodeError
from pathlib import Path
from typing import Any


TARGET_RESOURCE_TYPES = ("Patient", "Observation", "Condition")
TARGET_SET = set(TARGET_RESOURCE_TYPES)
LARGE_ARCHIVE_URL = (
    "https://github.com/smart-on-fhir/sample-bulk-fhir-datasets/"
    "archive/refs/heads/1000-patients.zip"
)
LARGE_EXTRACTED_DIRNAME = "sample-bulk-fhir-datasets-1000-patients"

OBS_VALUE_KEYS = (
    "valueQuantity",
    "valueInteger",
    "valueDecimal",
    "valueBoolean",
    "valueString",
    "valueCodeableConcept",
    "valueDateTime",
    "valueTime",
    "valuePeriod",
    "valueRange",
    "valueRatio",
    "valueSampledData",
)
OBS_COMPLEX_KEYS = (
    "basedOn",
    "partOf",
    "derivedFrom",
    "hasMember",
    "focus",
    "performer",
    "specimen",
    "encounter",
    "interpretation",
    "component",
)
CONDITION_COMPLEX_KEYS = (
    "encounter",
    "evidence",
    "stage",
    "bodySite",
    "recorder",
    "asserter",
    "note",
)
PATIENT_COMPLEX_KEYS = (
    "generalPractitioner",
    "managingOrganization",
    "link",
    "contact",
)


@dataclass
class ProfileResult:
    dataset_dir: Path
    output_dir: Path
    resource_counts: Counter[str]
    observation_total_count: int
    observation_numeric_count: int
    observation_with_unit_count: int
    candidate_rows: list[dict[str, Any]]
    invalid_target_rows: int
    invalid_json_lines: int
    total_lines_seen: int


def cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download and profile SMART sample-bulk-fhir-datasets outputs for "
            "empirical source-pool planning."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    download_parser = subparsers.add_parser(
        "download-large",
        help="Download and unzip the pre-generated Large (1,000 patient) dataset.",
    )
    add_download_args(download_parser)

    profile_large_parser = subparsers.add_parser(
        "profile-large",
        help="Download the Large dataset if needed, then profile it.",
    )
    add_download_args(profile_large_parser)
    add_output_args(profile_large_parser, default_output="outputs/large")

    profile_dir_parser = subparsers.add_parser(
        "profile-dir",
        help="Profile an already-downloaded dataset directory of NDJSON files.",
    )
    profile_dir_parser.add_argument(
        "--dataset-dir",
        required=True,
        type=Path,
        help="Path to the dataset directory that contains NDJSON files.",
    )
    add_output_args(profile_dir_parser, default_output="outputs/profile")

    generate_parser = subparsers.add_parser(
        "generate-and-profile",
        help=(
            "Run ./generate.sh <patient-count> in the upstream repository, then "
            "profile the generated dataset."
        ),
    )
    generate_parser.add_argument(
        "--repo-dir",
        required=True,
        type=Path,
        help="Path to a checked-out sample-bulk-fhir-datasets repository.",
    )
    generate_parser.add_argument(
        "--patient-count",
        type=int,
        default=2000,
        help="Patient count to pass to ./generate.sh (default: 2000).",
    )
    add_output_args(generate_parser, default_output="outputs/custom-generated")
    generate_parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Assume the generated dataset already exists and only profile it.",
    )
    return parser


def add_download_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path("data/downloads"),
        help="Directory where the Large archive and extracted files should live.",
    )


def add_output_args(parser: argparse.ArgumentParser, default_output: str) -> None:
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(default_output),
        help="Directory where profiling outputs should be written.",
    )


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def download_large_dataset(download_dir: Path) -> Path:
    download_dir = ensure_dir(download_dir)
    extracted_dir = download_dir / LARGE_EXTRACTED_DIRNAME
    if dataset_has_ndjson(extracted_dir):
        return extracted_dir

    archive_path = download_dir / "1000-patients.zip"
    if not archive_path.exists():
        print(f"Downloading Large dataset to {archive_path}...", file=sys.stderr)
        with urllib.request.urlopen(LARGE_ARCHIVE_URL) as response, archive_path.open(
            "wb"
        ) as output_file:
            shutil.copyfileobj(response, output_file)

    print(f"Extracting {archive_path}...", file=sys.stderr)
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(download_dir)

    if not dataset_has_ndjson(extracted_dir):
        raise FileNotFoundError(
            f"Expected extracted dataset under {extracted_dir}, but no NDJSON files were found."
        )
    return extracted_dir


def dataset_has_ndjson(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.ndjson"))


def resolve_dataset_dir(path: Path) -> Path:
    path = path.resolve()
    if dataset_has_ndjson(path):
        return path
    raise FileNotFoundError(f"No NDJSON files found under {path}")


def run_generate(repo_dir: Path, patient_count: int) -> Path:
    repo_dir = repo_dir.resolve()
    generate_script = repo_dir / "generate.sh"
    if not generate_script.exists():
        raise FileNotFoundError(f"Missing generator script: {generate_script}")

    print(
        f"Running {generate_script} {patient_count} inside {repo_dir}...",
        file=sys.stderr,
    )
    subprocess.run(
        [str(generate_script), str(patient_count)],
        cwd=repo_dir,
        check=True,
    )
    dataset_dir = repo_dir / f"{patient_count}-patients"
    return resolve_dataset_dir(dataset_dir)


def profile_dataset(dataset_dir: Path, output_dir: Path) -> ProfileResult:
    dataset_dir = resolve_dataset_dir(dataset_dir)
    output_dir = ensure_dir(output_dir.resolve())

    resource_counts: Counter[str] = Counter()
    candidate_rows: list[dict[str, Any]] = []
    invalid_target_rows = 0
    invalid_json_lines = 0
    total_lines_seen = 0

    observation_total_count = 0
    observation_numeric_count = 0
    observation_with_unit_count = 0

    for ndjson_path in sorted(dataset_dir.rglob("*.ndjson")):
        if ndjson_path.name == "log.ndjson":
            continue

        inferred_type = ndjson_path.name.split(".")[0]
        source_file = ndjson_path.relative_to(dataset_dir).as_posix()

        with ndjson_path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                total_lines_seen += 1
                line = raw_line.strip()
                if not line:
                    continue

                try:
                    resource = json.loads(line)
                except JSONDecodeError:
                    invalid_json_lines += 1
                    if inferred_type in TARGET_SET:
                        invalid_target_rows += 1
                        candidate_rows.append(
                            {
                                "candidate_id": (
                                    f"{inferred_type}:invalid:{source_file}:L{line_number}"
                                ),
                                "resource_type": inferred_type,
                                "resource_id": "",
                                "source_file": source_file,
                                "json_valid": False,
                                "likely_numeric": False,
                                "has_value": False,
                                "has_unit": False,
                                "needs_linked_context": False,
                                "complexity_guess": "unknown",
                            }
                        )
                    continue

                resource_type = resource.get("resourceType")
                if resource_type not in TARGET_SET:
                    continue

                resource_counts[resource_type] += 1
                references = collect_references(resource)
                has_reference = bool(references)

                has_value = False
                has_unit = False
                likely_numeric = False

                if resource_type == "Observation":
                    observation_total_count += 1
                    has_value = observation_has_value(resource)
                    has_unit = observation_has_unit(resource)
                    likely_numeric = observation_likely_numeric(resource)
                    if has_unit:
                        observation_with_unit_count += 1
                    if likely_numeric:
                        observation_numeric_count += 1

                needs_linked_context, complexity_guess = infer_complexity(
                    resource, resource_type, references
                )

                candidate_rows.append(
                    {
                        "candidate_id": build_candidate_id(
                            resource_type=resource_type,
                            resource_id=resource.get("id"),
                            source_file=source_file,
                            line_number=line_number,
                        ),
                        "resource_type": resource_type,
                        "resource_id": resource.get("id", ""),
                        "source_file": source_file,
                        "json_valid": True,
                        "likely_numeric": likely_numeric,
                        "has_value": has_value,
                        "has_unit": has_unit,
                        "needs_linked_context": needs_linked_context,
                        "complexity_guess": complexity_guess,
                    }
                )

    result = ProfileResult(
        dataset_dir=dataset_dir,
        output_dir=output_dir,
        resource_counts=resource_counts,
        observation_total_count=observation_total_count,
        observation_numeric_count=observation_numeric_count,
        observation_with_unit_count=observation_with_unit_count,
        candidate_rows=candidate_rows,
        invalid_target_rows=invalid_target_rows,
        invalid_json_lines=invalid_json_lines,
        total_lines_seen=total_lines_seen,
    )
    write_outputs(result)
    return result


def build_candidate_id(
    resource_type: str,
    resource_id: Any,
    source_file: str,
    line_number: int,
) -> str:
    if resource_id:
        return f"{resource_type}/{resource_id}"
    return f"{resource_type}:anon:{source_file}:L{line_number}"


def observation_has_value(resource: dict[str, Any]) -> bool:
    if any(key in resource for key in OBS_VALUE_KEYS):
        return True
    for component in ensure_list(resource.get("component")):
        if any(key in component for key in OBS_VALUE_KEYS):
            return True
    return False


def observation_has_unit(resource: dict[str, Any]) -> bool:
    if quantity_has_unit(resource.get("valueQuantity")):
        return True
    for component in ensure_list(resource.get("component")):
        if quantity_has_unit(component.get("valueQuantity")):
            return True
    return False


def observation_likely_numeric(resource: dict[str, Any]) -> bool:
    if quantity_has_numeric_value(resource.get("valueQuantity")):
        return True
    if is_numeric_scalar(resource.get("valueInteger")):
        return True
    if is_numeric_scalar(resource.get("valueDecimal")):
        return True
    for component in ensure_list(resource.get("component")):
        if quantity_has_numeric_value(component.get("valueQuantity")):
            return True
        if is_numeric_scalar(component.get("valueInteger")):
            return True
        if is_numeric_scalar(component.get("valueDecimal")):
            return True
    return False


def quantity_has_numeric_value(value_quantity: Any) -> bool:
    if not isinstance(value_quantity, dict):
        return False
    return is_numeric_scalar(value_quantity.get("value"))


def quantity_has_unit(value_quantity: Any) -> bool:
    if not isinstance(value_quantity, dict):
        return False
    return any(value_quantity.get(key) for key in ("unit", "code", "system"))


def is_numeric_scalar(value: Any) -> bool:
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    if isinstance(value, str):
        try:
            parsed = Decimal(value)
        except (InvalidOperation, ValueError):
            return False
        return parsed.is_finite()
    return False


def ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def collect_references(value: Any) -> set[str]:
    references: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                if key == "reference" and isinstance(child, str):
                    references.add(child)
                else:
                    walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    return references


def infer_complexity(
    resource: dict[str, Any],
    resource_type: str,
    references: set[str],
) -> tuple[bool, str]:
    reference_count = len(references)
    non_patient_ref_count = sum(
        1 for ref in references if not ref.startswith("Patient/") and "/" in ref
    )

    if resource_type == "Observation":
        component_present = bool(ensure_list(resource.get("component")))
        advanced_links = any(
            resource.get(key)
            for key in ("basedOn", "partOf", "derivedFrom", "hasMember", "focus", "performer", "specimen")
        )
        standard_context = bool(resource.get("encounter")) or reference_count > 1
        if component_present or advanced_links or non_patient_ref_count >= 2:
            return True, "high"
        if standard_context or reference_count:
            return True, "moderate"
        return False, "low"

    if resource_type == "Condition":
        advanced_condition_context = any(
            resource.get(key)
            for key in ("evidence", "stage", "bodySite", "recorder", "asserter", "note")
        )
        standard_context = bool(resource.get("encounter")) or reference_count > 1
        if (advanced_condition_context and standard_context) or non_patient_ref_count >= 2:
            return True, "high"
        if advanced_condition_context or standard_context or reference_count:
            return True, "moderate"
        return False, "low"

    patient_context = any(resource.get(key) for key in PATIENT_COMPLEX_KEYS)
    if patient_context and reference_count:
        return True, "moderate"
    if patient_context:
        return False, "low"
    return False, "low"


def write_outputs(result: ProfileResult) -> None:
    write_resource_counts_csv(result)
    write_observation_summary_csv(result)
    write_candidate_catalog_csv(result)
    write_pilot_quota_markdown(result)
    write_large_dataset_summary(result)


def write_resource_counts_csv(result: ProfileResult) -> None:
    output_path = result.output_dir / "resource_counts_summary.csv"
    total_target_resources = sum(result.resource_counts.values())
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "resource_type",
                "total_count",
                "count_pct_among_target_resources",
            ],
        )
        writer.writeheader()
        for resource_type in TARGET_RESOURCE_TYPES:
            total_count = result.resource_counts.get(resource_type, 0)
            writer.writerow(
                {
                    "resource_type": resource_type,
                    "total_count": total_count,
                    "count_pct_among_target_resources": format_pct(
                        total_count, total_target_resources
                    ),
                }
            )


def write_observation_summary_csv(result: ProfileResult) -> None:
    output_path = result.output_dir / "observation_profile_summary.csv"
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
                "total_observation_count": result.observation_total_count,
                "numeric_observation_count": result.observation_numeric_count,
                "numeric_observation_pct": format_pct(
                    result.observation_numeric_count, result.observation_total_count
                ),
                "with_unit_count": result.observation_with_unit_count,
                "with_unit_pct": format_pct(
                    result.observation_with_unit_count, result.observation_total_count
                ),
            }
        )


def write_candidate_catalog_csv(result: ProfileResult) -> None:
    output_path = result.output_dir / "candidate_seed_catalog.csv"
    fieldnames = [
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
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in result.candidate_rows:
            writer.writerow(
                {
                    key: normalize_csv_value(row[key])
                    for key in fieldnames
                }
            )
    write_gzip_copy(output_path)


def write_gzip_copy(path: Path) -> None:
    gzip_path = path.with_suffix(path.suffix + ".gz")
    with path.open("rb") as src, gzip.open(gzip_path, "wb") as dst:
        shutil.copyfileobj(src, dst)


def normalize_csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return str(value).lower()
    return value


def format_pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0.00"
    return f"{(numerator / denominator) * 100:.2f}"


def allocate_proportionally(
    counts: Counter[str],
    sample_size: int,
    minimum_per_present: int = 0,
) -> dict[str, int]:
    if sample_size <= 0:
        return {resource_type: 0 for resource_type in TARGET_RESOURCE_TYPES}

    total = sum(counts.values())
    if total == 0:
        return {resource_type: 0 for resource_type in TARGET_RESOURCE_TYPES}

    present_types = [
        resource_type for resource_type in TARGET_RESOURCE_TYPES if counts.get(resource_type, 0) > 0
    ]
    allocation = {resource_type: 0 for resource_type in TARGET_RESOURCE_TYPES}

    reserved = 0
    if minimum_per_present > 0 and present_types:
        if sample_size >= minimum_per_present * len(present_types):
            for resource_type in present_types:
                allocation[resource_type] = minimum_per_present
            reserved = minimum_per_present * len(present_types)

    remaining_sample_size = sample_size - reserved
    if remaining_sample_size <= 0:
        return allocation

    exact = {
        resource_type: (counts.get(resource_type, 0) / total) * remaining_sample_size
        for resource_type in TARGET_RESOURCE_TYPES
    }
    for resource_type, value in exact.items():
        allocation[resource_type] += int(math.floor(value))

    remainder = sample_size - sum(allocation.values())

    remainders = sorted(
        TARGET_RESOURCE_TYPES,
        key=lambda resource_type: (exact[resource_type] - math.floor(exact[resource_type])),
        reverse=True,
    )
    for resource_type in remainders[:remainder]:
        allocation[resource_type] += 1
    return allocation


def weighted_counts_for_debugging(counts: Counter[str]) -> Counter[str]:
    weighted = Counter(counts)
    weighted["Observation"] = round(weighted.get("Observation", 0) * 1.10)
    return weighted


def write_pilot_quota_markdown(result: ProfileResult) -> None:
    output_path = result.output_dir / "pilot_quota_recommendation.md"
    total_target_resources = sum(result.resource_counts.values())
    baseline_100 = allocate_proportionally(
        result.resource_counts, 100, minimum_per_present=1
    )
    debug_100 = allocate_proportionally(
        weighted_counts_for_debugging(result.resource_counts),
        100,
        minimum_per_present=1,
    )

    lines = [
        "# Pilot Quota Recommendation",
        "",
        "## Empirical Target-Resource Distribution",
        "",
        f"- Total retained target resources: {total_target_resources:,}",
        f"- Patient: {result.resource_counts.get('Patient', 0):,} "
        f"({format_pct(result.resource_counts.get('Patient', 0), total_target_resources)}%)",
        f"- Observation: {result.resource_counts.get('Observation', 0):,} "
        f"({format_pct(result.resource_counts.get('Observation', 0), total_target_resources)}%)",
        f"- Condition: {result.resource_counts.get('Condition', 0):,} "
        f"({format_pct(result.resource_counts.get('Condition', 0), total_target_resources)}%)",
        "",
        "## Numeric Observation Prevalence",
        "",
        f"- Total Observations: {result.observation_total_count:,}",
        f"- Numeric Observations: {result.observation_numeric_count:,} "
        f"({format_pct(result.observation_numeric_count, result.observation_total_count)}%)",
        f"- Observations with units: {result.observation_with_unit_count:,} "
        f"({format_pct(result.observation_with_unit_count, result.observation_total_count)}%)",
        "",
        "## Recommended Pilot Sampling Strategy",
        "",
        "Use approximately proportionate stratified sampling as the default rule.",
        "For any pilot size N, allocate each stratum as round(N x empirical resource share), "
        "while keeping a minimum of one example per available stratum when N is small.",
        "",
        "Example allocation for a 100-seed pilot using the observed Large-pool mix:",
        "",
        f"- Patient: {baseline_100['Patient']}",
        f"- Observation: {baseline_100['Observation']}",
        f"- Condition: {baseline_100['Condition']}",
        "",
        "### Optional Mild Observation Oversampling",
        "",
        "For early pipeline debugging, a modest Observation oversample is reasonable because "
        "Observations are usually the most schema-diverse and numerics are often especially useful "
        "for validation. A simple rule is to apply a 1.10x weight to Observation, renormalize, "
        "and avoid pushing the Observation share so high that Patient and Condition coverage erodes.",
        "",
        "Example allocation for a 100-seed debugging pass with mild Observation oversampling:",
        "",
        f"- Patient: {debug_100['Patient']}",
        f"- Observation: {debug_100['Observation']}",
        f"- Condition: {debug_100['Condition']}",
        "",
        "## Sufficiency Assessment for Downstream Experiments",
        "",
        large_sufficiency_paragraph(result),
        "",
        "## Profiling Notes",
        "",
        "- Numeric Observation detection is conservative: it flags `valueQuantity.value`, "
        "`valueInteger`, `valueDecimal`, and the same fields under `component` when present.",
        "- `needs_linked_context` and `complexity_guess` are heuristic signals driven by reference "
        "patterns and resource structure; they are intended for seed-planning triage, not formal labeling.",
        f"- Invalid JSON lines seen across all NDJSON files: {result.invalid_json_lines:,}",
        f"- Invalid JSON lines in target-typed files added to the candidate catalog as placeholders: "
        f"{result.invalid_target_rows:,}",
    ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def large_sufficiency_paragraph(result: ProfileResult) -> str:
    total_target_resources = sum(result.resource_counts.values())
    patient_count = result.resource_counts.get("Patient", 0)

    resource_level_ok = total_target_resources >= 2000
    pilot_ok = (
        patient_count > 0
        and result.resource_counts.get("Observation", 0) > 0
        and result.resource_counts.get("Condition", 0) > 0
    )

    if not pilot_ok:
        return (
            "The Large dataset is not sufficient for pilot extraction because at least one target "
            "resource stratum is absent."
        )

    if resource_level_ok and patient_count >= 1000:
        return (
            "The Large dataset is clearly sufficient for pilot seed extraction and should comfortably "
            "cover resource-level experiments at 100, 200, 600, 1000, and 2000 cases. However, for a "
            "2,500-case primary pool it is better treated as the initial Synthea-derived backbone rather "
            f"than the sole contributor, because the observed pool only contains {patient_count:,} "
            "Patient resources even though the raw resource volume is much larger."
        )

    return (
        "The Large dataset is usable for pilot extraction, but the observed resource volume suggests that "
        "you should generate a larger custom dataset before depending on it for the full experimental plan."
    )


def write_large_dataset_summary(result: ProfileResult) -> None:
    output_path = result.output_dir / "large_dataset_summary.md"
    total_target_resources = sum(result.resource_counts.values())
    patient_count = result.resource_counts.get("Patient", 0)

    pilot_sufficient = (
        patient_count > 0
        and result.resource_counts.get("Observation", 0) > 0
        and result.resource_counts.get("Condition", 0) > 0
    )
    raw_volume_sufficient = total_target_resources >= 2500
    sole_contributor_ideal = raw_volume_sufficient and patient_count >= 2500

    if pilot_sufficient:
        pilot_answer = (
            "Yes. The Large dataset is sufficient for pilot seed extraction because all three target "
            "resource types are present at non-trivial volume."
        )
    else:
        pilot_answer = (
            "No. The Large dataset is not sufficient for pilot seed extraction because at least one "
            "target resource type is missing."
        )

    if sole_contributor_ideal:
        pool_answer = (
            "Yes. Based on the observed counts, Large appears strong enough to serve as the sole "
            "Synthea-derived contributor to a 2,500-case primary pool."
        )
    elif raw_volume_sufficient:
        pool_answer = (
            "Not ideally. Large appears sufficient in raw resource count, but it likely should not be the "
            "sole Synthea-derived contributor to a 2,500-case primary pool if you want broad patient-level "
            f"variety, because the observed pool contains {patient_count:,} Patient resources."
        )
    else:
        pool_answer = (
            "No. Large does not appear sufficient as the sole Synthea-derived contributor to a 2,500-case "
            "primary pool on raw candidate volume alone."
        )

    if raw_volume_sufficient and not sole_contributor_ideal:
        next_step = (
            "Yes. Generate a custom 2,000-patient dataset next if the downstream pool should reduce "
            "patient-level reuse and expand diversity before final sampling."
        )
    elif not raw_volume_sufficient:
        next_step = "Yes. Generate a custom 2,000-patient dataset next."
    else:
        next_step = (
            "Not necessarily, but a custom 2,000-patient dataset is still a good hedge if you want more "
            "patient-level heterogeneity."
        )

    lines = [
        "# Large Dataset Summary",
        "",
        f"- Target resource total: {total_target_resources:,}",
        f"- Patient count: {patient_count:,}",
        f"- Observation count: {result.resource_counts.get('Observation', 0):,}",
        f"- Numeric Observation count: {result.observation_numeric_count:,} "
        f"({format_pct(result.observation_numeric_count, result.observation_total_count)}%)",
        f"- Condition count: {result.resource_counts.get('Condition', 0):,}",
        "",
        "## Answers",
        "",
        f"- Is Large sufficient for pilot seed extraction? {pilot_answer}",
        f"- Is Large likely sufficient as the sole Synthea-derived contributor to the planned "
        f"2,500-case primary pool? {pool_answer}",
        f"- If not, should we generate a custom 2,000-patient dataset next? {next_step}",
    ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def print_run_summary(result: ProfileResult) -> None:
    print("Profiling complete.")
    print(f"Dataset directory: {result.dataset_dir}")
    print(f"Output directory: {result.output_dir}")
    print(
        "Target resource counts: "
        + ", ".join(
            f"{resource_type}={result.resource_counts.get(resource_type, 0):,}"
            for resource_type in TARGET_RESOURCE_TYPES
        )
    )
    print(
        "Observation numeric prevalence: "
        f"{result.observation_numeric_count:,}/{result.observation_total_count:,} "
        f"({format_pct(result.observation_numeric_count, result.observation_total_count)}%)"
    )


def main() -> int:
    parser = cli()
    args = parser.parse_args()

    try:
        if args.command == "download-large":
            dataset_dir = download_large_dataset(args.download_dir)
            print(dataset_dir)
            return 0

        if args.command == "profile-large":
            dataset_dir = download_large_dataset(args.download_dir)
            result = profile_dataset(dataset_dir=dataset_dir, output_dir=args.output_dir)
            print_run_summary(result)
            return 0

        if args.command == "profile-dir":
            result = profile_dataset(
                dataset_dir=args.dataset_dir,
                output_dir=args.output_dir,
            )
            print_run_summary(result)
            return 0

        if args.command == "generate-and-profile":
            if args.skip_generate:
                dataset_dir = resolve_dataset_dir(args.repo_dir / f"{args.patient_count}-patients")
            else:
                dataset_dir = run_generate(args.repo_dir, args.patient_count)
            result = profile_dataset(dataset_dir=dataset_dir, output_dir=args.output_dir)
            print_run_summary(result)
            return 0

    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
