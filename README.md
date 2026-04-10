# FHIR Source Pool Profiling

This repository profiles SMART on FHIR sample bulk datasets for `Patient`,
`Observation`, and `Condition` prevalence so pilot seed quotas and later
training-pool design can follow the observed source pool instead of fixed
ratios.

## Included Results

- Verified Medium profiling outputs remain under `outputs/medium/`.
- Verified Large profiling outputs now live under `outputs/large/`.
- The raw Large `candidate_seed_catalog.csv` is generated locally but excluded
  from version control because it exceeds GitHub's regular file-size limit.
- A GitHub-friendly `candidate_seed_catalog.csv.gz` is checked in for the Large
  run instead.

## Main Commands

Profile the pre-generated Large dataset with the new CLI:

```bash
python3 scripts/fhir_pool_profiler.py profile-large --output-dir outputs/large
```

Equivalent `make` target:

```bash
make profile-large
```

Profile any already-downloaded dataset directory:

```bash
python3 scripts/fhir_pool_profiler.py profile-dir \
  --dataset-dir /path/to/sample-bulk-fhir-datasets-1000-patients \
  --output-dir outputs/large
```

Generate and profile a custom 2,000-patient dataset later:

```bash
python3 scripts/fhir_pool_profiler.py generate-and-profile \
  --repo-dir /path/to/sample-bulk-fhir-datasets \
  --patient-count 2000 \
  --output-dir outputs/custom-2000
```

Equivalent `make` target:

```bash
make generate-2000-and-profile REPO_DIR=/path/to/sample-bulk-fhir-datasets
```

If you want to run the upstream generator yourself first, that is supported too:

```bash
cd /path/to/sample-bulk-fhir-datasets
./generate.sh 2000
cd /Users/dongfang/Desktop/fhir-test-1000
python3 scripts/fhir_pool_profiler.py profile-dir \
  --dataset-dir /path/to/sample-bulk-fhir-datasets/2000-patients \
  --output-dir outputs/custom-2000
```

## Legacy Medium Workflow

The repository also keeps the earlier Medium-focused script:

```bash
python3 scripts/profile_bulk_fhir_pool.py --dataset medium --download
```

and supports reusing an extracted Medium dataset directory:

```bash
python3 scripts/profile_bulk_fhir_pool.py \
  --input-dir data/raw/sample-bulk-fhir-datasets-100-patients \
  --output-dir outputs/medium
```

## Output Files

The Large profiler writes:

- `resource_counts_summary.csv`
- `observation_profile_summary.csv`
- `candidate_seed_catalog.csv`
- `candidate_seed_catalog.csv.gz`
- `pilot_quota_recommendation.md`
- `large_dataset_summary.md`

## Profiling Rules

- Only `Patient`, `Observation`, and `Condition` resources are retained.
- Numeric Observation detection is conservative and currently flags:
  - `valueQuantity.value`
  - `valueInteger`
  - `valueDecimal`
  - the same numeric fields inside `component`
- `needs_linked_context` and `complexity_guess` are heuristic planning aids
  based on references and structural signals.
