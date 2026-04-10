# FHIR Source-Pool Profiling

This workspace profiles the SMART sample bulk FHIR datasets so pilot seed quotas
and later training-pool design can be grounded in observed prevalence rather than
fixed ratios.

## Commands

Profile the pre-generated Large dataset:

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

If you prefer to run the upstream generator yourself first, this remains
supported too:

```bash
cd /path/to/sample-bulk-fhir-datasets
./generate.sh 2000
cd /Users/dongfang/Desktop/fhir-test-1000
python3 scripts/fhir_pool_profiler.py profile-dir \
  --dataset-dir /path/to/sample-bulk-fhir-datasets/2000-patients \
  --output-dir outputs/custom-2000
```

## Output Files

Each profiling run writes:

- `resource_counts_summary.csv`
- `observation_profile_summary.csv`
- `candidate_seed_catalog.csv`
- `candidate_seed_catalog.csv.gz`
- `pilot_quota_recommendation.md`
- `large_dataset_summary.md`

For GitHub-friendly publication, the compressed `candidate_seed_catalog.csv.gz`
is intended to be checked in, while the raw `candidate_seed_catalog.csv` can stay
local because it may exceed GitHub's regular file-size limit.

## Profiling Rules

- Only `Patient`, `Observation`, and `Condition` resources are retained.
- Numeric Observation detection is conservative and currently flags:
  - `valueQuantity.value`
  - `valueInteger`
  - `valueDecimal`
  - the same numeric fields inside `component`
- `needs_linked_context` and `complexity_guess` are heuristic planning aids
  based on references and structural signals.
