# FHIR Source Pool Profiling

This workspace profiles the SMART on FHIR sample bulk dataset for `Patient`, `Observation`, and `Condition` prevalence so pilot seed quotas can follow the observed source pool.

## Run

Download and profile the Medium dataset:

```bash
python3 scripts/profile_bulk_fhir_pool.py --dataset medium --download
```

Reuse an already extracted dataset directory:

```bash
python3 scripts/profile_bulk_fhir_pool.py \
  --input-dir data/raw/sample-bulk-fhir-datasets-100-patients \
  --output-dir outputs/medium
```

Profile the Large dataset with the same pipeline:

```bash
python3 scripts/profile_bulk_fhir_pool.py --dataset large --download --output-dir outputs/large
```

## Outputs

Each run writes:

- `resource_counts_summary.csv`
- `observation_profile_summary.csv`
- `candidate_seed_catalog.csv`
- `pilot_quota_recommendation.md`

The default output location is `outputs/<dataset>/`.

The repository keeps the verified Medium profiling outputs under `outputs/medium/` for inspection, while downloaded raw dataset files under `data/` stay local and are excluded from version control.
