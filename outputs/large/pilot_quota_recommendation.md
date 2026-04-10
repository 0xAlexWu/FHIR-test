# Pilot Quota Recommendation

## Empirical Target-Resource Distribution

- Total retained target resources: 689,287
- Patient: 1,132 (0.16%)
- Observation: 648,136 (94.03%)
- Condition: 40,019 (5.81%)

## Numeric Observation Prevalence

- Total Observations: 648,136
- Numeric Observations: 533,816 (82.36%)
- Observations with units: 533,816 (82.36%)

## Recommended Pilot Sampling Strategy

Use approximately proportionate stratified sampling as the default rule.
For any pilot size N, allocate each stratum as round(N x empirical resource share), while keeping a minimum of one example per available stratum when N is small.

Example allocation for a 100-seed pilot using the observed Large-pool mix:

- Patient: 1
- Observation: 92
- Condition: 7

### Optional Mild Observation Oversampling

For early pipeline debugging, a modest Observation oversample is reasonable because Observations are usually the most schema-diverse and numerics are often especially useful for validation. A simple rule is to apply a 1.10x weight to Observation, renormalize, and avoid pushing the Observation share so high that Patient and Condition coverage erodes.

Example allocation for a 100-seed debugging pass with mild Observation oversampling:

- Patient: 1
- Observation: 93
- Condition: 6

## Sufficiency Assessment for Downstream Experiments

The Large dataset is clearly sufficient for pilot seed extraction and should comfortably cover resource-level experiments at 100, 200, 600, 1000, and 2000 cases. However, for a 2,500-case primary pool it is better treated as the initial Synthea-derived backbone rather than the sole contributor, because the observed pool only contains 1,132 Patient resources even though the raw resource volume is much larger.

## Profiling Notes

- Numeric Observation detection is conservative: it flags `valueQuantity.value`, `valueInteger`, `valueDecimal`, and the same fields under `component` when present.
- `needs_linked_context` and `complexity_guess` are heuristic signals driven by reference patterns and resource structure; they are intended for seed-planning triage, not formal labeling.
- Invalid JSON lines seen across all NDJSON files: 0
- Invalid JSON lines in target-typed files added to the candidate catalog as placeholders: 0
