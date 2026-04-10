# Large Dataset Summary

- Target resource total: 689,287
- Patient count: 1,132
- Observation count: 648,136
- Numeric Observation count: 533,816 (82.36%)
- Condition count: 40,019

## Answers

- Is Large sufficient for pilot seed extraction? Yes. The Large dataset is sufficient for pilot seed extraction because all three target resource types are present at non-trivial volume.
- Is Large likely sufficient as the sole Synthea-derived contributor to the planned 2,500-case primary pool? Not ideally. Large appears sufficient in raw resource count, but it likely should not be the sole Synthea-derived contributor to a 2,500-case primary pool if you want broad patient-level variety, because the observed pool contains 1,132 Patient resources.
- If not, should we generate a custom 2,000-patient dataset next? Yes. Generate a custom 2,000-patient dataset next if the downstream pool should reduce patient-level reuse and expand diversity before final sampling.
