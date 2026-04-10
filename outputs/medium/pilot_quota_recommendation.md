# Pilot Quota Recommendation

## Empirical pool distribution
Across 74,647 retained target resources in the medium dataset, the empirical distribution is Patient 120 (0.1608%), Observation 70,233 (94.0868%), and Condition 4,294 (5.7524%).

## Numeric Observation prevalence
Observation resources total 70,233. Of those, 58,312 are conservatively flagged as likely numeric (83.0265%), and 58,309 include an explicit unit (83.0222%).
By simple reference scanning, resources that reference at least one other resource are Patient 0/120, Observation 70,233/70,233, and Condition 4,294/4,294. The linked-context heuristic is intentionally stricter for Observation and Condition than for Patient.

## Recommended sampling plan
Use the observed pool distribution as the default set of strata weights, then convert those weights to integer quotas with largest-remainder rounding.
For an example pilot of 100 total seeds, pure proportional quotas would be: Patient 0, Observation 94, Condition 6.
Because Patients are extremely rare in the retained pool, strict proportional sampling does not reliably produce a non-zero Patient quota until roughly 623 total seeds. If the pilot needs all three resource types represented, use a mild coverage floor instead of a hard fixed ratio.
Condition becomes reliably non-zero under pure proportional sampling at roughly 18 total seeds.
A practical floor-constrained version at 100 total seeds is: Patient 1, Observation 92, Condition 7. This preserves the Observation-heavy shape while avoiding a zero-Patient pilot.

## Optional Observation oversampling for debugging
If early pipeline debugging benefits from more Observation examples, increase the Observation weight by about 1.10x, renormalize, and re-apportion. For the same pilot size, that would produce: Patient 0, Observation 95, Condition 5.
Because Observations already dominate this source pool, additional Observation oversampling is best treated as a debug-only option. If the debugging target is numeric value handling, prefer sampling that extra Observation share from the likely-numeric subset rather than from all Observations.

## Interpretation
The empirical source pool is overwhelmingly Observation-heavy, with Condition a distant second and Patient extremely sparse. That means the default pilot quota should follow the empirical distribution when representativeness matters, but a mildly adjusted version with a tiny Patient floor is usually the safer choice for small pilots that must exercise all resource-specific code paths.
