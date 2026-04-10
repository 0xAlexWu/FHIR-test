PYTHON ?= python3
PROFILE_SCRIPT := scripts/fhir_pool_profiler.py

.PHONY: profile-large profile-dir generate-2000-and-profile

profile-large:
	$(PYTHON) $(PROFILE_SCRIPT) profile-large --output-dir outputs/large

profile-dir:
	@if [ -z "$(DATASET_DIR)" ]; then echo "DATASET_DIR is required"; exit 2; fi
	@if [ -z "$(OUTPUT_DIR)" ]; then echo "OUTPUT_DIR is required"; exit 2; fi
	$(PYTHON) $(PROFILE_SCRIPT) profile-dir --dataset-dir "$(DATASET_DIR)" --output-dir "$(OUTPUT_DIR)"

generate-2000-and-profile:
	@if [ -z "$(REPO_DIR)" ]; then echo "REPO_DIR is required"; exit 2; fi
	$(PYTHON) $(PROFILE_SCRIPT) generate-and-profile --repo-dir "$(REPO_DIR)" --patient-count 2000 --output-dir outputs/custom-2000
