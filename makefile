.PHONY: \
		commit-stage test-stage deploy-stage \
		set-executable \
		pack pack-extract-metadata pack-route-raw pack-add-jobflow-steps \
		clean deepclean \
		clean-extract-metadata clean-route-raw clean-add-jobflow-steps \
		test functional-test \
		lint lint-lambdas lint-spark \
		deploy remove \
		pipenv deps deps-dev deps-prod prune-dev-deps \
		prep-target bootstrap \
		

ROOT = $(shell pwd)
APPNAME = tb-app-datalake

DIST = $(ROOT)/target/dist
DIST_EXTRACT_METADATA = $(DIST)/$(APPNAME)-extract-metadata.zip
DIST_ROUTE_RAW = $(DIST)/$(APPNAME)-route-raw.zip
DIST_ADD_JOBFLOW_STEPS = $(DIST)/$(APPNAME)-add-jobflow-steps.zip

REPORTS = $(ROOT)/target/reports
LINT_REPORT_LAMBDA = $(REPORTS)/lint_lambda.txt
LINT_REPORT_SPARK = $(REPORTS)/lint_spark.txt
LINT_REPORT_TESTS = $(REPORTS)/lint_tests.txt
UNITTEST_REPORT = $(REPORTS)/unittest.txt

LOGS = $(ROOT)/target/logs
PIP_INSTALL_LOG = $(LOGS)/pip.txt
PIPENV_PROD_LOG = $(LOGS)/pipenv-prod.txt
PIPENV_DEV_LOG = $(LOGS)/pipenv-dev.txt
PACK_EXTRACT_METADATA_LOG = $(LOGS)/pack_extract_metadata.txt
PACK_ROUTE_RAW_LOG = $(LOGS)/pack_route_raw.txt
PACK_ADD_JOBFLOW_STEPS_LOG = $(LOGS)/pack_add_jobflow_steps.txt

#
# Pipeline Rules
#

# You should run at least this rule and make sure it passes locally
# before opening a pull request to master. Preferably, also run the
# test-stage rule (which will be far more effective once we have tests)

commit-stage: clean
	@echo "=============================================================="
	@echo "== BEGIN: Pipeline Commit Stage"
	@echo "=============================================================="
	$(MAKE) bootstrap
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) pack
	@echo "=============================================================="
	@echo "== END: Pipeline Commit Stage"
	@echo "=============================================================="

# The pipeline will invoke the test-stage rule with an altered service
# name environment variable, suffixed with "test". If you are in a dev
# branch, it is safe to run this rule, as it will deploy to a stage
# named after your branch then test it, then remove the deployment.

test-stage:
	@echo "=============================================================="
	@echo "== BEGIN: Pipeline Test Stage"
	@echo "=============================================================="
	# $(MAKE) deploy
	# $(MAKE) functional-test
	# $(MAKE) remove
	@echo "=============================================================="
	@echo "== END: Pipeline Test Stage"
	@echo "=============================================================="

deploy-stage:
	@echo "=============================================================="
	@echo "== BEGIN: Pipeline Deploy Stage"
	@echo "=============================================================="
	$(MAKE) deploy
	@echo "=============================================================="
	@echo "== END: Pipeline Deploy Stage"
	@echo "=============================================================="

#
# Lambda packaging rules
#

pack: pack-extract-metadata pack-route-raw pack-add-jobflow-steps

pack-extract-metadata: clean-extract-metadata prep-target
	@echo '==> Packing extract_metadata lambda...'
	cd functions/extract_metadata; zip -9Dr $(DIST_EXTRACT_METADATA) * -x *.pyc tests/* | tee $(PACK_EXTRACT_METADATA_LOG)

pack-route-raw: clean-route-raw prep-target
	@echo '==> Packing route_raw lambda...'
	cd functions/route_raw; zip -9Dr $(DIST_ROUTE_RAW) * -x *.pyc tests/* | tee $(PACK_ROUTE_RAW_LOG)

pack-add-jobflow-steps: clean-add-jobflow-steps prep-target
	@echo '==> Packing add_jobflow_steps lambda...'
	cd functions/add_jobflow_steps; zip -9Dr $(DIST_ADD_JOBFLOW_STEPS) * -x *.pyc tests/* tests/*/* | tee $(PACK_ADD_JOBFLOW_STEPS_LOG)

#
# Cleaning Rules
#

clean:
	rm -rf target

deepclean: clean
	pipenv --rm

clean-extract-metadata:
	@echo '==> Cleaning old extract-metadata package...'
	rm -f $(DIST_EXTRACT_METADATA)

clean-route-raw:
	@echo '==> Cleaning old route-raw package...'
	rm -f $(DIST_ROUTE_RAW)

clean-add-jobflow-steps:
	@echo '==> Cleaning old add-jobflow-steps package...'
	rm -f $(DIST_ADD_JOBFLOW_STEPS)

#
# Test Rules
#

test: prep-target
	pipenv run python -m unittest discover -p '*_test.py' 2>&1 | tee $(UNITTEST_REPORT)

functional-test: set-executable
	bin/test.sh

#
# Linting Rules
#

lint: lint-lambdas lint-spark lint-tests

lint-lambdas: prep-target
	rm -f $(LINT_REPORT_LAMBDA)
	pipenv run flake8 functions --statistics --output-file=$(LINT_REPORT_LAMBDA) --tee

lint-spark: prep-target
	rm -f $(LINT_REPORT_SPARK)
	pipenv run flake8 spark --statistics --output-file=$(LINT_REPORT_SPARK) --tee --exit-zero # remove --exit-zero to fail build on lint fail

lint-tests: prep-target
	rm -f $(LINT_REPORT_TESTS)
	pipenv run flake8 tests --statistics --output-file=$(LINT_REPORT_TESTS) --tee

#
# Deployment Rules
#

deploy: set-executable
	bin/deploy.sh

remove: set-executable
	bin/remove.sh

#
# Dependency installation Rules
#

pipenv: prep-target
	pip install pipenv==9.0.1 2>&1 | tee $(PIP_INSTALL_LOG)

deps: deps-dev deps-prod

deps-dev: prep-target
	@echo '=> Installing development dependencies...'
	pipenv install --dev 2>&1 | tee $(PIPENV_DEV_LOG)

deps-prod: prep-target
	@echo '=> Installing production dependencies...'
	pipenv install | tee $(PIPENV_PROD_LOG)

prune-dev-deps:
	# pipenv uninstall --dev (this removes the dependencies from pipfile!)

#
# Setup Rules
#

prep-target:
	mkdir -p $(DIST)
	mkdir -p $(REPORTS)
	mkdir -p $(LOGS)

bootstrap: | pipenv deps

#
# Utility Rules
#

set-executable:
	chmod -c +x bin/*.sh
