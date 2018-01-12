.PHONY: pack-lambdas test lint deps deps-dev deps-prod pipenv bootstrap \
		pack-extract-metadata pack-route-raw pack-start-job-store \
		clean-extract-metadata clean-route-raw clean-start-job-store \
		commit-stage

ROOT = $(shell pwd)
APPNAME = tb-app-datalake

DIST = $(ROOT)/target/dist
DIST_EXTRACT_METADATA = $(DIST)/$(APPNAME)-extract-metadata.zip
DIST_ROUTE_RAW = $(DIST)/$(APPNAME)-route-raw.zip
DIST_START_JOB_STORE = $(DIST)/$(APPNAME)-start-job-store.zip

REPORTS = $(ROOT)/target/reports
LINT_REPORT_LAMBDA = $(REPORTS)/lint_lambda.txt
LINT_REPORT_SPARK = $(REPORTS)/lint_spark.txt
UNITTEST_REPORT = $(REPORTS)/unittest.txt

LOGS = $(ROOT)/target/logs
PIP_INSTALL_LOG = $(LOGS)/pip.txt
PIPENV_PROD_LOG = $(LOGS)/pipenv-prod.txt
PIPENV_DEV_LOG = $(LOGS)/pipenv-dev.txt
PACK_EXTRACT_METADATA_LOG = $(LOGS)/pack_extract_metadata.txt
PACK_ROUTE_RAW_LOG = $(LOGS)/pack_route_raw.txt
PACK_START_JOB_STORE_LOG = $(LOGS)/pack_start_job_store.txt


commit-stage: | clean bootstrap lint test pack

clean:
	rm -rf target

deepclean: clean
	pipenv --rm

pack: pack-extract-metadata pack-route-raw pack-start-job-store

pack-extract-metadata: clean-extract-metadata prep-target
	@echo '==> Packing extract_metadata lambda...'
	cd functions/extract_metadata; zip -9Dr $(DIST_EXTRACT_METADATA) * -x *.pyc **/*.pyc tests/* test_data/* cf.py | tee $(PACK_EXTRACT_METADATA_LOG)

pack-route-raw: clean-route-raw prep-target
	@echo '==> Packing route_raw lambda...'
	cd functions/route_raw; zip -9Dr $(DIST_ROUTE_RAW) * -x *.pyc **/*.pyc tests/* test_data/* cf.py | tee $(PACK_ROUTE_RAW_LOG)

pack-start-job-store: clean-start-job-store prep-target
	@echo '==> Packing start_job_store lambda...' | tee $(PACK_START_JOB_STORE_LOG)
	cd functions/start_job_store; zip -9Dr $(DIST_START_JOB_STORE) * -x *.pyc **/*.pyc tests/* test_data/* cf.py | tee -a $(PACK_START_JOB_STORE_LOG)
	# need to remove the dev dependencies (but not remove them from the pipfile)
	# not including dependencies because only production dependency is boto3, which is already installed on the lambda image
	#@echo '--> Adding dependencies from virtual env...' | tee -a $(PACK_START_JOB_STORE_LOG)
	#cd $(shell pipenv --venv)/lib/python2.7/site-packages; zip -9r $(DIST_START_JOB_STORE) * | tee -a $(PACK_START_JOB_STORE_LOG)

prep-target:
	mkdir -p $(DIST)
	mkdir -p $(REPORTS)
	mkdir -p $(LOGS)

clean-extract-metadata:
	@echo '==> Cleaning old extract-metadata package...'
	rm -f $(DIST_EXTRACT_METADATA)

clean-route-raw:
	@echo '==> Cleaning old route-raw package...'
	rm -f $(DIST_ROUTE_RAW)

clean-start-job-store:
	@echo '==> Cleaning old start-job-store package...'
	rm -f $(DIST_START_JOB_STORE)

prune-dev-deps:
	# pipenv uninstall --dev

test: prep-target
	pipenv run python -m unittest discover -p '*_test.py' 2>&1 | tee $(UNITTEST_REPORT)

lint: lint-lambdas lint-spark

lint-lambdas: prep-target
	rm -f $(LINT_REPORT_LAMBDA)
	pipenv run flake8 functions --statistics --output-file=$(LINT_REPORT_LAMBDA) --tee --exit-zero # remove --exit-zero to fail build on lint fail

lint-spark: prep-target
	rm -f $(LINT_REPORT_SPARK)
	pipenv run flake8 spark --statistics --output-file=$(LINT_REPORT_SPARK) --tee --exit-zero # remove --exit-zero to fail build on lint fail

deps: deps-dev deps-prod

deps-dev: prep-target
	@echo '=> Installing development dependencies...'
	export PIPENV_NOSPIN=true; pipenv install --dev 2>&1 | tee $(PIPENV_DEV_LOG)

deps-prod: prep-target
	@echo '=> Installing production dependencies...'
	export PIPENV_NOSPIN=true; pipenv install | tee $(PIPENV_PROD_LOG)

pipenv: prep-target
	pip install pipenv 2>&1 | tee $(PIP_INSTALL_LOG)

bootstrap: | pipenv deps
