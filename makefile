.PHONY: pack-lambdas test lint deps deps-dev deps-prod pipenv bootstrap \
		pack-extract-metadata pack-route-raw pack-start-job-store \
		clean-extract-metadata clean-route-raw clean-start-job-store

ROOT = $(shell pwd)
DIST_EXTRACT_METADATA = $(ROOT)/dist/extract_metadata.zip
DIST_ROUTE_RAW = $(ROOT)/dist/route_raw.zip
DIST_START_JOB_STORE = $(ROOT)/dist/start_job_store.zip

pack-lambdas: pack-extract-metadata pack-route-raw pack-start-job-store

pack-extract-metadata: clean-extract-metadata prep-dist
	@echo '==> Packing extract_metadata lambda...'

pack-route-raw: clean-route-raw prep-dist
	@echo '==> Packing route_raw lambda...'

pack-start-job-store: clean-start-job-store prep-dist
	@echo '==> Packing start_job_store lambda...'
	cd functions/start_job_store; zip -9Dr $(DIST_START_JOB_STORE) * -x *.pyc **/*.pyc tests/* test_data/* cf.py
	# need to remove the dev dependencies (but not remove them from the pipfile)
	cd $(shell pipenv --venv)/lib/python2.7/site-packages; zip -q9r $(DIST_START_JOB_STORE) *

prep-dist:
	mkdir -p dist

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

test: 
	pipenv run python -m unittest discover -p '*_test.py'

lint:
	pipenv run flake8

lint-lambdas:
	pipenv run flake8 functions

deps: deps-dev deps-prod

deps-dev:
	@echo '=> Installing development dependencies...'
	pipenv install --dev

deps-prod:
	@echo '=> Installing production dependencies...'
	pipenv install

pipenv:
	pip install pipenv

bootstrap: | pipenv deps
