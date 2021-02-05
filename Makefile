#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME := data-toolz

PROJECT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

#################################################################################
# pipenv management                                                             #
#################################################################################

.PHONE: env-lock
env-lock:
	pipenv lock

.PHONY: env-dev
env-dev:
	PIPENV_VENV_IN_PROJECT=1 pipenv install --dev

.PHONY: env-rm
env-rm:
	pipenv --rm

#################################################################################
# code test / lint                                                              #
#################################################################################

.PHONY: format-check
format-check:
	pipenv run black --check $(PROJECT_DIR)

.PHONY: format-apply
format-apply:
	pipenv run black $(PROJECT_DIR)

.PHONY: test
test:
	pipenv run pytest $(PROJECT_DIR)

.PHONY: lint
lint:
	pipenv run pylint --rcfile=setup.cfg $(PROJECT_DIR)
