#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PROJECT_NAME := eu-jobs-python
VENV = $(PROJECT_DIR)/.venv
PIP = $(VENV)/bin/pip
PYTHON ?= python3.7
VIRTUALENV = $(PYTHON) -m venv
SHELL=/bin/bash
TESTS_DIR=./tests

#################################################################################
# virtual environment and dependencies                                          #
#################################################################################

.PHONY: venv
## create virtual environment
venv: ./.venv/.requirements

.venv:
	$(VIRTUALENV) $(VENV)
	$(PIP) install -U pip setuptools wheel

.venv/.requirements: .venv
	$(PIP) install -r $(PROJECT_DIR)/requirements.txt
	$(PIP) install -r $(PROJECT_DIR)/requirements-dev.txt
	touch $(VENV)/.requirements

.PHONY: venv-clean
## clean virtual environment
venv-clean:
	rm -rf $(VENV)

#################################################################################
# code format / code style                                                      #
#################################################################################

.PHONY: format-check
## check compliance with code style (via 'black')
format-check: .venv/.requirements
	$(VENV)/bin/black --check $(PROJECT_DIR)/ $(TESTS_DIR)/

.PHONY: format-apply
## reformat code for compliance with code style (via 'black')
format-apply: venv
	$(VENV)/bin/black $(PROJECT_DIR)/ $(TESTS_DIR)/

#################################################################################
# Tests                                                                         #
#################################################################################

.PHONY: test
test: venv
## run pytest
	@PYTHONPATH=$(PYTHONPATH):$(PROJECT_DIR) $(VENV)/bin/pytest $(PROJECT_DIR) -v
