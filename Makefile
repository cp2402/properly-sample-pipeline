.DEFAULT_GOAL := help
SHELL=/bin/bash

help: ## display this message
	@echo "usage: make [target]"
	@echo
	@echo "available targets:"
	@grep -h "##" $(MAKEFILE_LIST) | grep -v grep  | column -t -s '##'
	@echo


.PHONY: init
init: ## setup environment
	@echo "Initalizing environment..."
	mkdir -p  ./dags ./logs ./data ./plugins
	echo -e "AIRFLOW_UID=$(shell id -u)" > .env
	@echo "Initalizing airflow database..."
	docker-compose up airflow-init

.PHONY: start
start: ## start service
	docker-compose up

.PHONY: stop
stop: ## stop service
	docker-compose down

.PHONY: clean
clean: ## stop and remove service and env
	docker-compose down --volumes --remove-orphans