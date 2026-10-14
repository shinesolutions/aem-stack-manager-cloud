version ?= 2.1.1-pre.0

define python_venv
	. .venv/bin/activate && $(1)
endef

ci: clean deps lint test package

clean:
	rm -rf stage
	rm -rf .venv
	rm -rf .pytest_cache

deps:
	python3 -m venv .venv
	$(call python_venv,python3 -m pip install -r requirements.txt)

deps-upgrade:
	python3 -m venv .venv
	$(call python_venv,python3 -m pip install -r requirements-dev.txt)
	$(call python_venv,pip-compile --upgrade)

rmdeps:
	rm -rf .venv

lint:
	shellcheck scripts/*.sh
	# TODO: re-enable when all Pylint errors are cleaned up
	# pylint lambda/*.py


test:
	python3 -m venv .venv
	$(call python_venv,pytest --log-cli-level=info)

package:
	mkdir -p stage
	zip \
	    --recurse-paths stage/aem-stack-manager-cloud-$(version).zip ./ \
	    --exclude='.git*' \
	    --exclude='.librarian*' \
	    --exclude='.tmp*' \
	    --exclude='stage*' \
	    --exclude='.idea*' \
	    --exclude='.DS_Store*' \
	    --exclude='logs*' \
	    --exclude='*.retry' \
	    --exclude='*.iml'

release:
	rtk release

.PHONY:  ci clean deps lint package release
