version ?= 2.1.1-pre.0
package_name ?= aem-stack-manager-cloud

define python_venv
	. .venv/bin/activate && $(1)
endef

ci: clean deps lint test package

clean:
	rm -rf stage

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
	pylint lambda/*.py


test:
	python3 -m venv .venv
	$(call python_venv,pytest --log-cli-level=info)

package:
	mkdir -p stage
	zip \
	    --recurse-paths stage/$(package_name)-$(version).zip ./ \
	    --exclude='.git*' \
	    --exclude='.librarian*' \
	    --exclude='.tmp*' \
	    --exclude='stage*' \
	    --exclude='.idea*' \
	    --exclude='.DS_Store*' \
	    --exclude='logs*' \
	    --exclude='*.retry' \
	    --exclude='*.iml' \
	    --exclude='lambda/__pycache__*' \
	    --exclude='.pytest_cache*' \
	    --exclude='.venv*'

release:
	rtk release

publish:
	gh release create $(version) --title $(version) --notes "" || echo "Release $(version) has been created on GitHub"
	gh release upload $(version) stage/$(package_name)-$(version).tar.gz

.PHONY:  ci clean deps lint package release publish
