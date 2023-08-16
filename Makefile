version ?= 2.1.1-pre.0

ci: clean deps lint test package

clean:
	rm -rf stage

deps:
	pip install -r requirements-dev.txt

lint:
	shellcheck scripts/*.sh
	# TODO: re-enable when all Pylint errors are cleaned up
	# pylint lambda/*.py


test:
	pytest --log-cli-level=info

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
