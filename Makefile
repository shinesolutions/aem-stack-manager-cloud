version ?= 1.3.5

ci: clean deps lint package

clean:
	rm -rf stage

deps:
	pip install -r requirements.txt

lint:
	shellcheck scripts/*.sh
	# TODO: re-enable when all Pylint errors are cleaned up
	# pylint lambda/*.py

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
