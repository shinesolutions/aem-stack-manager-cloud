version ?= 1.2.0

ci: clean deps lint package

clean:
	rm -rf stage

deps:
	pip install -r requirements.txt

lint:
	shellcheck scripts/*.sh

validate:
	for template in cloudformation/*.yaml; do \
		echo "checking template $${template} ...."; \
		aws cloudformation validate-template --template-body "file://$$template"; \
	done

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

.PHONY:  ci clean deps lint validate package
