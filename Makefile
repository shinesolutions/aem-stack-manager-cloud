version ?= 2.0.0

# development targets

ci: clean deps lint package

clean:

deps:
	pip install -r requirements.txt

lint:
	shellcheck scripts/*.sh

validate:
	for template in cloudformation/*.yaml; do \
		echo "checking template $${template} ...."; \
		aws cloudformation validate-template --template-body "file://$$template"; \
	done

# utility targets

package:
	rm -rf stage
	mkdir -p stage
	tar \
	    --exclude='.git*' \
	    --exclude='.librarian*' \
	    --exclude='.tmp*' \
	    --exclude='stage*' \
	    --exclude='.idea*' \
	    --exclude='.DS_Store*' \
	    --exclude='logs*' \
	    --exclude='*.retry' \
	    --exclude='*.iml' \
	    -cvf \
	    stage/aem-stack-manager-cloud-$(version).tar ./
	gzip stage/aem-stack-manager-cloud-$(version).tar

.PHONY:  ci clean deps lint validate package
