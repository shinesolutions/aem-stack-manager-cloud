version ?= 2.0.0

# development targets

ci: clean deps lint package

clean:
	rm -f ansible/playbooks/*.retry

deps:
	pip install -r requirements.txt

lint:
	shellcheck scripts/*.sh
	for playbook in ansible/playbooks/*.yaml; do \
		ansible-playbook -vvv $$playbook --syntax-check; \
	done

validate:
	for template in cloudformation/*.yaml; do \
		echo "checking template $${template} ...."; \
		aws cloudformation validate-template --template-body "file://$$template"; \
	done

# stacks set management targets
create-stack-manager-cloud:
	./scripts/create-stack.sh  $(config_path)

delete-stack-manager-cloud:
	./scripts/delete-stack.sh  $(config_path)

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
