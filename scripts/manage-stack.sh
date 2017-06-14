#!/bin/bash

set -o nounset
set -o errexit

if [[ "$#" -gt 1 ]]; then
  echo "Usage: ${0} [config_path]"
  exit 1
fi

if [[ ${0} =~ create-stack ]]; then
  tag=create
  action_verb=creating
elif [[ ${0} =~ delete-stack ]]; then
  tag=delete
  action_verb=deleting
else
  echo "This script must be called as 'create-stack' or 'delete-stack'"
  exit 1
fi

config_paths=()
if [[ "$#" -gt 0 ]]; then
  IFS=':' read -ra temp_config_paths <<< "${1}"
  for p in "${temp_config_paths[@]}"; do
    if [[ -n "${p}" ]]; then
      config_paths+=( "${p}" )
    fi
  done
fi

run_id=${RUN_ID:-$(date +%Y-%m-%d:%H:%M:%S)}
log_path=logs/"${run_id}-${tag}".log

# Construct Ansible extra_vars flags. If `config_path` is set, all files
# directly under the directory with extension `.yaml` or `.yml` will be added.
# The search for config files _will not_ descend into subdirectories.

if [[ ${#config_paths[@]} -gt 0 ]]; then
  OIFS="${IFS}"
  IFS=$'\n'
  for d in "${config_paths[@]}"; do
    for config_file in $( find -L "${d}" -maxdepth 1 -type f -a \( -name '*.yaml' -o -name '*.yml' \) | sort ); do
      echo "  Adding extra vars from file: ${config_file}"
      extra_vars+=(--extra-vars "@$config_file")
    done
  done

  IFS="${OIFS}"
fi

mkdir -p "logs"
echo "Start ${action_verb} AEM Stack Manager Cloud native implementation stack"

if [ -z "${extra_vars+x}" ]; then
  ANSIBLE_LOG_PATH=$log_path \
    ansible-playbook -v ansible/playbooks/aem-stack-manager-cloud.yaml \
    -i ansible/inventory/hosts \
    --tags "${tag}"
else
  ANSIBLE_LOG_PATH=$log_path \
    ansible-playbook -v ansible/playbooks/aem-stack-manager-cloud.yaml \
    -i ansible/inventory/hosts \
    --tags "${tag}" \
    "${extra_vars[@]}"
fi

echo "Finished ${action_verb} aem stack manger cloud stack"
