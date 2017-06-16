#!/bin/bash

set -o nounset
set -o errexit


if [ "$#" -ne 2 ]; then
  echo "Usage: $0 function-name externalId"
  exit 1
fi

FUNC="$1"
EXTID="$2"

cat > payload.json << PAYLOAD
{
  "externalId": "${EXTID}"
}
PAYLOAD

echo "Querying latest task staus with function $FUNC and index value $EXTID"
for (( index = 0; index < 720; index++ )); do
  aws lambda invoke --function-name "$FUNC" --invocation-type RequestResponse \
      --payload file://payload.json response.json

  status=$(jq -r '.status' response.json)
  echo "Query $index returned status: $status."

  if [ "$status" = 'Success' ] || [ "$status" = 'Failed' ]; then
    break
  fi

  sleep 5
done

rm -f payload.json response.json

if [ "$status" != 'Success' ]; then
  echo "Task has eitehr failed or not reached \"Success\" status after one hour. Please check"
  exit 1
fi
