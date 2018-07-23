# @file clear.sh
# @description CAUTION: Running this DELETES ALL ITEMS from the specified DynamoDB title

TABLE_NAME=RecallsTestData-EN
KEY=recallId

read -p "Are you sure? This will delete all items in 'RecallsTestData-EN'. Continue? [y/n] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
  aws dynamodb scan --table-name $TABLE_NAME --attributes-to-get "$KEY" \
    --query "Items[].$KEY.S" --output text | \
    tr "\t" "\n" | \
    xargs -t -I keyvalue aws dynamodb delete-item --table-name $TABLE_NAME \
    --key "{\"$KEY\": {\"S\": \"keyvalue\"}}"
else
  echo "ABORTING DELETE!"
fi
