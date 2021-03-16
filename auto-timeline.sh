#!/bin/bash

# username=elonmusk
# userid=44196397
# username=SnoopDogg
# userid=3004231

username=genesimmons
userid=14401698

curl -X GET -H "Authorization: Bearer $TWITTER_BEARER_TOKEN" "https://api.twitter.com/2/users/$userid/tweets?tweet.fields=public_metrics,created_at&max_results=100" > $username-1.json

cat $username-1.json | jq -r '.meta.next_token' >> pagetoken.txt

i=2

while [ `tail -1 pagetoken.txt` != "null" ]; do
	token=`tail -1 pagetoken.txt`

	curl -X GET -H "Authorization: Bearer $TWITTER_BEARER_TOKEN" "https://api.twitter.com/2/users/$userid/tweets?tweet.fields=public_metrics,created_at&max_results=100&pagination_token=$token" > $username-$i.json

	cat $username-$i.json | jq -r '.meta.next_token' >> pagetoken.txt

	((i++))
done

for i in {1..33}; do
	cat $username-$i.json | jq -c '.data[]' >> $username-tweets-3200.json
done

