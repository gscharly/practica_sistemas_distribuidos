# Description

The script tweets_sentiments.py uses the MapReduce distributed computing paradigm to perform sentiment analysis using tweets collected through the Twitter API. Specifically, it utilizes MRJob, which allows simulating the behavior of a distributed system locally, as well as running on real distributed environments (such as Cloudera) and with the AWS EMR service

# How to use it?
It has 3 operating modes, specified by the --job-options parameter in the execution command:

- sentiments: calculates the aggregated sentiments of each community/province in Spain.
- most-happy: calculates the happiest community/province based on previous sentiment scores.
- trending: prints the top 10 trending topics that are most frequently mentioned.

## Locally
python tweets_sentiments.py -r local tweets.json --file Redondo_words.txt --file comunidades.json --job-options sentiments

## Using EMR AWS
python tweets_sentiments.py -r emr s3://bucket/tweets.json --file s3://bucket/Redondo_words.txt
--file s3://bucket/comunidades.json --output-dir=s3://bucket/output --job-options sentiments
