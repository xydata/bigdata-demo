
######This Project is a folk of mkrcah/scala-kafka-twitter######

### Demonstration project to monitor Twitter stream and identify on unexpected increases in tweet volume 

This is WIP.
[Getting Started](https://github.com/xydata/bigdata-demo#getting-started)

Current infrastructure:
- TweetCollector serializes Tweets (without code generation) to Avro and sent to Kafka
- TweetAnalyzer picks up serialized Tweets and monitor tweets for unexpected volume in Spark
- Volume thresholds and detected alerts managed in HDFS

![image](https://docs.google.com/drawings/d/1jVxh473mabBTm5tDIDd7dRRDgrNGorXefEp6ZDdyIyc/pub?w=960&h=720)

### Getting started

1. Get Twitter credentials and fill them in `reference.conf.example` and rename to `reference.conf`

2. Start Kafka [(instructions)](http://kafka.apache.org/documentation.html#introduction) in single-node mode on localhost

3. Start TweetCollector
```
./gradlew collect 
```
This will start to read recent tweets, encode them to Avro and send to the Kafka cluster in binary format (`Array[Byte]`). 

4. Start TweetAnalyzer
```
 ./gradlew analyze
```
This will run Spark streaming connected to the Kafka queue. In 5-second intervals 
the program reads tweets from Kafka, analyzes the tweet texts 
and print the 10 most tweeted company of the 400 S & P
 