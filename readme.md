Use Twitter website to create a developer account and then create a token under it.

https://developer.twitter.com/en/portal/projects/new

**start docker using docker compose** 

inside docker-compose folder execute below command

`docker-compose -f common.yml -f kafka-cluster.yml up`

To Monitor Kafka Cluster KafkaCast Utility

https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html

kafkacat is a command line utility that you can use to test and debug Apache Kafka® deployments. 
You can use kafkacat to produce, consume, and list topic and partition information for Kafka. 
Described as “netcat for Kafka”, 
it is a swiss-army knife of tools for inspecting and creating data in Kafka.

Run KafkaCat Locally

https://hub.docker.com/r/confluentinc/cp-kafkacat

`docker run -it --network=host confluentinc/cp-kafkacat kafkacat -L -b localhost:19092`

To Check List Of Brokers are running in kafka cluster.

`kafkacat -L -b localhost:19092`

We are using Avro Serialization 

https://avro.apache.org/docs/current/spec.html

https://avro.apache.org/docs/current/gettingstartedjava.html










