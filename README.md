# cs523
cs523 Big Data Technology Final Project.

###Description
this project will consume twitter tweets and stream it to Kafka topic
then the consumer will consume those tweets streams and save it to Hive table
by transform it with the number of words and tweet length.

after each batch insert it will find out the largest tweet by words in hive

## Environment
I used docker to setup the cluster

### Requirments
- Anaconda2(python 3.7)
- Docker

###Running the project
First you need to run the docker compose image by `docker-compose up` command

### Stream data to Kafka
to stream twitter data to Kafka you need to run producer script
``` 
$ python code/producer.py
```

### submit spark application

to consume data stream from kafka and save it in hive table 
 get into spark-master docker container bash and execute this command

```
$  /spark/bin/spark-submit  --jars /apps/spark-streaming-kafka-0-8-assembly_2.11-2.4.8.jar  /apps/consumer.py 
```




Salah Khdairat
