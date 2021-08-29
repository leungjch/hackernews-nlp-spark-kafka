# Hacker News Real-time NLP Pipeline

# Running
First, setup the Kafka broker:
```shell
cd kafka-docker
docker-compose -f docker-compose-expose.yml up
```

Then, run the Kafka producer:
```shell
cd stream/producer
mvn spring-boot:run
```

Lastly, run the Spark compute pipeline (a Kafka consumer)
```shell
cd process/app
sbt run
```

The Spark UI is at [http://localhost:4040](http://localhost:4040)

# Debugging memory errors
If you run into problems with OutOfMemoryError, try running
```shell
export SBT_OPTS="-Xms1024M -Xmx4G -Xss2M -XX:MaxMetaspaceSize=2G"
```
