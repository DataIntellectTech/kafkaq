# kafkaq
q language bindings for Apache Kafka based on librdkafka


## Installation

- Install [librdkafka](https://github.com/edenhill/librdkafka)
- Download/close this repo and navigate to the kafkaq directory
- On Linux, compile with the following:
```
gcc -O2 -fPIC -shared -Wall kafkaq.c -o kafkaq.so -lrdkafka -lpthread
```

### Kakfa Installation

To test with a local instance of kafka (more details [here](https://kafka.apache.org/quickstart)):

```
> # Download kafka package
> wget http://apache.mirrors.nublue.co.uk/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz
> # Extract package and navigate to directory
> tar -xzf kafka_2.11-0.10.2.0.tgz
> cd kafka_2.11-0.10.2.0
> # start zookeeper server
> bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
> # start kafka server
> bin/kafka-server-start.sh -daemon config/server.properties
> # create a topic called 'test'
> bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```

### Usage Example

The included q file kafka.q assumes the kafkaq.so library is available in the same folder
In this example q is acting as both producer and consumer via the local kafka instance started above, a more realistic scenario q would be either producer or consumer but not both.

```
q)\l kafka.q
q)initconsumer[`localhost:9092;()]
q)initproducer[`localhost:9092;()]
q)kupd / print default definition for incoming data - ignore key, print message as ascii
{[k;x] -1 `char$x;}
q)subscribe[`test;0] / subscribe to topic test, partition 0
q)pub:{publish[`test;0;`;`byte$x]} / define pub to publish text input to topic test on partition 0 with no key defined
q)pub"hello world"
q)hello world

q)
```

### Notes

### Performance
