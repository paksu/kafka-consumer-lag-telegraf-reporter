# kafka-consumer-lag-telegraf-reporter

A script to collect metrics from Kafka consumer group offsets and lag outputs InfluxDB Line protocol. Designed to work with Telegraf exec plugin.

https://docs.influxdata.com/influxdb/v0.9/write_protocols/line/
https://github.com/influxdata/telegraf/tree/master/plugins/inputs/exec

Works with Kafka 0.9 and Telegraf 0.13

**This script should be run on the Kafka broker node**

If you find this tool useful please let me know https://twitter.com/joonapaak
### Usage

With kafka new consumer (>0.9):
```
python kafka_consumer_lag_reporter.py --kafka-dir=/path/to/kafka_2.11-0.9.0.1 --group some_group --bootstrap-server kafka-broker-host.com:9092
```

With old kafka consumer:
```
python kafka_consumer_lag_reporter.py --kafka-dir=/path/to/kafka_2.11-0.9.0.1 --group some_group --zookeeper zookeeper-host.com:2181
```

### Usage with telegraf

This script works with telegraf exec plugin. Just download this script and set the following configuration in your telegraf.conf

```
[[inputs.exec]]
    commands = ["python /path/to/kafka_consumer_lag_reporter.py --kafka-dir=/path/to/kafka_2.11-0.9.0.1 --group some_group --bootstrap-server kafka-broker-host.com:9092"]
    data_format = "influx"
```
