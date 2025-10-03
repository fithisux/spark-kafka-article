# Code Spark Structured Streaming Article

The code is adapted from the wonderful repository of [Gary Stafford](https://github.com/garystafford/streaming-sales-generator)

Before launching the cluster you need to create a bunch of folders

* Spark configuration folder (to control the configuration) `/localvolumes/all_conf`
* Spark logging folder (to view logs) `/localvolumes/all_logs`
* The ivycache (we will see later) `/localvolumes/ivycache`
* The jars folder (for spark connect) `/localvolumes/jars`
* Out working directory (Spark Connect puts there the output) `/localvolumes/sc-work-dir`

You also need to copy the [spark-defaults.conf](spark-defaults.conf) into `/localvolumes/all_conf`

and also download for spark connect the jar spark-connect_2.13-4.0.1.jar from [here](https://mvnrepository.com/artifact/org.apache.spark/spark-connect) and place it in `localvolumes/jars`.

Start your docker deployment 

```
docker compose up
```

and for [test_stream_kafka_nb.ipynb](test_stream_kafka_nb.ipynb) you need to go to [http://localhost:9080](http://localhost:9080)
and create a suitable topic.

You can now run the test notebooks or the [test_stream_kafka.py](test_stream_kafka.py) through the command (on Windows)

```
docker run -it --network=spark-kafka-article_streaming-stack -v %cd%\localvolumes\all_conf\spark-defaults.conf:/opt/spark/conf/spark-defaults.conf -v  %cd%\localvolumes\ivycache:/opt/spark/ivycache:rw -v %cd%\test_stream_kafka.py:/opt/spark/test_stream_kafka.py apache/spark:4.0.1-scala2.13-java17-python3-r-ubuntu /opt/spark/bin/spark-submit /opt/spark/test_stream_kafka.py
```

For running the kafka examples, clone the repo in [bytewax article](https://github.com/fithisux/bytewax-kafka-article),

create a virtual environment with the included `requirmeents.txt` and run the `producer.py`.

Shot down your deployment with

```
docker compose down -v
```

and cleanup logs in `/localvolumes/all_conf`