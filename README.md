# Kafka Connect Connector for S3

This project is mirror of a [Confluent's open source project](https://github.com/confluentinc/kafka-connect-storage-cloud).

*kafka-connect-storage-cloud* is the repository for Confluent's [Kafka Connectors](http://kafka.apache.org/documentation.html#connect)
designed to be used to copy data from Kafka into Amazon S3. 

## Kafka Connect Sink Connector for Amazon Simple Storage Service (S3)

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-storage-cloud/kafka-connect-s3/docs/index.html).

Blogpost for this connector can be found [here](https://www.confluent.io/blog/apache-kafka-to-amazon-s3-exactly-once).

### Additional features

* A new Field Partitioner which can pick value if the incoming record is of the form of map.

* A toggle to keep s3 path in lowercase. AWS Athena only supports partition fieldnames in lowercase.

* As we have a kafka cluster with hundreds of topics, and each topics have ten partitions. Throughput varies widely from topic to topic. It's really a huge memory footprint when connect to s3, since thousands of S3OutputStream created. I known my kafka cluster maybe not follow the best pactice. The fix buffer size(default: 25MB) each S3OutputStream hold is not good for me.

I customize a elastic buffer for S3OutputStream. The elastic buffer have a logic capacity. However physical allocation is elastic with logic capacity as max limit. Physical size expend by initCapacity * (2^N) and shrink to initial size when s3-part is uploaded.

By my test, it can save 60% memory for me.

* While implementing Kafka Connect pipeline that writes to S3, we found the servers were not completely utilized, having idle CPU and more network bandwidth available. Profiling the process revealed ~90% of time is spent in upload to S3. It also looked that each chunk was uploaded serially - blocking reading while its being uploaded. We implemented this approach of a pool of workers utilizing S3 multipart uploads in parallel, and found the utilization of machines could be improved (and as a result were able to scale down the cluster size required to keep up with the Kafka topic).
