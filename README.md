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

* Memory footprint is huge when it connects to s3, since thousands of S3OutputStream created. The fix buffer size(default: 25MB) for each S3OutputStream is not       good. I customized an elastic buffer for S3OutputStream. Physical size expended by initCapacity * (2^N) and shrink to initial size when s3-part is uploaded.

* Implemented an approach of a pool of workers utilizing S3 multipart uploads in parallel, and found the utilization of machines could be improved.
