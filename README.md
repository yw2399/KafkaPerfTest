# KafkaPerfTest
## Architecture
The test environment is made up of 2 components an echo server (EchoConsumer.java) and a traffic generator (SimpleProducer.java).

The echo server continuously polls a pre-designated topic. Once it finds a new record (key:value), it writes (key+10000:value) back to the same topic.

The traffic generator contains one Kafka producer and one Kafka Consumer. For each message size, the producer writes a fixed amount of messages  of the size to the pre-designated topic. Then it waits until the consumer reads a 'confirmation' echoed back from the echo server. For instance, when testing the size of 1MB, the producer will continuously write 100 records of the same size then it waits until the consumer reads a record having the key 10099=10000+99, and finally it calculates the interval involved with the 100 records.

## Configuration Details
Batch size is 10, but I flush at the end of the test for each size, so this should not make the interval longer.
