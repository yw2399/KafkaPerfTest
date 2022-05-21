#!/usr/bin/bash
rm *.class

javac -cp "./kafka/libs/*"  SimpleProducer.java
javac -cp "./kafka/libs/*"  EchoConsumer.java
