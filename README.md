This a simple count application in kafka streams.
It can connect to a Kafka cluster via SASL_SSL. 

It will read the records on one topic (configured via `input.topic`).
It will emit a count every 5 on the output topic (configured via `output.topic`).
The count represents the amount of records received in each 5 second interval. 


The configuration parameters can be provided via the regular mechanisms for Spring Boot applications:

The following configuration parameters are possible:

* `kafka.bootstrap_servers`: like `yourcluster.aivencloud.com:22638`
* `kafka.authentication`:  either `""` or `sasl_ssl`
* `kafka.username`
* `kafka.password`
* `ssl.truststore.location`: absolute path to a JKS file with the trusted CAs. For example, if you are using Aiven you
can download the aiven CA from their site and add it to a trustore so that you will 
accept any server certificate from aiven.
* `ssl.truststore.password`: password to access the truststore
* `input_topic`: name of the topic to read from
* `output_topic`: name of the topic to write to




# Create a truststore JKS for Aiven 

First download the CA cert  (`admin-cert-pem`)
```
keytool -importcert -v -trustcacerts -alias AivenCARoot -file admin-cert.pem -keystore truststore.jks  -storepass YOURPASSWORD -noprompt 
```