package com.rubenlaguna.springkafkastreams;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.PrimitiveIterator;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
public class SpringkafkastreamsApplication implements CommandLineRunner {
    public static final Logger LOG = LoggerFactory.getLogger(SpringkafkastreamsApplication.class);

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private Environment env;

    public static void main(String[] args) {
        SpringApplication.run(SpringkafkastreamsApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Starting");
        Instant end = Instant.now().plus(Duration.ofMinutes(5));
        final PrimitiveIterator.OfInt rand = new Random().ints(100, 200).iterator();
        final Counter nMessagesProcessedCounter = meterRegistry.counter("n_messages_processed");


        Topology topology = new Topology();

        topology = topology.addSource(
                Topology.AutoOffsetReset.LATEST,
                "input-source",
                new UsePreviousTimeOnInvalidTimestamp(),
                Serdes.String().deserializer(),
                Serdes.String().deserializer(),
                env.getProperty("input_topic", "input-topic")
                );


        topology = topology.addProcessor(
                "count-processor",
                () -> new AbstractProcessor<String, String>() {

                    private AtomicLong counter = new AtomicLong();

                    @Override
                    public void init(final ProcessorContext context) {
                        super.init(context);
                        context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                            @Override
                            public void punctuate(long timestamp) {
                                final long counterValue = counter.getAndSet(0);
                                context.forward("", Long.toString(counterValue));
                                LOG.info("send count {}", counterValue);
                            }
                        });
                    }

                    @Override
                    public void process(String key, String value) {
                        counter.incrementAndGet();
                        nMessagesProcessedCounter.increment(); // spring actuator metrics
                    }
                },
                "input-source"

        );

        topology = topology.addSink(
                "count-sink",
                env.getProperty("output_topic", "output-topic") ,
                Serdes.String().serializer(),
                Serdes.String().serializer(),
                "count-processor"
        );


        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("kafka.bootstrap_servers", "localhost:9092"));
        if (env.getProperty("kafka.authentication", "").equalsIgnoreCase("sasl_ssl")) {
            props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            final String jaasConfig = MessageFormat.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" password=\"{1}\";",
                    env.getProperty("kafka.username"),
                    env.getProperty("kafka.password")
            );
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
            if (env.getProperty("ssl.truststore.location") != null) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, env.getProperty("ssl.truststore.location"));
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, env.getProperty("ssl.truststore.password"));
            }
            if (env.getProperty("ssl.keystore.location") != null) {
                // not really necessary (I guess it's good to have if we add the SSL authentication (via client certificate)
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, env.getProperty("ssl.keystore.location"));
                props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, env.getProperty("ssl.keystore.password"));
            }
        }

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("addShutDownHook executed!");
            kafkaStreams.close();
        }));


        LOG.info("Stopping");
    }
}
