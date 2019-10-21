package com.rubenlaguna.springkafkastreams;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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

import java.time.Duration;
import java.time.Instant;
import java.util.PrimitiveIterator;
import java.util.Properties;
import java.util.Random;

@SpringBootApplication
public class SpringkafkastreamsApplication implements CommandLineRunner {
    public static final Logger LOG = LoggerFactory.getLogger(SpringkafkastreamsApplication.class);

    @Autowired
    private MeterRegistry meterRegistry;

    public static void main(String[] args) {
        SpringApplication.run(SpringkafkastreamsApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Starting");
        Instant end = Instant.now().plus(Duration.ofMinutes(5));
        final PrimitiveIterator.OfInt rand = new Random().ints(100, 200).iterator();
        final Counter counter = meterRegistry.counter("n_requests");
//        while(Instant.now().isBefore(end)) {
////            Thread.sleep(rand.next());
////            counter.increment();
////        }


         Topology topology = new Topology();

        topology = topology.addSource(
                Topology.AutoOffsetReset.LATEST,
                "firehose-source",
                new UsePreviousTimeOnInvalidTimestamp(),
                Serdes.String().deserializer(),
                Serdes.String().deserializer(),
                "firehose");


        topology = topology.addProcessor(
                "count-processor",
                () -> new AbstractProcessor<String, String>() {

                    private int counter = 0;

                    @Override
                    public void init(final ProcessorContext context) {
                        super.init(context);
                        context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                            @Override
                            public void punctuate(long timestamp) {
                                context.forward("", Integer.toString(counter));
                            }
                        });
                    }

                    @Override
                    public void process(String key, String value) {
                        counter++;
                    }
                },
                "firehose-source"

        );

        topology = topology.addSink(
                "count-sink",
                "output-topic",
                Serdes.String().serializer(),
                Serdes.String().serializer(),
                "count-processor"
        );


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("addShutDownHook executed!");
            kafkaStreams.close();
        }));


        LOG.info("Stopping");
    }
}
