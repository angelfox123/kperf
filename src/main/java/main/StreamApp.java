package main;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class StreamApp {
    public static void main(String[] args) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-stream");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "simple_stream_1");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()
            .getClass().getName());
        streamsConfiguration.put(StreamsConfig.PRODUCER_PREFIX
            + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 30);
        streamsConfiguration.put(StreamsConfig.PRODUCER_PREFIX
            + ProducerConfig.LINGER_MS_CONFIG,"5");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,2);


        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream(
            "input",
            Consumed.with(
                new Serdes.StringSerde(),
                new Serdes.StringSerde()
            )
        );

        inputStream.to(
            "output",
            Produced.with(new Serdes.StringSerde(), new Serdes.StringSerde())
        );

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
    }
}
