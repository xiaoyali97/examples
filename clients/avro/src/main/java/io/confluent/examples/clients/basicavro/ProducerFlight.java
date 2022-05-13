package io.confluent.examples.clients.basicavro;

import com.mycorp.mynamespace.sampleRecord;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Properties;
import java.io.IOException;

public class ProducerFlight {

    private static final String TOPIC = "schema-context-test";
    private static final Properties props = new Properties();
    private static String configFile;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-0wg55.us-central1.gcp.devel.cpdev.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='5IHMXBHB27ZUASHF' password='secret';");
        props.put("sasl.mechanism", "PLAIN");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "https://psrc-w6y8w.us-west-2.aws.devel.cpdev.cloud");

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", "H3XGVBJRA4O7UDPN:<secret>");

        try (KafkaProducer<String, sampleRecord> producer = new KafkaProducer<>(props)) {

            for (int i = 0; i < 10; i++) {
                final sampleRecord flight = new sampleRecord(i, "sfo", "pvd");
                final ProducerRecord<String, sampleRecord> record = new ProducerRecord<>(TOPIC, String.valueOf(i), flight);
                producer.send(record);
                Thread.sleep(1000L);
            }

            producer.flush();
            System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);

        } catch (final SerializationException e) {
            e.printStackTrace();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

    }

}
