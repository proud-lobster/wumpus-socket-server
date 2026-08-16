package com.proudlobster.wumpus.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;

public class KafkaHandlerTest {

    @RegisterExtension
    public static final SharedKafkaTestResource KAFKA_TEST = new SharedKafkaTestResource();

    private final KafkaTestUtils KAFKA_UTILS = KAFKA_TEST.getKafkaTestUtils();
    private final String TOPIC = "test-topic";
    private final int PARTITIONS = 1;
    private final short REPLICATION_FACTOR = 1;
    private final int SEL_PARTITION = PARTITIONS - 1;
    private final Properties BASE_PROPS = new Properties();
    private final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    @BeforeEach
    public void setProps() {
        BASE_PROPS.put("bootstrap.servers", KAFKA_TEST.getKafkaConnectString());
        BASE_PROPS.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        BASE_PROPS.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @BeforeEach
    public void createTopic() {
        KAFKA_UTILS.createTopic(TOPIC, PARTITIONS, REPLICATION_FACTOR);
        // TODO how to ensure existing topic is cleared at the start of each test?
    }

    @AfterEach
    public void clearProps() {
        BASE_PROPS.clear();
    }

    private KafkaConsumer<String, String> consumer() {
        return KAFKA_UTILS.getKafkaConsumer(StringDeserializer.class, StringDeserializer.class);
    }

    private CompletableFuture<ConsumerRecords<String, String>> records() {
        return CompletableFuture.supplyAsync(() -> {
            try (KafkaConsumer<String, String> consumer = consumer()) {
                final TopicPartition tp = new TopicPartition(TOPIC, SEL_PARTITION);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));
                return consumer.poll(POLL_TIMEOUT);
            }
        });
    }

    @Test
    public void testKafkaHandler() throws IOException {
        final KafkaHandler handler = new KafkaHandler(TOPIC, BASE_PROPS);
        final ClientMessage message = ClientMessage.Directive.LOGIN.create("sessionId", "payload");
        final CompletableFuture<ConsumerRecords<String, String>> futureRecords = records();

        handler.accept(message);

        final ConsumerRecords<String, String> records = futureRecords.join();
        assertFalse(records.isEmpty());
        final ConsumerRecord<String, String> record = records.iterator().next();
        assertEquals(message.whole(), record.value());
        handler.close();
    }
}
