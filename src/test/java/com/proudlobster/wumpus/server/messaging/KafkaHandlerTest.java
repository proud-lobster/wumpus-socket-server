package com.proudlobster.wumpus.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;

public class KafkaHandlerTest {

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    private final Properties BASE_PROPS = new Properties();

    @BeforeEach
    public void setProps() {
        BASE_PROPS.put("bootstrap.servers", sharedKafkaTestResource.getKafkaConnectString());
        BASE_PROPS.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        BASE_PROPS.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @AfterEach
    public void clearProps() {
        BASE_PROPS.clear();
    }

    @Test
    public void testKafkaHandler() {    // TODO cleanup!
        final KafkaHandler handler = new KafkaHandler("test-topic", BASE_PROPS);
        final ClientMessage message = ClientMessage.Directive.LOGIN.create("sessionId", "payload");

        KafkaTestUtils utils = sharedKafkaTestResource.getKafkaTestUtils();

        // 1. Ensure topic exists with 1 partition
        utils.createTopic("test-topic", 1, (short) 1);

        try (KafkaConsumer<String, String> consumer = utils.getKafkaConsumer(StringDeserializer.class,
                StringDeserializer.class)) {

            // 2. Directly assign partition 0 (bypasses consumer group join delay)
            org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition("test-topic", 0);
            consumer.assign(java.util.Collections.singletonList(tp));
            consumer.seekToBeginning(java.util.Collections.singletonList(tp));

            // 3. Produce the message via your handler
            handler.accept(message);

            // 4. Poll for records (short timeout is now safe because assignment is instant)
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Expected message to be present in Kafka topic");
            ConsumerRecord<String, String> record = records.iterator().next();
            assertEquals(message.whole(), record.value());
        }
    }
}
