package com.proudlobster.wumpus.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;

public class KafkaListenerTest {

    @RegisterExtension
    public static final SharedKafkaTestResource KAFKA_TEST = new SharedKafkaTestResource();

    private static final Logger LOG = LoggerFactory.getLogger(KafkaHandler.class);
    private static final Callback CALLBACK = (metadata, exception) -> {
        LOG.info("CALLBACK: " + metadata);
        if (exception != null) {
            LOG.error("Error sending message", exception);
        }
    };
    private final KafkaTestUtils KAFKA_UTILS = KAFKA_TEST.getKafkaTestUtils();
    private final String TOPIC = "test-topic";
    private final int PARTITIONS = 1;
    private final short REPLICATION_FACTOR = 1;
    private final String GROUP_ID = "test-group";
    private final Properties BASE_PROPS = new Properties();

    @BeforeEach
    public void setProps() {
        BASE_PROPS.put("bootstrap.servers", KAFKA_TEST.getKafkaConnectString());
        BASE_PROPS.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        BASE_PROPS.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        BASE_PROPS.put("group.id", GROUP_ID);
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

    @Test
    public void testKafkaListener() throws IOException {
        final KafkaProducer<String, String> producer = KAFKA_UTILS.getKafkaProducer(StringSerializer.class,
                StringSerializer.class, BASE_PROPS);
        final KafkaListener listener = new KafkaListener(TOPIC, BASE_PROPS);
        final ClientMessage msg = ClientMessage.Directive.EXECUTE.create("sessionId", "payload");
        listener.subscribe();
        producer.send(new ProducerRecord<>(TOPIC, "0", msg.whole()), CALLBACK);
        final List<ClientMessage> msgs = listener.get();
        assertEquals(1, msgs.size());
        assertEquals(msg.whole(), msgs.get(0).whole());
        listener.close();
    }
}
