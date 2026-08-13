package com.proudlobster.wumpus.server.messaging;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaHandler implements ClientMessage.Handler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaHandler.class);
    private static final Callback CALLBACK = (metadata, exception) -> {
        if (exception != null) {
            LOG.error("Error sending message", exception);
        }
    };

    private final Properties props;
    private final String topic;

    public KafkaHandler(final String topic, final Properties props) {
        this.topic = topic;
        this.props = new Properties();
        this.props.putAll(props);
    }

    private KafkaProducer<String, String> getProducer() {
        if (props.isEmpty()) {
            try (InputStream input = new FileInputStream("kafka.properties")) {
                props.load(input);
            } catch (Exception e) {
                LOG.error("Error loading kafka.properties", e);
            }
        }
        return new KafkaProducer<>(props);
    }

    @Override
    public void accept(ClientMessage t) {
        try (KafkaProducer<String, String> producer = getProducer()) {
            producer.send(new ProducerRecord<>(topic, "0", t.whole()), CALLBACK);
        }
    }

}
