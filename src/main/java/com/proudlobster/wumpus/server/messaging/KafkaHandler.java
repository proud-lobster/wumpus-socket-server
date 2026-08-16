package com.proudlobster.wumpus.server.messaging;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaHandler implements ClientMessage.Handler, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaHandler.class);
    private static final Callback CALLBACK = (metadata, exception) -> {
        if (exception != null) {
            LOG.error("Error sending message", exception);
        }
    };

    private final Properties props;
    private final String topic;
    private final AtomicReference<KafkaProducer<String, String>> producerRef;

    public KafkaHandler(final String topic, final Properties props) {
        this.topic = topic;
        this.props = new Properties();
        this.props.putAll(props);
        this.producerRef = new AtomicReference<>();
    }

    private KafkaProducer<String, String> getProducer() {
        if (producerRef.get() == null) {
            producerRef.set(new KafkaProducer<>(props));
        }
        return producerRef.get();
    }

    @Override
    public void accept(ClientMessage t) {
        getProducer().send(new ProducerRecord<>(topic, "0", t.whole()), CALLBACK);
    }

    @Override
    public void close() throws IOException {
        getProducer().close();
    }

}
