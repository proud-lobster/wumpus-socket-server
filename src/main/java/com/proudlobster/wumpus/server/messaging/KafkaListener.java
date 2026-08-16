package com.proudlobster.wumpus.server.messaging;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaListener implements Supplier<List<ClientMessage>>, AutoCloseable {

    private final Properties props;
    private final String topic;
    private final AtomicReference<KafkaConsumer<String, String>> consumerRef;
    private final AtomicBoolean open;

    public KafkaListener(final String topic, final Properties props) {
        this.props = new Properties();
        this.props.putAll(props);
        this.topic = topic;
        this.open = new AtomicBoolean(true);
        this.consumerRef = new AtomicReference<>();
    }

    private KafkaConsumer<String, String> getConsumer() {
        if (consumerRef.get() == null) {
            consumerRef.set(new KafkaConsumer<>(props));
        }
        return consumerRef.get();
    }

    @Override
    public List<ClientMessage> get() {
        getConsumer().subscribe(Collections.singletonList(topic));
        while (open.get()) {
            final ConsumerRecords<String, String> records = getConsumer().poll(Duration.ofSeconds(1));
            return StreamSupport.stream(records.spliterator(), false)
                    .map(ConsumerRecord::value)
                    .map(ClientMessage::create)
                    .toList();
        }
        return List.of();
    }

    @Override
    public void close() throws IOException {
        open.set(false);
        getConsumer().close();
    }

}
