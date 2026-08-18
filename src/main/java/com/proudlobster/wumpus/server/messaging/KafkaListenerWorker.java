package com.proudlobster.wumpus.server.messaging;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.proudlobster.wumpus.server.SessionHandler;

public class KafkaListenerWorker implements Runnable, AutoCloseable {

    private final KafkaListener listener;
    private final AtomicBoolean open;
    private final SessionHandler sessions;

    public KafkaListenerWorker(final String topic, final Properties props, final SessionHandler sessions) {
        this.listener = new KafkaListener(topic, props);
        this.open = new AtomicBoolean(true);
        this.sessions = sessions;
    }

    @Override
    public void run() {
        listener.subscribe();
        while (open.get()) {
            listener.get()
                    .stream()
                    .forEach(sessions::send);
        }
    }

    @Override
    public void close() throws IOException {
        open.set(false);
        listener.close();
    }

}
