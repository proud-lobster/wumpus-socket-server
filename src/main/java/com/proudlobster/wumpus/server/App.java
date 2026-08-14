package com.proudlobster.wumpus.server;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.proudlobster.wumpus.Main;
import com.proudlobster.wumpus.server.messaging.ClientMessage;
import com.proudlobster.wumpus.server.messaging.KafkaHandler;
import com.proudlobster.wumpus.server.messaging.PingHandler;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_SERVER_ID = "default";
    private static final String DEFAULT_SERVER_PORT = "8080";
    private static final String DEFAULT_SERVER_PATH = "/socket";
    private static final String KAFKA_PROP_PREFIX = "kafka.";

    /**
     * server.id: unique identifier for this server instance, used to identify
     * sessions that belong to this server
     * server.port: port to listen on for websocket connections
     * server.path: path to listen on for websocket connections
     * kafka.*: properties for connecting to Kafka
     */
    private final Properties APP_PROPS;

    public App(Properties props) {
        this.APP_PROPS = new Properties();
        this.APP_PROPS.putAll(props);
    }

    public void start() throws Exception {
        final String serverId = APP_PROPS.getProperty("server.id", DEFAULT_SERVER_ID);
        LOG.info("Server ID: " + serverId);
        final SessionHandler sessions = new SessionHandler(serverId);

        LOG.info("Connecting to streams...");
        // TODO

        LOG.info("Registering handlers...");
        // TODO
        // TODO build out handlers dynamically
        final Properties kafkaProps = new Properties();
        for (String key : APP_PROPS.stringPropertyNames()) {
            if (key.startsWith(KAFKA_PROP_PREFIX)) {
                kafkaProps.setProperty(key.substring(KAFKA_PROP_PREFIX.length()), APP_PROPS.getProperty(key));
            }
        }
        ClientMessage.registerHandler(ClientMessage.Directive.PING, PingHandler.create(sessions));
        ClientMessage.registerHandler(ClientMessage.Directive.LOGIN, new KafkaHandler("wumpus-send", kafkaProps));

        LOG.info("Starting server...");
        final int port = Integer.parseInt(APP_PROPS.getProperty("server.port", DEFAULT_SERVER_PORT));
        final String path = APP_PROPS.getProperty("server.path", DEFAULT_SERVER_PATH);
        final WumpusServer server = new WumpusServer(port, path, sessions);
        server.start();

        ClientMessage.Directive.LOGIN.create("fooid", "test").handle();

        LOG.info("Starting outbound thread...");
        // TODO stream consumer that does the send to sessions
    }
}
