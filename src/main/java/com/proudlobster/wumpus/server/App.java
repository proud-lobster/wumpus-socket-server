package com.proudlobster.wumpus.server;

import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.proudlobster.wumpus.Main;
import com.proudlobster.wumpus.server.messaging.ClientMessage;
import com.proudlobster.wumpus.server.messaging.KafkaHandler;
import com.proudlobster.wumpus.server.messaging.KafkaListenerWorker;
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
     * topics.account: account topic
     * topics.command: command topic
     * topics.outbound: outbound topic
     */
    private final Properties APP_PROPS;

    private KafkaHandler accountHandler;
    private KafkaHandler commandHandler;
    private WumpusServer server;
    private KafkaListenerWorker worker;
    private SessionHandler sessions;

    public App(Properties props) {
        this.APP_PROPS = new Properties();
        this.APP_PROPS.putAll(props);
    }

    public void start() throws Exception {
        final String serverId = APP_PROPS.getProperty("server.id", DEFAULT_SERVER_ID);
        LOG.info("Starting server (ID: {0})...", serverId);
        this.sessions = new SessionHandler(serverId);

        LOG.info("Registering handlers...");
        final Properties kafkaProps = new Properties();
        APP_PROPS.stringPropertyNames().stream()
                .filter(k -> k.startsWith(KAFKA_PROP_PREFIX))
                .map(k -> k.substring(KAFKA_PROP_PREFIX.length()))
                .forEach(k -> kafkaProps.setProperty(k, APP_PROPS.getProperty(k)));
        ClientMessage.registerHandler(ClientMessage.Directive.PING, PingHandler.create(sessions));
        this.accountHandler = new KafkaHandler(APP_PROPS.getProperty("topics.account"), kafkaProps);
        ClientMessage.registerHandler(ClientMessage.Directive.LOGIN, accountHandler);
        ClientMessage.registerHandler(ClientMessage.Directive.LOGOUT, accountHandler);
        ClientMessage.registerHandler(ClientMessage.Directive.TOKEN, accountHandler);
        this.commandHandler = new KafkaHandler(APP_PROPS.getProperty("topics.command"), kafkaProps);
        ClientMessage.registerHandler(ClientMessage.Directive.EXECUTE, commandHandler);
        ClientMessage.registerHandler(ClientMessage.Directive.DATA, commandHandler);

        LOG.info("Starting server...");
        final int port = Integer.parseInt(APP_PROPS.getProperty("server.port", DEFAULT_SERVER_PORT));
        final String path = APP_PROPS.getProperty("server.path", DEFAULT_SERVER_PATH);
        this.server = new WumpusServer(port, path, sessions);
        this.server.start();

        final String outboundTopicName = APP_PROPS.getProperty("topics.outbound") + "." + serverId;
        LOG.info("Starting outbound thread (Topic: {0})...", outboundTopicName);
        this.worker = new KafkaListenerWorker(outboundTopicName, kafkaProps, sessions);
        final Thread outbound = new Thread(worker);
        outbound.run();
    }

    public void stop() {
        LOG.info("Stopping the server...");
        LOG.info("Closing all open sessions...");
        sessions.closeAll();

        LOG.info("Closing Kafka outbound listener...");
        try {
            worker.close();
        } catch (IOException e) {
            LOG.error("Error closing Kafka listener.", e);
        }

        LOG.info("Stopping server...");
        try {
            server.stop();
        } catch (Exception e) {
            LOG.error("Error stopping server.", e);
        }

        LOG.info("Closing account message handler...");
        try {
            accountHandler.close();
        } catch (IOException e) {
            LOG.error("Error closing account message handler.", e);
        }

        LOG.info("Closing command message handler.");
        try {
            commandHandler.close();
        } catch (IOException e) {
            LOG.error("Error closing command message handler.", e);
        }
    }
}
