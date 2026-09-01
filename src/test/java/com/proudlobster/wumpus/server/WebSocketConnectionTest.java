package com.proudlobster.wumpus.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;

import com.proudlobster.wumpus.server.helper.TestWebSocket;
import com.proudlobster.wumpus.server.messaging.ClientMessage;

public class WebSocketConnectionTest {

    @RegisterExtension
    public static final SharedKafkaTestResource KAFKA_TEST = new SharedKafkaTestResource();

    public static final String PROTO = "ws";
    public static final String HOST = "localhost";
    public static final int PORT = 8080;
    public static final String PATH = "/socket";
    public static final String URL = PROTO + "://" + HOST + ":" + PORT + PATH;

    public static final String SERVER_ID = "wumpus-server";
    public static final String ECHO_TOPIC_NAME = "foo";

    @Test
    public void testConnection() throws Exception {
        final WumpusServer server = new WumpusServer(PORT, PATH, new SessionHandler(SERVER_ID));
        server.start();
        try (final TestWebSocket ws = new TestWebSocket()) {
            ws.connect(URI.create(URL));
            final String response = ws.receive();
            assertTrue(response.contains(ClientMessage.Directive.SUCCESS.name()));
        }
        server.stop();
    }

    @Test
    public void echo() throws Exception {
        final Properties p = new Properties();
        p.setProperty("server.id", SERVER_ID);
        p.setProperty("server.port", PORT + "");
        p.setProperty("server.path", PATH);
        p.setProperty("kafka.bootstrap.servers", KAFKA_TEST.getKafkaConnectString());
        p.setProperty("kafka.key.serializer", StringSerializer.class.getName());
        p.setProperty("kafka.value.serializer", StringSerializer.class.getName());
        p.setProperty("kafka.key.deserializer", StringDeserializer.class.getName());
        p.setProperty("kafka.value.deserializer", StringDeserializer.class.getName());
        p.setProperty("kafka.group.id", "wumpus-group");
        p.setProperty("topics.account", ECHO_TOPIC_NAME + "." + SERVER_ID);
        p.setProperty("topics.command", ECHO_TOPIC_NAME + "." + SERVER_ID);
        p.setProperty("topics.outbound", ECHO_TOPIC_NAME);
        final App app = new App(p);
        app.start();

        System.out.println("Commencing test...");
        try (final TestWebSocket ws = new TestWebSocket()) {
            System.out.println("Connecting...");
            ws.connect(URI.create(URL));
            final String response = ws.receive();
            final ClientMessage success = ClientMessage.create(response);
            assertEquals(ClientMessage.Directive.SUCCESS, success.directive());
            System.out.println("Connection success!");

            System.out.println("Sending test payload...");
            final String sessionId = success.sessionId();
            final ClientMessage foo = ClientMessage.Directive.EXECUTE.create(sessionId, "foo");
            ws.send(foo.whole());
            System.out.println("Test payload sent.");
            final String response2 = ws.receive();
            final ClientMessage echo = ClientMessage.create(response2);
            System.out.println("Test response received.");
            assertEquals(foo.sessionId(), echo.sessionId());
            assertEquals(foo.directive(), echo.directive());
            assertEquals(foo.payload(), echo.payload());
        }

        app.stop();
    }
}
