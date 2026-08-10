package com.proudlobster.wumpus.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.proudlobster.wumpus.server.messaging.ClientMessage;

public class WebSocketConnectionTest {

    public static final String PROTO = "ws";
    public static final String HOST = "localhost";
    public static final int PORT = 8080;
    public static final String PATH = "/socket";
    public static final String URL = PROTO + "://" + HOST + ":" + PORT + PATH;

    private WumpusServer server;

    @BeforeEach
    public void startServer() throws Exception {
        server = new WumpusServer(8080, "/socket", new SessionHandler("test"));
        server.start();
    }

    @AfterEach
    public void stopServer() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testConnection() throws InterruptedException, ExecutionException, TimeoutException {

        final CompletableFuture<String> messageFuture = new CompletableFuture<>();
        final WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                messageFuture.complete(data.toString());
                return WebSocket.Listener.super.onText(webSocket, data, last);
            }
        };

        final WebSocket ws = HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(URI.create(URL), listener)
                .join();

        final String response = messageFuture.get(3, TimeUnit.SECONDS);
        assertTrue(response.contains(ClientMessage.Directive.SUCCESS.name()));

        ws.sendClose(WebSocket.NORMAL_CLOSURE, "Test complete").join();
    }
}
