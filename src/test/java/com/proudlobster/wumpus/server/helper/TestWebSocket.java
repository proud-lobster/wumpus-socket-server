package com.proudlobster.wumpus.server.helper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;

public class TestWebSocket implements AutoCloseable {

    private WebSocket ws;
    private final TestWebSocketListener wsl;

    public TestWebSocket() {
        wsl = new TestWebSocketListener();
    }

    public void connect(final URI uri) {
        ws = HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(uri, wsl).join();
    }

    public String receive() throws InterruptedException {
        return wsl.awaitNextMessage();
    }

    public void send(final String text) {
        ws.sendText(text, true).join();
    }

    @Override
    public void close() {
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "Complete").join();
    }
}
