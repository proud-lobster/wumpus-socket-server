package com.proudlobster.wumpus.server.helper;

import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

public class TestWebSocketListener implements WebSocket.Listener {

    private final LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
    private final StringBuilder textBuffer = new StringBuilder();

    @Override
    public void onOpen(WebSocket webSocket) {
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        textBuffer.append(data);

        if (last) {
            messageQueue.offer(textBuffer.toString());
            textBuffer.setLength(0);
        }

        webSocket.request(1);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        error.printStackTrace();
    }

    public String awaitNextMessage() throws InterruptedException {
        return messageQueue.take();
    }
}
