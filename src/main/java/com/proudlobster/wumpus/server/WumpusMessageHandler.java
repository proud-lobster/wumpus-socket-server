package com.proudlobster.wumpus.server;

import java.util.Optional;

import com.proudlobster.wumpus.server.messaging.ClientMessage;

import jakarta.websocket.MessageHandler;

@FunctionalInterface
public interface WumpusMessageHandler extends MessageHandler.Whole<String> {

    public static WumpusMessageHandler create(final String sessionId) {
        return msg -> Optional.of(msg)
                .map(ClientMessage::create)
                .filter(m -> m.sessionId().equals(sessionId))
                .ifPresentOrElse(ClientMessage::handle, () -> invalidMessage(msg));
    }

    public static void invalidMessage(final String msg) {
        throw new IllegalArgumentException("Invalid message: " + msg);
    }
}
