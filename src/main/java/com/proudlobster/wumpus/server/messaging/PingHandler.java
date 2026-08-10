package com.proudlobster.wumpus.server.messaging;

import com.proudlobster.wumpus.server.SessionHandler;

@FunctionalInterface
public interface PingHandler extends ClientMessage.Handler {

    public static PingHandler create(final SessionHandler sessions) {
        return m -> sessions.send(ClientMessage.Directive.PING.create(m.sessionId(), System.currentTimeMillis() + ""));
    }
    
}
