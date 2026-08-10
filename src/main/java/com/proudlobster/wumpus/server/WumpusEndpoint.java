package com.proudlobster.wumpus.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;

public class WumpusEndpoint extends Endpoint {

    private static final Logger LOG = LoggerFactory.getLogger(WumpusEndpoint.class);
    
    private final SessionHandler sessions;

    public WumpusEndpoint(final SessionHandler sessions) {
        this.sessions = sessions;
    }

    @Override
    public void onOpen(final Session session, final EndpointConfig config) {
        sessions.register(session);
    }

    @Override
    public void onError(final Session session, final Throwable thr) {
        LOG.error("Error occurred in session {}.", session.getId(), thr);
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
        sessions.close(session);
    }

}
