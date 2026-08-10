package com.proudlobster.wumpus.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.websocket.Session;

public class SessionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SessionHandler.class);

    private final String serverId;
    private final Map<String, Session> sessions;

    public SessionHandler(final String serverId) {
        this.serverId = serverId;
        this.sessions = new ConcurrentHashMap<>();
    }

    public void register(final Session session) {
        final String sessionId = serverId + "-" + session.getId();
        sessions.put(sessionId, session);
        session.addMessageHandler(WumpusMessageHandler.create(sessionId));
        send(ClientMessage.Directive.SUCCESS.create(sessionId, "You are now connected."));
    }

    public void send(final ClientMessage msg) {
        send(msg.sessionId(), msg.whole());
    }

    public void send(final String sessionId, final String msg) {
        final Session s = sessions.get(sessionId);
        if (!isOpen(sessionId)) {
            LOG.warn("Session {} is not open, cannot send message.", sessionId);
            return;
        }
        try {
            s.getBasicRemote().sendText(msg);
        } catch (Exception e) {
            LOG.error("Could not send message to session {}.", sessionId, e);
            close(s);
        }
    }

    public void close(final Session session) {
        try {
            session.close();
        } catch (Exception e) {
            LOG.error("Could not close session {}.", session.getId(), e);
        }
    }

    public boolean isOpen(final String sessionId) {
        return sessions.get(sessionId).isOpen();
    }
}
