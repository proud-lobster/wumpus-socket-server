package com.proudlobster.wumpus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.proudlobster.wumpus.server.SessionHandler;
import com.proudlobster.wumpus.server.WumpusServer;
import com.proudlobster.wumpus.server.messaging.ClientMessage;
import com.proudlobster.wumpus.server.messaging.PingHandler;

public interface Main {

    Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String... args) throws Exception {
        LOG.info("Wumpus Socket Server");
        LOG.info("by Proud Lobster Games");

        final String serverId = args.length > 0 ? args[0] : "default";
        LOG.info("Server ID: " + serverId);
        final SessionHandler sessions = new SessionHandler(serverId);

        LOG.info("Connecting to streams...");
        // TODO

        LOG.info("Registering handlers...");
        // TODO
        ClientMessage.registerHandler(ClientMessage.Directive.PING, PingHandler.create(sessions));

        LOG.info("Starting server...");
        final WumpusServer server = new WumpusServer(8080, "/socket", sessions);
        server.start();

        LOG.info("Starting outbound thread...");
        // TODO stream consumer that does the send to sessions

        LOG.info("Server is running.");
    }
}