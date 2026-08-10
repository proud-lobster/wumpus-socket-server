package com.proudlobster.wumpus;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.proudlobster.wumpus.server.SessionHandler;
import com.proudlobster.wumpus.server.WumpusConfigurator;
import com.proudlobster.wumpus.server.messaging.ClientMessage;
import com.proudlobster.wumpus.server.messaging.PingHandler;

public interface Main {

    Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String... args) {
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

        LOG.info("Configuring server...");
        final Server server = new Server(8080);
        final ServletContextHandler handler = new ServletContextHandler("/");
        server.setHandler(handler);
        WumpusConfigurator.configure(handler, sessions);

        LOG.info("Starting server...");
        try {
            server.start();
        } catch (Exception e) {
            LOG.error("Could not start server.", e);
            System.exit(1);
        }

        LOG.info("Starting outbound thread...");
        // TODO stream consumer that does the send to sessions

        LOG.info("Server is running.");
    }
}