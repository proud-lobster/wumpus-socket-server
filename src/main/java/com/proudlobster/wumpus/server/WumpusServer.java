package com.proudlobster.wumpus.server;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;

public class WumpusServer {

    final Server server;

    public WumpusServer(final int port, final String path, final SessionHandler sessions) {
        this.server = new Server(port);
        final ServletContextHandler handler = new ServletContextHandler("/");
        this.server.setHandler(handler);
        WumpusConfigurator.configure(handler, sessions, path);
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }
}
