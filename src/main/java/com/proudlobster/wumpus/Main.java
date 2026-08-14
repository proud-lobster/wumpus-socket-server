package com.proudlobster.wumpus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.proudlobster.wumpus.server.App;

public interface Main {

    Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String... args) throws Exception {
        LOG.info("Wumpus Socket Server");
        LOG.info("by Proud Lobster Games");

        // TODO build properties
        final App app = new App(System.getProperties());
        app.start();

        LOG.info("Application server is running.");
    }
}