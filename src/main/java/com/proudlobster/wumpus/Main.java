package com.proudlobster.wumpus;

import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.proudlobster.wumpus.server.App;

public interface Main {

    Logger LOG = LoggerFactory.getLogger(Main.class);

    private static Properties loadDefaults() {
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("default.properties")) {
            final Properties prop = new Properties();
            prop.load(input);
            return prop;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String... args) throws Exception {
        LOG.info("Wumpus Socket Server");
        LOG.info("by Proud Lobster Games");

        final Properties props = new Properties();
        props.putAll(loadDefaults());
        props.putAll(System.getProperties());
        final App app = new App(props);
        app.start();

        LOG.info("Application server is running.");
    }
}