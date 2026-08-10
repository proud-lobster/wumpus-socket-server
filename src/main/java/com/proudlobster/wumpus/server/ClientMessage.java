package com.proudlobster.wumpus.server;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionalInterface
public interface ClientMessage {

    public static enum Directive {
        LOGIN,
        LOGOUT,
        TOKEN,
        SUCCESS,
        FAILURE,
        PRINT,
        EXECUTE,
        DATA,
        PING;

        @FunctionalInterface
        public interface DirectiveHandler extends UnaryOperator<ClientMessage> {
        }

        private DirectiveHandler handler = m -> null;

        private Optional<ClientMessage> handle(final ClientMessage m) {
            try {
                return Optional.ofNullable(handler.apply(m));
            } catch (final Exception e) {
                LOG.error("Operating error for client message: " + m.whole(), e);
                return Optional.of(Directive.FAILURE.create(m.sessionId(), e.getMessage()));
            }
        }

        public ClientMessage create(final String sessionId, final String payload) {
            return ClientMessage.create(sessionId, this, payload);
        }
    }

    Logger LOG = LoggerFactory.getLogger(ClientMessage.class);
    String DELIMITER = ((char) 0x1e) + "";
    String PAYLOAD_DELIMITER = " ";

    public static void registerHandler(final Directive directive, final Directive.DirectiveHandler handler) {
        directive.handler = handler;
    }
    
    public static ClientMessage create(final String sessionId, final Directive directive, final String payload) {
        final String[] parts = { sessionId.toString(), directive.name(), payload };
        return () -> parts;
    }

    public static ClientMessage create(final String whole) {
        final String[] parts = whole.split(Pattern.quote(DELIMITER));
        return () -> parts;
    }

    String[] parts();

    default String whole() {
        return Arrays.stream(parts()).collect(Collectors.joining(DELIMITER));
    }

    default String part(final int p) {
        return parts()[p];
    }

    default String sessionId() {
        return part(0);
    }

    default Directive directive() {
        return Directive.valueOf(part(1).toUpperCase());
    }

    default String payload() {
        return part(2);
    }

    default String[] payloadParts() {
        return payload().split(Pattern.quote(PAYLOAD_DELIMITER));
    }

    default String payloadPart(final int p) {
        final String[] parts = payloadParts();
        return p < parts.length ? parts[p] : "";
    }

    default String payloadTail(final int from) {
        final String[] parts = payloadParts();
        if (from >= parts.length) {
            return "";
        }
        return Arrays.stream(parts, from, parts.length).collect(Collectors.joining(PAYLOAD_DELIMITER));
    }

    default void handle() {
        directive().handle(this);
    }

}
