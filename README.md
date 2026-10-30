# Wumpus Socket Server
Socket server for the Wumpus console protocol.  By default, routes messages from a console session to a Kafka stream, and from an outbound Kafka stream back to the associated console session.

## Protocol
This server supports non-SSL web socket connections and is intended to be used in conjuction with a SSL ingress listener.  The delivered web socket server implementation is Jetty.

### Messages
Communication over the WS connection is in the form of four-part unencoded string messages, meaning an SSL adapter is necessary for security.  The four parts are delimited with an ASCII record seperator (0x1E).

The four message parts are as follows:
- A session ID unique for the console session, assigned by the server.  This ID is sent by the server to the client upon initial connection.
- An epoch timestamp, in milliseconds.
- A message directive, which must be one of the following values (see descriptions of each directive below):
    - LOGIN
    - LOGOUT
    - TOKEN
    - SUCCESS
    - FAILURE
    - PRINT
    - EXECUTE
    - DATA
    - PING
- A message payload, which for some directives may be an empty string.

### Directives

#### LOGIN
Sent by the client to attempt to authenticate this session with a user account.

There are two valid client payloads for this directive:
- If a payload is sent containing only an email address and no user account already exists for that email address then a new user account will be created for that email address and a one-time code will be sent to that email address to be entered in the console.  Upon receiving a SUCCESS response message, the console should present an opportunity to enter this code (see TOKEN).
- If a payload is sent containing a valid user account email address followed by a space and then a valid client token and a SUCCESS response message is sent back from the server then the console session has been successfully authenticated as belonging to the user account. 

This directive is not sent by the server.

#### LOGOUT