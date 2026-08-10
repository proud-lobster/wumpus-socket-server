import websocket

url = "ws://localhost:8080/socket"

def on_message(ws, message):
    print(f"📩 Received message from server: {message}")

def on_error(ws, error):
    print(f"❌ WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"🔌 Connection closed. Code: {close_status_code}, Reason: {close_msg or 'None'}")

def on_open(ws):
    print("✅ Connected successfully to the WebSocket server!")
    # Optional: Send a test message once connected
    ws.send("Hello server!")

if __name__ == "__main__":
    print(f"Connecting to {url}...")
    
    # Enable debug output if you need deep troubleshooting
    # websocket.enableTrace(True)
    
    ws = websocket.WebSocketApp(url,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)

    # Run the connection (blocking call that listens for messages)
    ws.run_forever()