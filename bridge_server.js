/**
 * bridge_server.js — TCP socket bridge between brain.js and scrape.py
 *
 * brain.js starts this server on localhost:9400.
 * scrape.py connects and sends newline-delimited JSON messages.
 * brain.js receives them as tool calls and can respond back.
 */
import net from "net";
import { EventEmitter } from "events";

const BRIDGE_PORT = 9400;

class BridgeServer extends EventEmitter {
  constructor() {
    super();
    this.server = null;
    this.client = null;      // The single scrape.py connection
    this.buffer = "";        // Incoming data buffer
    this._pendingResolve = null; // For blocking request/response
  }

  start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        console.log("🔌 Bridge: scrape.py connected");
        this.client = socket;
        this.buffer = "";

        socket.on("data", (data) => {
          this.buffer += data.toString();
          let newlineIdx;
          while ((newlineIdx = this.buffer.indexOf("\n")) !== -1) {
            const line = this.buffer.slice(0, newlineIdx).trim();
            this.buffer = this.buffer.slice(newlineIdx + 1);
            if (line) {
              try {
                const msg = JSON.parse(line);
                this.emit("message", msg);
              } catch (e) {
                console.log(`🔌 Bridge: bad JSON: ${line.slice(0, 100)}`);
              }
            }
          }
        });

        socket.on("close", () => {
          console.log("🔌 Bridge: scrape.py disconnected");
          this.client = null;
        });

        socket.on("error", (err) => {
          console.log(`🔌 Bridge socket error: ${err.message}`);
          this.client = null;
        });
      });

      this.server.on("error", (err) => {
        if (err.code === "EADDRINUSE") {
          console.log(`🔌 Bridge: port ${BRIDGE_PORT} in use, trying to reuse...`);
          this.server.close();
          this.server.listen(BRIDGE_PORT, "127.0.0.1", () => {
            console.log(`🔌 Bridge server listening on localhost:${BRIDGE_PORT}`);
            resolve();
          });
        } else {
          reject(err);
        }
      });

      this.server.listen(BRIDGE_PORT, "127.0.0.1", () => {
        console.log(`🔌 Bridge server listening on localhost:${BRIDGE_PORT}`);
        resolve();
      });
    });
  }

  /**
   * Send a JSON response back to scrape.py
   */
  respond(responseObj) {
    if (!this.client || this.client.destroyed) {
      console.log("🔌 Bridge: no client connected, can't respond");
      return false;
    }
    try {
      this.client.write(JSON.stringify(responseObj) + "\n");
      return true;
    } catch (e) {
      console.log(`🔌 Bridge: send error: ${e.message}`);
      return false;
    }
  }

  /**
   * Get the last received message (for tool integration)
   */
  getLatestMessage() {
    return this._latestMessage || null;
  }

  stop() {
    if (this.client) {
      this.client.destroy();
      this.client = null;
    }
    if (this.server) {
      this.server.close();
      this.server = null;
    }
  }
}

// Singleton
const bridge = new BridgeServer();

// Store messages in a queue for brain.js to consume
const messageQueue = [];
bridge.on("message", (msg) => {
  messageQueue.push(msg);
  bridge._latestMessage = msg;
});

export { bridge, messageQueue, BRIDGE_PORT };
