const mqtt = require("mqtt");
const { Client } = require("pg");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const { log } = require("console");

response_tank = {
  "waterLevel": 75,
  "mode": "Automatic",
  "logs": [
    {
      "id": 1,
      "pump_id": 1,
      "state": "ON",
      "timestamp": "2025-04-01T10:00:00Z"
    },
    {
      "id": 2,
      "pump_id": 1,
      "state": "OFF",
      "timestamp": "2025-04-01T10:15:00Z"
    },
    {
      "id": 3,
      "pump_id": 1,
      "state": "ON",
      "timestamp": "2025-04-01T10:30:00Z"
    }
  ],
  "history": [
    {
      "timestamp": "2025-04-01T09:00:00Z",
      "level": 50
    },
    {
      "timestamp": "2025-04-01T09:30:00Z",
      "level": 60
    },
    {
      "timestamp": "2025-04-01T10:00:00Z",
      "level": 75
    },
    {
      "timestamp": "2025-04-01T10:30:00Z",
      "level": 80
    },
    {
      "timestamp": "2025-04-01T11:00:00Z",
      "level": 85
    }
  ]
}


// MQTT Configuration
const MQTT_BROKER = "fa462ba4ab664fe6a8a9c40b953e889f.s1.eu.hivemq.cloud";
const MQTT_USERNAME = "Useitforall123";
const MQTT_PASSWORD = "Useitforall123";
const MQTT_TOPIC = "pumpstate/+/+"; // Subscribe to all pump state messages

// PostgreSQL Configuration
const dbClient = new Client({
  host: 'dpg-cvbd2bt6l47c73ac19qg-a.oregon-postgres.render.com',
  user: 'priyanga',
  password: 'opUHlB9AG9U4EDKRU8eBhLD16yODookI',
  database: 'watertankdb',
  port: 5432,
  ssl: {
    rejectUnauthorized: true,
  },
});

// Express & Socket.io setup
const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server);

// Connect to PostgreSQL
dbClient
  .connect()
  .then(() => console.log("Connected to PostgreSQL"))
  .catch((err) => console.error("PostgreSQL connection error:", err));

// Create tables if not exist
const createTables = async () => {
  const queries = `
    CREATE TABLE IF NOT EXISTS pump_logs (
      id SERIAL PRIMARY KEY,
      pump_id INT NOT NULL,
      state VARCHAR(10) NOT NULL,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS pump_state (
      pump_id INT PRIMARY KEY,
      state VARCHAR(10) NOT NULL,
      mode VARCHAR(10) NOT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
    );

    CREATE TABLE IF NOT EXISTS water_level_logs (
      id SERIAL PRIMARY KEY,
      tank_id VARCHAR(50) NOT NULL,
      water_level FLOAT NOT NULL,
      timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS water_liters_logs (
    id SERIAL PRIMARY KEY,
    house_id VARCHAR(50) NOT NULL,
    liters FLOAT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

  `;
  await dbClient.query(queries);
};
createTables();

// MQTT Configuration
const mqttOptions = {
  host: MQTT_BROKER,
  port: 8883,
  protocol: 'mqtts',
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
};

const mqttClient = mqtt.connect(mqttOptions);

mqttClient.on("connect", () => {
  console.log("Connected to MQTT Broker");
  mqttClient.subscribe(MQTT_TOPIC, (err) => {
    if (err) {
      console.error("MQTT Subscription error:", err);
    } else {
      console.log(`Subscribed to topic: ${MQTT_TOPIC}`);
    }
  });
});

// Handle MQTT messages
mqttClient.on("message", async (topic, message) => {
  try {
    const payload = message.toString();
    const parts = topic.split("/");
    const pumpId = parseInt(parts[2]); // Extract pump ID from topic

    console.log(`Received: Pump ${pumpId} -> ${payload}`);

    // Insert into pump_logs
    await dbClient.query(
      "INSERT INTO pump_logs (pump_id, state) VALUES ($1, $2)",
      [pumpId, payload]
    );

    // Update pump_state table
    const { rows } = await dbClient.query(
      "SELECT state FROM pump_state WHERE pump_id = $1",
      [pumpId]
    );
    if (rows.length === 0 || rows[0].state !== payload) {
      await dbClient.query(
        `INSERT INTO pump_state (pump_id, state, mode, updated_at) 
         VALUES ($1, $2, 'Automatic', CURRENT_TIMESTAMP) 
         ON CONFLICT (pump_id) 
         DO UPDATE SET state = EXCLUDED.state, updated_at = CURRENT_TIMESTAMP;`,
        [pumpId, payload]
      );
    }

    // Emit data to frontend via WebSocket
    io.emit("pumpStateUpdate", { pumpId, state: payload });
  } catch (err) {
    console.error("Error processing MQTT message:", err);
  }
});

// WebSocket connection handler
io.on("connection", (socket) => {
  console.log("Client connected to WebSocket");
});

// API to get data for a tank (Water level, Mode, Logs, etc.)
app.get("/api/tank/:tankId", async (req, res) => {
  const tankId = req.params.tankId;
  console.log(tankId)
  try {
    const pumpStateRes = await dbClient.query("SELECT * FROM pump_state WHERE pump_id = $1", [tankId]);
    const logsRes = await dbClient.query("SELECT * FROM pump_logs WHERE pump_id = $1 ORDER BY timestamp DESC LIMIT 10", [tankId]);
    
    res.json({
      waterLevel: Math.floor(Math.random() * 100), // Simulated data for water level
      mode: pumpStateRes.rows[0]?.mode || "Automatic", // Default "Automatic" mode
      logs: logsRes.rows,
      history: logsRes.rows.map(log => ({ timestamp: log.timestamp, level: Math.floor(Math.random() * 100) })) // Simulated historical data
    });
  } catch (err) {
    console.error("Error fetching tank data:", err);
    //res.status(500).send("Internal Server Error");
    res.status(200).send(response_tank)

  }
});

// API to toggle pump state
app.post("/api/tank/:tankId/pump", async (req, res) => {
  const tankId = req.params.tankId;
  const { state } = req.body; // true for ON, false for OFF
  try {
    const mode = state ? "Manual" : "Automatic"; // Toggle between Manual and Automatic
    await dbClient.query(
      "UPDATE pump_state SET state = $1, mode = $2, updated_at = CURRENT_TIMESTAMP WHERE pump_id = $3",
      [state ? "ON" : "OFF", mode, tankId]
    );

    io.emit("pumpStateUpdate", { pumpId: tankId, state: state ? "ON" : "OFF" });

    res.send({ success: true });
  } catch (err) {
    console.error("Error updating pump state:", err);
    res.status(500).send("Internal Server Error");
  }
});

app.get("/", (req, res) => {
  res.status(200).send("Water tank API")
})

// Start Express server
const PORT = 5000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
