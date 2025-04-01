const mqtt = require("mqtt");
const { Client } = require("pg");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

// MQTT Configuration
const MQTT_BROKER = "fa462ba4ab664fe6a8a9c40b953e889f.s1.eu.hivemq.cloud";
const MQTT_USERNAME = "Useitforall123";
const MQTT_PASSWORD = "Useitforall123";
const MQTT_TOPICS = ["project/pump/+/status", "project/tank/+/level", "project/water/+/litres"];

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

    CREATE TABLE IF NOT EXISTS water_level_logs (
      id SERIAL PRIMARY KEY,
      tank_id INT NOT NULL,
      water_level FLOAT NOT NULL,
      timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS water_liters_logs (
      id SERIAL PRIMARY KEY,
      house_id INT NOT NULL,
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

  // Subscribe to topics
  MQTT_TOPICS.forEach((topic) => {
    mqttClient.subscribe(topic, (err) => {
      if (err) {
        console.error(`MQTT Subscription error for ${topic}:`, err);
      } else {
        console.log(`Subscribed to topic: ${topic}`);
      }
    });
  });
});

// Handle MQTT messages
mqttClient.on("message", async (topic, message) => {
  try {
    const payload = message.toString();
    const parts = topic.split("/");
    const entityType = parts[1]; // project/pump/1/status → "pump"
    const entityId = parseInt(parts[2]); // Extract ID from topic

    console.log(`Received: ${topic} → ${payload}`);

    if (entityType === "pump" && parts[3] === "status") {
      // Store pump status
      await dbClient.query(
        "INSERT INTO pump_logs (pump_id, state) VALUES ($1, $2)",
        [entityId, payload]
      );
      io.emit("pumpStatusUpdate", { pumpId: entityId, state: payload });
    } else if (entityType === "tank" && parts[3] === "level") {
      // Store water level
      await dbClient.query(
        "INSERT INTO water_level_logs (tank_id, water_level) VALUES ($1, $2)",
        [entityId, parseFloat(payload)]
      );
      io.emit("waterLevelUpdate", { tankId: entityId, level: parseFloat(payload) });
    } else if (entityType === "water" && parts[3] === "litres") {
      // Store water delivered
      await dbClient.query(
        "INSERT INTO water_liters_logs (house_id, liters) VALUES ($1, $2)",
        [entityId, parseFloat(payload)]
      );
      io.emit("waterDeliveryUpdate", { houseId: entityId, liters: parseFloat(payload) });
    }
  } catch (err) {
    console.error("Error processing MQTT message:", err);
  }
});

// WebSocket connection handler
io.on("connection", (socket) => {
  console.log("Client connected to WebSocket");
});

// API to get water level logs
app.get("/api/tank/:tankId", async (req, res) => {
  const tankId = req.params.tankId;
  try {
    const logsRes = await dbClient.query(
      "SELECT * FROM water_level_logs WHERE tank_id = $1 ORDER BY timestamp DESC LIMIT 10",
      [tankId]
    );
    res.json({ logs: logsRes.rows });
  } catch (err) {
    console.error("Error fetching tank data:", err);
    res.status(500).send("Internal Server Error");
  }
});

// API to get water delivered logs
app.get("/api/water/:houseId", async (req, res) => {
  const houseId = req.params.houseId;
  try {
    const logsRes = await dbClient.query(
      "SELECT * FROM water_liters_logs WHERE house_id = $1 ORDER BY timestamp DESC LIMIT 10",
      [houseId]
    );
    res.json({ logs: logsRes.rows });
  } catch (err) {
    console.error("Error fetching water data:", err);
    res.status(500).send("Internal Server Error");
  }
});

app.get("/api/pump/:pumpId", async (req, res) => {
  const pumpId = req.params.pumpId;
  try {
    const logsRes = await dbClient.query("SELECT * FROM pump_logs WHERE pump_id = $1 ORDER BY timestamp DESC LIMIT 10", [pumpId]);
    res.json({ logs: logsRes.rows });
  } catch (err) {
    console.error("Error fetching pump data:", err);
    res.status(500).send("Internal Server Error");
  }
});

app.get("/", (req, res) => {
  res.status(200).send("Water Monitoring API");
});

// Start Express server
const PORT = 5000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
