const express = require("express");
const https = require("https");
const fs = require("fs");
const os = require("os");
const { Server } = require("socket.io");
const { Pool } = require("pg");
const path = require("path");
const crypto = require("crypto");

const app = express();

const PORT = Number(process.env.PORT || 3000);
const INSTANCE_NAME = process.env.INSTANCE_NAME || "Node-App";

const DB_HOST = process.env.DB_HOST || "172.17.0.59";
const DB_PORT = Number(process.env.DB_PORT || 5432);
const DB_NAME = process.env.DB_NAME || "chat_db";
const DB_USER = process.env.DB_USER || "chat_user";
const DB_PASSWORD = process.env.DB_PASSWORD || "chat_pass_2026";

app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: true, limit: "1mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

const pool = new Pool({
    user: DB_USER,
    host: DB_HOST,
    database: DB_NAME,
    password: DB_PASSWORD,
    port: DB_PORT,
    max: 5,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 2000
});

// ---------- Metrics ----------

let activeRequests = 0;
let ewmaLatencyMs = 0;
const EWMA_ALPHA = 0.2;

app.use((req, res, next) => {
    const start = process.hrtime.bigint();
    activeRequests++;

    res.on("finish", () => {
        activeRequests--;

        const latency =
            Number(process.hrtime.bigint() - start) / 1_000_000;

        if (ewmaLatencyMs === 0) {
            ewmaLatencyMs = latency;
        } else {
            ewmaLatencyMs =
                EWMA_ALPHA * latency +
                (1 - EWMA_ALPHA) * ewmaLatencyMs;
        }
    });

    next();
});

// ---------- CPU sampling ----------

let previousCpu = null;
let cpuPercent = 0;

function readCpuUsage() {
    const cpus = os.cpus();

    let idle = 0;
    let total = 0;

    for (const cpu of cpus) {
        const t = cpu.times;
        idle += t.idle;
        total += t.user + t.nice + t.sys + t.idle + t.irq;
    }

    if (previousCpu) {
        const idleDelta = idle - previousCpu.idle;
        const totalDelta = total - previousCpu.total;

        if (totalDelta > 0) {
            cpuPercent = Math.max(
                0,
                Math.min(100, 100 * (1 - idleDelta / totalDelta))
            );
        }
    }

    previousCpu = { idle, total };
}

setInterval(readCpuUsage, 500);
readCpuUsage();

// ---------- Health ----------

app.get("/health", async (req, res) => {
    try {
        await pool.query("SELECT 1");
        res.status(200).json({
            status: "ok",
            instance: INSTANCE_NAME
        });
    } catch (err) {
        res.status(503).json({
            status: "unhealthy",
            instance: INSTANCE_NAME
        });
    }
});

// ---------- Metrics ----------

app.get("/metrics", async (req, res) => {
    const totalMemory = os.totalmem();
    const freeMemory = os.freemem();
    const memoryPercent =
        ((totalMemory - freeMemory) / totalMemory) * 100;

    res.json({
        instance: INSTANCE_NAME,
        cpu: Number(cpuPercent.toFixed(2)),
        memory: Number(memoryPercent.toFixed(2)),
        activeRequests,
        ewmaLatencyMs: Number(ewmaLatencyMs.toFixed(2)),
        load1: Number((os.loadavg()[0] || 0).toFixed(2)),
        uptime: Math.floor(process.uptime())
    });
});

// ---------- Crypto ----------

const MASTER_KEY = crypto.scryptSync(
    process.env.MASTER_PASSWORD || "password",
    process.env.MASTER_SALT || "salt",
    32
);

function encrypt(text) {
    const nonce = crypto.randomBytes(12);

    const cipher = crypto.createCipheriv(
        "aes-256-gcm",
        MASTER_KEY,
        nonce
    );

    let ciphertext = cipher.update(text, "utf8", "hex");
    ciphertext += cipher.final("hex");

    const tag = cipher.getAuthTag().toString("hex");

    return {
        ciphertext: ciphertext + tag,
        nonce: nonce.toString("hex")
    };
}

function decrypt(encData, nonceHex) {
    try {
        const nonce = Buffer.from(nonceHex, "hex");
        const tag = Buffer.from(encData.slice(-32), "hex");
        const ciphertext = encData.slice(0, -32);

        const decipher = crypto.createDecipheriv(
            "aes-256-gcm",
            MASTER_KEY,
            nonce
        );

        decipher.setAuthTag(tag);

        return (
            decipher.update(ciphertext, "hex", "utf8") +
            decipher.final("utf8")
        );
    } catch (err) {
        return "[Decryption Error]";
    }
}

function verifySignature(message, signatureHex, publicKeyJWK) {
    try {
        if (!signatureHex || !publicKeyJWK) return false;

        const key = crypto.createPublicKey({
            key: publicKeyJWK,
            format: "jwk"
        });

        const verifier = crypto.createVerify("SHA256");
        verifier.update(message);

        return verifier.verify(
            {
                key,
                dsaEncoding: "ieee-p1363"
            },
            Buffer.from(signatureHex, "hex")
        );
    } catch (err) {
        return false;
    }
}

// ---------- Database helpers ----------

function getMessageId(req) {
    return (
        req.get("X-Message-ID") ||
        req.body?.["message-id"] ||
        req.body?.message_id ||
        req.body?.id ||
        req.query?.["message-id"] ||
        req.query?.message_id ||
        req.query?.id ||
        crypto.randomUUID()
    );
}

async function insertMessage({
    messageId,
    sender,
    message,
    signature = null,
    publicKey = null
}) {
    const { ciphertext, nonce } = encrypt(message);

    const result = await pool.query(
        `
        INSERT INTO messages
        (
            message_id,
            room_id,
            sender,
            message,
            ciphertext,
            nonce,
            signature,
            public_key,
            origin_node
        )
        VALUES
        ($1, 'LOBBY', $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (message_id) DO NOTHING
        RETURNING
            message_id,
            room_id,
            sender,
            message,
            timestamp,
            origin_node
        `,
        [
            messageId,
            sender,
            message,
            ciphertext,
            nonce,
            signature,
            publicKey ? JSON.stringify(publicKey) : null,
            INSTANCE_NAME
        ]
    );

    return result.rows[0] || null;
}

// ---------- Required API: /message ----------

app.post("/message", async (req, res) => {
    const sender = String(
        req.body?.["client-name"] ??
        req.body?.client_name ??
        req.body?.clientName ??
        req.query?.["client-name"] ??
        req.query?.client_name ??
        req.query?.clientName ??
        ""
    ).trim();

    const message = String(
        req.body?.msg ??
        req.body?.message ??
        req.query?.msg ??
        req.query?.message ??
        ""
    ).trim();

    if (!sender || !message) {
        return res.status(400).json({
            error: "Both client-name and msg are required"
        });
    }

    const messageId = getMessageId(req);

    try {
        const inserted = await insertMessage({
            messageId,
            sender,
            message
        });

        if (!inserted) {
            return res.status(200).json({
                status: "duplicate",
                message_id: messageId
            });
        }

        io.to("LOBBY").emit("chat_message", {
            id: inserted.message_id,
            username: inserted.sender,
            message: inserted.message,
            timestamp: new Date(
                inserted.timestamp
            ).toLocaleTimeString([], {
                hour: "2-digit",
                minute: "2-digit"
            }),
            verified: true
        });

        return res.status(201).json({
            status: "created",
            message_id: inserted.message_id,
            "client-name": inserted.sender,
            msg: inserted.message,
            timestamp: inserted.timestamp
        });
    } catch (err) {
        console.error("POST /message error:", err.message);

        return res.status(500).json({
            error: "Failed to store message"
        });
    }
});

// ---------- Required API: /feed ----------

app.get("/feed", async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT
                message_id,
                sender,
                message,
                timestamp,
                origin_node
            FROM messages
            WHERE room_id = 'LOBBY'
            ORDER BY timestamp ASC, message_id ASC
        `);

        res.json(
            result.rows.map(row => ({
                id: row.message_id,
                message_id: row.message_id,
                "client-name": row.sender,
                msg: row.message,
                sender: row.sender,
                message: row.message,
                timestamp: row.timestamp,
                origin_node: row.origin_node
            }))
        );
    } catch (err) {
        console.error("GET /feed error:", err.message);

        res.status(500).json({
            error: "Failed to retrieve feed"
        });
    }
});

// ---------- Socket.IO ----------

const roomUsers = new Map();

io.on("connection", socket => {

    socket.on("join_room", async data => {
        const username = String(data?.username || "").trim();

        if (!username) return;

        socket.username = username;

        roomUsers.set(socket.id, {
            username,
            publicKey: data.publicKey
        });

        socket.join("LOBBY");

        try {
            const result = await pool.query(`
                SELECT
                    message_id,
                    sender,
                    message,
                    timestamp
                FROM messages
                WHERE room_id = 'LOBBY'
                ORDER BY timestamp ASC, message_id ASC
                LIMIT 50
            `);

            socket.emit(
                "message_history",
                result.rows.map(row => ({
                    id: row.message_id,
                    username: row.sender,
                    message: row.message,
                    timestamp: new Date(
                        row.timestamp
                    ).toLocaleTimeString([], {
                        hour: "2-digit",
                        minute: "2-digit"
                    }),
                    verified: true
                }))
            );
        } catch (err) {
            console.error("History error:", err.message);
            socket.emit("message_history", []);
        }

        socket.emit("room_joined", {
            username,
            users: [...roomUsers.values()].map(u => u.username),
            capacity: 4
        });

        io.to("LOBBY").emit(
            "system_log",
            `${username} joined via ${INSTANCE_NAME}`
        );

        io.to("LOBBY").emit("room_users_update", {
            users: [...roomUsers.values()].map(u => u.username),
            capacity: 4
        });
    });

    socket.on("chat_message", async data => {
        if (!socket.username) return;

        const userData = roomUsers.get(socket.id);

        if (!userData) return;

        const message = String(data?.message || "").trim();

        if (!message) return;

        const isValid = verifySignature(
            message,
            data.signature,
            userData.publicKey
        );

        const messageId =
            data?.id ||
            data?.message_id ||
            crypto.randomUUID();

        try {
            const inserted = await insertMessage({
                messageId,
                sender: socket.username,
                message,
                signature: data.signature || null,
                publicKey: userData.publicKey || null
            });

            if (!inserted) {
                return;
            }

            io.to("LOBBY").emit("chat_message", {
                id: inserted.message_id,
                username: socket.username,
                message,
                timestamp: new Date(
                    inserted.timestamp
                ).toLocaleTimeString([], {
                    hour: "2-digit",
                    minute: "2-digit"
                }),
                verified: isValid
            });
        } catch (err) {
            console.error("DB Insert Error:", err.message);
        }
    });

    socket.on("disconnect", () => {
        if (!socket.username) return;

        roomUsers.delete(socket.id);

        io.to("LOBBY").emit("room_users_update", {
            users: [...roomUsers.values()].map(u => u.username),
            capacity: 4
        });
    });
});

// ---------- Cross-node synchronization ----------

let lastSyncTime = new Date(0);

async function syncMessages() {
    try {
        const result = await pool.query(
            `
            SELECT
                message_id,
                sender,
                message,
                timestamp,
                origin_node
            FROM messages
            WHERE timestamp > $1
            ORDER BY timestamp ASC, message_id ASC
            LIMIT 500
            `,
            [lastSyncTime]
        );

        for (const row of result.rows) {
            const timestamp = new Date(row.timestamp);

            if (timestamp > lastSyncTime) {
                lastSyncTime = timestamp;
            }

            if (row.origin_node === INSTANCE_NAME) {
                continue;
            }

            io.to("LOBBY").emit("chat_message", {
                id: row.message_id,
                username: row.sender,
                message: row.message,
                timestamp: timestamp.toLocaleTimeString([], {
                    hour: "2-digit",
                    minute: "2-digit"
                }),
                verified: true
            });
        }
    } catch (err) {
        // Database may temporarily be unavailable.
    }
}

setInterval(syncMessages, 500);

// ---------- HTTPS ----------

let options;

try {
    options = {
        key: fs.readFileSync(
            process.env.TLS_KEY || "key.pem"
        ),
        cert: fs.readFileSync(
            process.env.TLS_CERT || "cert.pem"
        )
    };
} catch (err) {
    console.error(
        "TLS certificate error:",
        err.message
    );
    process.exit(1);
}

const server = https.createServer(options, app);

const io = new Server(server, {
    cors: {
        origin: "*"
    }
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(
        `[${INSTANCE_NAME}] Server running on HTTPS port ${PORT}`
    );
});
