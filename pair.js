const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const ytdl = require('ytdl-core');
const yts = require('yt-search');
const FileType = require('file-type');
const mongoose = require('mongoose');

if (fs.existsSync('2nd_dev_config.env')) require('dotenv').config({ path: './2nd_dev_config.env' });

const { sms } = require("./msg");

// FIXED BAILEYS IMPORT
const baileysImport = require('@whiskeysockets/baileys');
const {
    default: makeWASocket,
    useMultiFileAuthState,
    delay,
    makeCacheableSignalKeyStore,
    Browsers,
    jidNormalizedUser,
    proto,
    prepareWAMessageMedia,
    downloadContentFromMessage,
    getContentType,
    generateWAMessageFromContent,
    DisconnectReason,
    fetchLatestBaileysVersion
} = baileysImport;

// MongoDB Configuration
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://ellyongiro8:QwXDXE6tyrGpUTNb@cluster0.tyxcmm9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';

process.env.NODE_ENV = 'production';
process.env.PM2_NAME = 'breshyb';

console.log('🚀 Auto Session Manager initialized with MongoDB');

const config = {
    // General Bot Settings
    AUTO_VIEW_STATUS: 'true',
    AUTO_LIKE_STATUS: 'true',
    AUTO_LIKE_EMOJI: ['💗', '🩵', '🥺', '🫶', '😶'],
    
    // Auto-management Settings
    AUTO_SAVE_INTERVAL: 300000,
    AUTO_CLEANUP_INTERVAL: 900000,
    AUTO_RECONNECT_INTERVAL: 300000,
    AUTO_RESTORE_INTERVAL: 1800000,
    MONGODB_SYNC_INTERVAL: 600000,
    MAX_SESSION_AGE: 604800000,
    DISCONNECTED_CLEANUP_TIME: 300000,
    MAX_FAILED_ATTEMPTS: 3,
    INITIAL_RESTORE_DELAY: 10000,
    IMMEDIATE_DELETE_DELAY: 60000,

    // Command Settings
    PREFIX: '.',
    MAX_RETRIES: 3,

    // File Paths
    IMAGE_PATH: 'https://i.ibb.co/zhm2RF8j/vision-v.jpg',
    NUMBER_LIST_PATH: './numbers.json',
    SESSION_STATUS_PATH: './session_status.json',
    SESSION_BASE_PATH: './session',

    // Owner Details
    OWNER_NUMBER: '254740007567',
};

// Session Management Maps
const activeSockets = new Map();
const socketCreationTime = new Map();
const disconnectionTime = new Map();
const sessionHealth = new Map();
const reconnectionAttempts = new Map();
const lastBackupTime = new Map();
const pendingSaves = new Map();
const restoringNumbers = new Set();
const sessionConnectionStatus = new Map();
const stores = new Map();

// Auto-management intervals
let autoSaveInterval;
let autoCleanupInterval;
let autoReconnectInterval;
let autoRestoreInterval;
let mongoSyncInterval;

// MongoDB Connection
let mongoConnected = false;

// MongoDB Schemas
const sessionSchema = new mongoose.Schema({
    number: { type: String, required: true, unique: true, index: true },
    sessionData: { type: Object, required: true },
    status: { type: String, default: 'active', index: true },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
    lastActive: { type: Date, default: Date.now },
    health: { type: String, default: 'active' }
});

const Session = mongoose.model('Session', sessionSchema);

// Initialize MongoDB Connection
async function initializeMongoDB() {
    try {
        if (mongoConnected) return true;

        await mongoose.connect(MONGODB_URI, {
            serverSelectionTimeoutMS: 30000,
            socketTimeoutMS: 45000,
            maxPoolSize: 10,
            minPoolSize: 5
        });

        mongoConnected = true;
        console.log('✅ MongoDB Atlas connected successfully');

        await Session.createIndexes().catch(err => console.error('Index creation error:', err));

        return true;
    } catch (error) {
        console.error('❌ MongoDB connection error:', error.message);
        mongoConnected = false;

        setTimeout(() => {
            initializeMongoDB();
        }, 5000);

        return false;
    }
}

// MongoDB Session Management Functions
async function saveSessionToMongoDB(number, sessionData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        if (!isSessionActive(sanitizedNumber)) {
            console.log(`⏭️ Not saving inactive session to MongoDB: ${sanitizedNumber}`);
            return false;
        }

        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            {
                sessionData: sessionData,
                status: 'active',
                updatedAt: new Date(),
                lastActive: new Date(),
                health: sessionHealth.get(sanitizedNumber) || 'active'
            },
            { upsert: true, new: true }
        );

        console.log(`✅ Session saved to MongoDB: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB save failed for ${number}:`, error.message);
        pendingSaves.set(number, {
            data: sessionData,
            timestamp: Date.now()
        });
        return false;
    }
}

async function loadSessionFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        const session = await Session.findOne({ 
            number: sanitizedNumber,
            status: { $ne: 'deleted' }
        });

        if (session) {
            console.log(`✅ Session loaded from MongoDB: ${sanitizedNumber}`);
            return session.sessionData;
        }

        return null;
    } catch (error) {
        console.error(`❌ MongoDB load failed for ${number}:`, error.message);
        return null;
    }
}

async function deleteSessionFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        await Session.deleteOne({ number: sanitizedNumber });

        console.log(`🗑️ Session deleted from MongoDB: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB delete failed for ${number}:`, error.message);
        return false;
    }
}

async function getAllActiveSessionsFromMongoDB() {
    try {
        const sessions = await Session.find({ 
            status: 'active',
            health: { $ne: 'invalid' }
        });

        console.log(`📊 Found ${sessions.length} active sessions in MongoDB`);
        return sessions;
    } catch (error) {
        console.error('❌ Failed to get sessions from MongoDB:', error.message);
        return [];
    }
}

async function updateSessionStatusInMongoDB(number, status, health = null) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        const updateData = {
            status: status,
            updatedAt: new Date()
        };

        if (health) {
            updateData.health = health;
        }

        if (status === 'active') {
            updateData.lastActive = new Date();
        }

        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            updateData,
            { upsert: false }
        );

        console.log(`📝 Session status updated in MongoDB: ${sanitizedNumber} -> ${status}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB status update failed for ${number}:`, error.message);
        return false;
    }
}

async function cleanupInactiveSessionsFromMongoDB() {
    try {
        const result = await Session.deleteMany({
            $or: [
                { status: 'disconnected' },
                { status: 'invalid' },
                { status: 'failed' },
                { status: 'bad_mac_cleared' },
                { health: 'invalid' },
                { health: 'disconnected' },
                { health: 'bad_mac_cleared' }
            ]
        });

        console.log(`🧹 Cleaned ${result.deletedCount} inactive sessions from MongoDB`);
        return result.deletedCount;
    } catch (error) {
        console.error('❌ MongoDB cleanup failed:', error.message);
        return 0;
    }
}

async function getMongoSessionCount() {
    try {
        const count = await Session.countDocuments({ status: 'active' });
        return count;
    } catch (error) {
        console.error('❌ Failed to count MongoDB sessions:', error.message);
        return 0;
    }
}

// Create necessary directories
function initializeDirectories() {
    const dirs = [
        config.SESSION_BASE_PATH,
        './temp'
    ];

    dirs.forEach(dir => {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
            console.log(`📁 Created directory: ${dir}`);
        }
    });
}

initializeDirectories();

// Session validation function
async function validateSessionData(sessionData) {
    try {
        if (!sessionData || typeof sessionData !== 'object') {
            return false;
        }

        if (!sessionData.me || !sessionData.myAppStateKeyId) {
            return false;
        }

        const requiredFields = ['noiseKey', 'signedIdentityKey', 'signedPreKey', 'registrationId'];
        for (const field of requiredFields) {
            if (!sessionData[field]) {
                console.warn(`⚠️ Missing required field: ${field}`);
                return false;
            }
        }

        return true;
    } catch (error) {
        console.error('❌ Session validation error:', error);
        return false;
    }
}

// Handle Bad MAC errors
async function handleBadMacError(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    console.log(`🔧 Handling Bad MAC error for ${sanitizedNumber}`);

    try {
        if (activeSockets.has(sanitizedNumber)) {
            const socket = activeSockets.get(sanitizedNumber);
            try {
                if (socket?.ws) {
                    socket.ws.close();
                } else if (socket?.end) {
                    socket.end();
                } else if (socket?.logout) {
                    await socket.logout();
                }
            } catch (e) {
                console.error('Error closing socket:', e.message);
            }
            activeSockets.delete(sanitizedNumber);
        }

        if (stores.has(sanitizedNumber)) {
            stores.delete(sanitizedNumber);
        }

        const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        if (fs.existsSync(sessionPath)) {
            console.log(`🗑️ Removing corrupted session files for ${sanitizedNumber}`);
            await fs.remove(sessionPath);
        }

        await deleteSessionFromMongoDB(sanitizedNumber);

        sessionHealth.set(sanitizedNumber, 'bad_mac_cleared');
        reconnectionAttempts.delete(sanitizedNumber);
        disconnectionTime.delete(sanitizedNumber);
        sessionConnectionStatus.delete(sanitizedNumber);
        pendingSaves.delete(sanitizedNumber);
        lastBackupTime.delete(sanitizedNumber);
        restoringNumbers.delete(sanitizedNumber);

        await updateSessionStatus(sanitizedNumber, 'bad_mac_cleared', new Date().toISOString());

        console.log(`✅ Cleared Bad MAC session for ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ Failed to handle Bad MAC for ${sanitizedNumber}:`, error);
        return false;
    }
}

async function downloadAndSaveMedia(message, mediaType) {
    try {
        const stream = await downloadContentFromMessage(message, mediaType);
        let buffer = Buffer.from([]);

        for await (const chunk of stream) {
            buffer = Buffer.concat([buffer, chunk]);
        }

        return buffer;
    } catch (error) {
        console.error('Download Media Error:', error);
        throw error;
    }
}

// Check if command is from owner
function isOwner(sender) {
    const senderNumber = sender.replace('@s.whatsapp.net', '').replace(/[^0-9]/g, '');
    const ownerNumber = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    return senderNumber === ownerNumber;
}

// **SESSION MANAGEMENT**

function isSessionActive(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const health = sessionHealth.get(sanitizedNumber);
    const connectionStatus = sessionConnectionStatus.get(sanitizedNumber);
    const socket = activeSockets.get(sanitizedNumber);

    return (
        connectionStatus === 'open' &&
        health === 'active' &&
        socket &&
        socket.user &&
        !disconnectionTime.has(sanitizedNumber)
    );
}

// Check if socket is ready for operations
function isSocketReady(socket) {
    if (!socket) return false;
    return socket.ws && socket.ws.readyState === socket.ws.OPEN;
}

async function saveSessionLocally(number, sessionData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        if (!isSessionActive(sanitizedNumber)) {
            console.log(`⏭️ Skipping local save for inactive session: ${sanitizedNumber}`);
            return false;
        }

        const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);

        await fs.ensureDir(sessionPath);

        await fs.writeFile(
            path.join(sessionPath, 'creds.json'),
            JSON.stringify(sessionData, null, 2)
        );

        console.log(`💾 Active session saved locally: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ Failed to save session locally for ${number}:`, error);
        return false;
    }
}

async function restoreSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        const sessionData = await loadSessionFromMongoDB(sanitizedNumber);

        if (sessionData) {
            if (!validateSessionData(sessionData)) {
                console.warn(`⚠️ Invalid session data for ${sanitizedNumber}, clearing...`);
                await handleBadMacError(sanitizedNumber);
                return null;
            }

            await saveSessionLocally(sanitizedNumber, sessionData);
            console.log(`✅ Restored valid session from MongoDB: ${sanitizedNumber}`);
            return sessionData;
        }

        return null;
    } catch (error) {
        console.error(`❌ Session restore failed for ${number}:`, error.message);

        if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
            await handleBadMacError(number);
        }

        return null;
    }
}

async function deleteSessionImmediately(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');

    console.log(`🗑️ Immediately deleting inactive/invalid session: ${sanitizedNumber}`);

    if (activeSockets.has(sanitizedNumber)) {
        const socket = activeSockets.get(sanitizedNumber);
        try {
            if (socket?.ws) {
                socket.ws.close();
            } else if (socket?.end) {
                socket.end();
            } else if (socket?.logout) {
                await socket.logout();
            }
        } catch (e) {
            console.error('Error closing socket:', e.message);
        }
    }

    const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
    if (fs.existsSync(sessionPath)) {
        await fs.remove(sessionPath);
        console.log(`🗑️ Deleted session directory: ${sanitizedNumber}`);
    }

    await deleteSessionFromMongoDB(sanitizedNumber);

    pendingSaves.delete(sanitizedNumber);
    sessionConnectionStatus.delete(sanitizedNumber);
    disconnectionTime.delete(sanitizedNumber);
    sessionHealth.delete(sanitizedNumber);
    reconnectionAttempts.delete(sanitizedNumber);
    socketCreationTime.delete(sanitizedNumber);
    lastBackupTime.delete(sanitizedNumber);
    restoringNumbers.delete(sanitizedNumber);
    activeSockets.delete(sanitizedNumber);
    stores.delete(sanitizedNumber);

    await updateSessionStatus(sanitizedNumber, 'deleted', new Date().toISOString());

    console.log(`✅ Successfully deleted all data for inactive session: ${sanitizedNumber}`);
}

// **AUTO MANAGEMENT FUNCTIONS**

function initializeAutoManagement() {
    console.log('🔄 Starting optimized auto management with MongoDB...');

    initializeMongoDB().then(() => {
        setTimeout(async () => {
            console.log('🔄 Initial auto-restore on startup...');
            await autoRestoreAllSessions();
        }, config.INITIAL_RESTORE_DELAY);
    });

    autoSaveInterval = setInterval(async () => {
        console.log('💾 Auto-saving active sessions...');
        await autoSaveAllActiveSessions();
    }, config.AUTO_SAVE_INTERVAL);

    mongoSyncInterval = setInterval(async () => {
        console.log('🔄 Syncing active sessions with MongoDB...');
        await syncPendingSavesToMongoDB();
    }, config.MONGODB_SYNC_INTERVAL);

    autoCleanupInterval = setInterval(async () => {
        console.log('🧹 Auto-cleaning inactive sessions...');
        await autoCleanupInactiveSessions();
    }, config.AUTO_CLEANUP_INTERVAL);

    autoReconnectInterval = setInterval(async () => {
        console.log('🔗 Auto-checking reconnections...');
        await autoReconnectFailedSessions();
    }, config.AUTO_RECONNECT_INTERVAL);

    autoRestoreInterval = setInterval(async () => {
        console.log('🔄 Hourly auto-restore check...');
        await autoRestoreAllSessions();
    }, config.AUTO_RESTORE_INTERVAL);
}

async function syncPendingSavesToMongoDB() {
    if (pendingSaves.size === 0) {
        console.log('✅ No pending saves to sync with MongoDB');
        return;
    }

    console.log(`🔄 Syncing ${pendingSaves.size} pending saves to MongoDB...`);
    let successCount = 0;
    let failCount = 0;

    for (const [number, sessionInfo] of pendingSaves) {
        if (!isSessionActive(number)) {
            console.log(`⏭️ Session became inactive, skipping: ${number}`);
            pendingSaves.delete(number);
            continue;
        }

        try {
            const success = await saveSessionToMongoDB(number, sessionInfo.data);
            if (success) {
                pendingSaves.delete(number);
                successCount++;
            } else {
                failCount++;
            }
            await delay(500);
        } catch (error) {
            console.error(`❌ Failed to save ${number} to MongoDB:`, error.message);
            failCount++;
        }
    }

    console.log(`✅ MongoDB sync completed: ${successCount} saved, ${failCount} failed, ${pendingSaves.size} pending`);
}

async function autoSaveAllActiveSessions() {
    try {
        let savedCount = 0;
        let skippedCount = 0;

        for (const [number, socket] of activeSockets) {
            if (isSessionActive(number)) {
                const success = await autoSaveSession(number);
                if (success) {
                    savedCount++;
                } else {
                    skippedCount++;
                }
            } else {
                console.log(`⏭️ Skipping save for inactive session: ${number}`);
                skippedCount++;
                await deleteSessionImmediately(number);
            }
        }

        console.log(`✅ Auto-save completed: ${savedCount} active saved, ${skippedCount} skipped/deleted`);
    } catch (error) {
        console.error('❌ Auto-save all sessions failed:', error);
    }
}

async function autoSaveSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        if (!isSessionActive(sanitizedNumber)) {
            console.log(`⏭️ Not saving inactive session: ${sanitizedNumber}`);
            return false;
        }

        const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        const credsPath = path.join(sessionPath, 'creds.json');

        if (fs.existsSync(credsPath)) {
            const fileContent = await fs.readFile(credsPath, 'utf8');
            const credData = JSON.parse(fileContent);

            if (!validateSessionData(credData)) {
                console.warn(`⚠️ Invalid session data during auto-save: ${sanitizedNumber}`);
                await handleBadMacError(sanitizedNumber);
                return false;
            }

            await saveSessionToMongoDB(sanitizedNumber, credData);

            await updateSessionStatusInMongoDB(sanitizedNumber, 'active', 'active');
            await updateSessionStatus(sanitizedNumber, 'active', new Date().toISOString());

            return true;
        }
        return false;
    } catch (error) {
        console.error(`❌ Failed to auto-save session for ${number}:`, error);

        if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
            await handleBadMacError(number);
        }

        return false;
    }
}

async function autoCleanupInactiveSessions() {
    try {
        const sessionStatus = await loadSessionStatus();
        let cleanedCount = 0;

        for (const [number, socket] of activeSockets) {
            const isActive = isSessionActive(number);
            const status = sessionStatus[number]?.status || 'unknown';
            const disconnectedTimeValue = disconnectionTime.get(number);

            const shouldDelete =
                !isActive ||
                (disconnectedTimeValue && (Date.now() - disconnectedTimeValue > config.DISCONNECTED_CLEANUP_TIME)) ||
                ['failed', 'invalid', 'max_attempts_reached', 'deleted', 'disconnected', 'bad_mac_cleared'].includes(status);

            if (shouldDelete) {
                await deleteSessionImmediately(number);
                cleanedCount++;
            }
        }

        const mongoCleanedCount = await cleanupInactiveSessionsFromMongoDB();
        cleanedCount += mongoCleanedCount;

        console.log(`✅ Auto-cleanup completed: ${cleanedCount} inactive sessions cleaned`);
    } catch (error) {
        console.error('❌ Auto-cleanup failed:', error);
    }
}

async function autoReconnectFailedSessions() {
    try {
        const sessionStatus = await loadSessionStatus();
        let reconnectCount = 0;

        for (const [number, status] of Object.entries(sessionStatus)) {
            if (status.status === 'failed' && !activeSockets.has(number) && !restoringNumbers.has(number)) {
                const attempts = reconnectionAttempts.get(number) || 0;
                const disconnectedTimeValue = disconnectionTime.get(number);

                if (disconnectedTimeValue && (Date.now() - disconnectedTimeValue > config.DISCONNECTED_CLEANUP_TIME)) {
                    console.log(`⏭️ Deleting long-disconnected session: ${number}`);
                    await deleteSessionImmediately(number);
                    continue;
                }

                if (attempts < config.MAX_FAILED_ATTEMPTS) {
                    console.log(`🔄 Auto-reconnecting ${number} (attempt ${attempts + 1})`);
                    reconnectionAttempts.set(number, attempts + 1);
                    restoringNumbers.add(number);

                    const mockRes = {
                        headersSent: false,
                        send: () => { },
                        status: () => mockRes
                    };

                    await EmpirePair(number, mockRes);
                    reconnectCount++;
                    await delay(5000);
                } else {
                    console.log(`❌ Max reconnection attempts reached, deleting ${number}`);
                    await deleteSessionImmediately(number);
                }
            }
        }

        console.log(`✅ Auto-reconnect completed: ${reconnectCount} sessions reconnected`);
    } catch (error) {
        console.error('❌ Auto-reconnect failed:', error);
    }
}

async function autoRestoreAllSessions() {
    try {
        if (!mongoConnected) {
            console.log('⚠️ MongoDB not connected, skipping auto-restore');
            return { restored: [], failed: [] };
        }

        console.log('🔄 Starting auto-restore process from MongoDB...');
        const restoredSessions = [];
        const failedSessions = [];

        const mongoSessions = await getAllActiveSessionsFromMongoDB();

        for (const session of mongoSessions) {
            const number = session.number;

            if (activeSockets.has(number) || restoringNumbers.has(number)) {
                continue;
            }

            try {
                console.log(`🔄 Restoring session from MongoDB: ${number}`);
                restoringNumbers.add(number);

                if (!validateSessionData(session.sessionData)) {
                    console.warn(`⚠️ Invalid session data in MongoDB, clearing: ${number}`);
                    await handleBadMacError(number);
                    failedSessions.push(number);
                    continue;
                }

                await saveSessionLocally(number, session.sessionData);

                const mockRes = {
                    headersSent: false,
                    send: () => { },
                    status: () => mockRes
                };

                await EmpirePair(number, mockRes);
                restoredSessions.push(number);

                await delay(3000);
            } catch (error) {
                console.error(`❌ Failed to restore session ${number}:`, error.message);
                failedSessions.push(number);
                restoringNumbers.delete(number);

                if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
                    await handleBadMacError(number);
                } else {
                    await updateSessionStatusInMongoDB(number, 'failed', 'disconnected');
                }
            }
        }

        console.log(`✅ Auto-restore completed: ${restoredSessions.length} restored, ${failedSessions.length} failed`);

        if (restoredSessions.length > 0) {
            console.log(`✅ Restored sessions: ${restoredSessions.join(', ')}`);
        }

        if (failedSessions.length > 0) {
            console.log(`❌ Failed sessions: ${failedSessions.join(', ')}`);
        }

        return { restored: restoredSessions, failed: failedSessions };
    } catch (error) {
        console.error('❌ Auto-restore failed:', error);
        return { restored: [], failed: [] };
    }
}

async function updateSessionStatus(number, status, timestamp, extra = {}) {
    try {
        const sessionStatus = await loadSessionStatus();
        sessionStatus[number] = {
            status,
            timestamp,
            ...extra
        };
        await saveSessionStatus(sessionStatus);
    } catch (error) {
        console.error('❌ Failed to update session status:', error);
    }
}

async function loadSessionStatus() {
    try {
        if (fs.existsSync(config.SESSION_STATUS_PATH)) {
            return JSON.parse(await fs.readFile(config.SESSION_STATUS_PATH, 'utf8'));
        }
        return {};
    } catch (error) {
        console.error('❌ Failed to load session status:', error);
        return {};
    }
}

async function saveSessionStatus(sessionStatus) {
    try {
        await fs.writeFile(config.SESSION_STATUS_PATH, JSON.stringify(sessionStatus, null, 2));
    } catch (error) {
        console.error('❌ Failed to save session status:', error);
    }
}

// **HELPER FUNCTIONS**

function formatMessage(title, content, footer) {
    return `*${title}*\n\n${content}\n\n> *${footer}*`;
}

function getSriLankaTimestamp() {
    return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss');
}

const createSerial = (size) => {
    return crypto.randomBytes(size).toString('hex').slice(0, size);
}

const myquoted = {
    key: {
        remoteJid: 'status@broadcast',
        participant: '254740007567@s.whatsapp.net',
        fromMe: false,
        id: createSerial(16).toUpperCase()
    },
    message: {
        contactMessage: {
            displayName: "ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ",
            vcard: `BEGIN:VCARD\nVERSION:3.0\nFN:Marisel\nORG:ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ;\nTEL;type=CELL;type=VOICE;waid=254740007567:254740007567\nEND:VCARD`,
            contextInfo: {
                stanzaId: createSerial(16).toUpperCase(),
                participant: "0@s.whatsapp.net",
                quotedMessage: {
                    conversation: "mᥱrᥴᥱძᥱs mіᥒі"
                }
            }
        }
    },
    messageTimestamp: Math.floor(Date.now() / 1000),
    status: 1,
    verifiedBizName: "Meta"
};

async function setupStatusHandlers(socket) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const message = messages[0];
        if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;

        try {
            if (config.AUTO_VIEW_STATUS === 'true') {
                let retries = config.MAX_RETRIES;
                while (retries > 0) {
                    try {
                        await socket.readMessages([message.key]);
                        console.log('Auto-viewed status');
                        break;
                    } catch (error) {
                        retries--;
                        if (retries === 0) throw error;
                        await delay(1000 * (config.MAX_RETRIES - retries));
                    }
                }
            }

            if (config.AUTO_LIKE_STATUS === 'true') {
                const randomEmoji = config.AUTO_LIKE_EMOJI[Math.floor(Math.random() * config.AUTO_LIKE_EMOJI.length)];
                let retries = config.MAX_RETRIES;
                while (retries > 0) {
                    try {
                        await socket.sendMessage(
                            message.key.remoteJid,
                            { react: { text: randomEmoji, key: message.key } },
                            { statusJidList: [message.key.participant] }
                        );
                        console.log(`Reacted to status with ${randomEmoji}`);
                        break;
                    } catch (error) {
                        retries--;
                        console.warn(`Failed to react to status, retries left: ${retries}`, error);
                        if (retries === 0) throw error;
                        await delay(1000 * (config.MAX_RETRIES - retries));
                    }
                }
            }
        } catch (error) {
            console.error('Status handler error:', error);
        }
    });
}

function setupCommandHandlers(socket, number) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;
        
        let command = null;
        let args = [];
        let sender = msg.key.remoteJid;

        // Extract command and arguments
        if (msg.message.conversation) {
            const text = msg.message.conversation.trim();
            if (text.startsWith(config.PREFIX)) {
                const parts = text.slice(config.PREFIX.length).trim().split(/\s+/);
                command = parts[0].toLowerCase();
                args = parts.slice(1);
            }
        } else if (msg.message.extendedTextMessage?.text) {
            const text = msg.message.extendedTextMessage.text.trim();
            if (text.startsWith(config.PREFIX)) {
                const parts = text.slice(config.PREFIX.length).trim().split(/\s+/);
                command = parts[0].toLowerCase();
                args = parts.slice(1);
            }
        }

        if (!command) return;

        console.log(`📥 Command received: ${command} from ${sender}`);

        try {
            switch (command) {
                case 'alive': {
                    try {
                        await socket.sendMessage(sender, { react: { text: '🔮', key: msg.key } });
                        const startTime = socketCreationTime.get(number) || Date.now();
                        const uptime = Math.floor((Date.now() - startTime) / 1000);
                        const hours = Math.floor(uptime / 3600);
                        const minutes = Math.floor((uptime % 3600) / 60);
                        const seconds = Math.floor(uptime % 60);

                        const captionText = `
*┏────〘 ᴍᴇʀᴄᴇᴅᴇs 〙───⊷*
*┃* ʙᴏᴛ ᴜᴘᴛɪᴍᴇ: ${hours}h ${minutes}m ${seconds}s
*┃* ᴀᴄᴛɪᴠᴇ ʙᴏᴛs: ${activeSockets.size}
*┃* ʏᴏᴜʀ ɴᴜᴍʙᴇʀ: ${number}
*┃* ᴍᴇᴍᴏʀʏ ᴜsᴀɢᴇ: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB
*┃* ᴍᴏɴɢᴏᴅʙ: ${mongoConnected ? 'Connected' : 'Connecting...'}
*┃* ᴘᴇɴᴅɪɴɢ sᴀᴠᴇs: ${pendingSaves.size}
*┗──────────────⊷*

> *▫️ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ᴍᴀɪɴ*
> sᴛᴀᴛᴜs: ONLINE ✅
> ʀᴇsᴘᴏɴᴅ ᴛɪᴍᴇ: ${Date.now() - msg.messageTimestamp * 1000}ms`;

                        await socket.sendMessage(sender, {
                            image: { url: config.IMAGE_PATH },
                            caption: `> ᴀᴍ ᴀʟɪᴠᴇ ɴ ᴋɪᴄᴋɪɴɢ 🥳\n\n${captionText}`
                        }, { quoted: myquoted });
                    } catch (error) {
                        console.error('Alive command error:', error);
                        const startTime = socketCreationTime.get(number) || Date.now();
                        const uptime = Math.floor((Date.now() - startTime) / 1000);
                        const hours = Math.floor(uptime / 3600);
                        const minutes = Math.floor((uptime % 3600) / 60);
                        const seconds = Math.floor(uptime % 60);

                        await socket.sendMessage(sender, {
                            image: { url: config.IMAGE_PATH },
                            caption: `*🤖 ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ᴀʟɪᴠᴇ*\n\n` +
                                    `*┏────〘 ᴍᴇʀᴄᴇᴅᴇs 〙───⊷*\n` +
                                    `*┃* ᴜᴘᴛɪᴍᴇ: ${hours}h ${minutes}m ${seconds}s\n` +
                                    `*┃* sᴛᴀᴛᴜs: ᴏɴʟɪɴᴇ\n` +
                                    `*┃* ɴᴜᴍʙᴇʀ: ${number}\n` +
                                    `*┃* ᴀᴄᴛɪᴠᴇ ᴜsᴇʀs: ${activeSockets.size}\n` +
                                    `*┃* ᴍᴏɴɢᴏᴅʙ: ${mongoConnected ? 'Connected' : 'Connecting...'}\n` +
                                    `*┃* ᴘᴇɴᴅɪɴɢ sᴀᴠᴇs: ${pendingSaves.size}\n` +
                                    `*┗──────────────⊷*\n\n` +
                                    `Type *${config.PREFIX}menu* for commands`
                        }, { quoted: myquoted });
                    }
                    break;
                }

                case 'song': {
                    try {
                        const q = args.join(' ');

                        if (!q || q.trim() === '') {
                            return await socket.sendMessage(sender, { text: '*`Need YT_URL or Title`*' }, { quoted: myquoted });
                        }

                        await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });

                        const search = await yts(q);
                        if (!search?.videos || search.videos.length === 0) {
                            return await socket.sendMessage(sender, { text: '*`No results found`*' }, { quoted: myquoted });
                        }

                        const data = search.videos[0];
                        const url = data.url;
                        const desc = `
*ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ʙᴏᴛ ᴘʟᴀʏᴇʀ*
*┏────〘 ᴍᴜsɪᴄ 〙/───⊷*
*┃*  *ɢᴇᴛ ʙᴏᴛ* - https://up-tlm1.onrender.com/
*┃*  *Title:* ${data.title} 🎧
*┃*  *Duration:* ${data.timestamp}
*┃*  *Uploaded On:* ${data.ago}
*┗──────────────⊷*
> © ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ
`;

                        await socket.sendMessage(sender, {
                            image: { url: data.thumbnail },
                            caption: desc
                        }, { quoted: myquoted });

                        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

                        // Using ytdl-core for audio download
                        const stream = ytdl(url, { filter: 'audioonly', quality: 'highestaudio' });
                        const chunks = [];
                        
                        for await (const chunk of stream) {
                            chunks.push(chunk);
                        }
                        
                        const buffer = Buffer.concat(chunks);

                        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

                        await socket.sendMessage(sender, {
                            audio: buffer,
                            mimetype: "audio/mpeg",
                            ptt: true
                        }, { quoted: myquoted });

                    } catch (err) {
                        console.error("Song download error:", err);
                        await socket.sendMessage(sender, { text: "*`Error occurred while downloading: " + (err.message || "Unknown error") + "`*" }, { quoted: myquoted });
                    }
                    break;
                }

                case 'video': {
                    try {
                        if (!args[0]) {
                            return await socket.sendMessage(sender, {
                                text: '*❌ Please provide a YouTube URL or search query*'
                            }, { quoted: myquoted });
                        }

                        const query = args.join(' ');
                        let videoUrl = query;

                        // If not a URL, search for it
                        if (!query.includes('youtube.com') && !query.includes('youtu.be')) {
                            await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });

                            const search = await yts(query);
                            if (!search?.videos || search.videos.length === 0) {
                                return await socket.sendMessage(sender, {
                                    text: '*❌ No videos found*'
                                }, { quoted: myquoted });
                            }

                            videoUrl = search.videos[0].url;
                        }

                        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

                        // Using ytdl-core for video download
                        const stream = ytdl(videoUrl, { quality: 'highest' });
                        const chunks = [];
                        
                        for await (const chunk of stream) {
                            chunks.push(chunk);
                        }
                        
                        const buffer = Buffer.concat(chunks);

                        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

                        await socket.sendMessage(sender, {
                            video: buffer,
                            caption: formatMessage(
                                '🎬 ʏᴏᴜᴛᴜʙᴇ ᴠɪᴅᴇᴏ ᴅᴏᴡɴʟᴏᴀᴅ',
                                `✅ *Video Downloaded Successfully*`,
                                'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                            )
                        }, { quoted: myquoted });

                    } catch (error) {
                        console.error('❌ Video download error:', error);
                        await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
                        await socket.sendMessage(sender, {
                            text: `*❌ Failed to download video*`
                        }, { quoted: myquoted });
                    }
                    break;
                }

                case 'vv':
                case 'viewonce': {
                    try {
                        const quotedMsg = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;

                        if (!quotedMsg) {
                            return await socket.sendMessage(sender, {
                                text: '❌ *Please reply to a ViewOnce message!*'
                            }, { quoted: myquoted });
                        }

                        await socket.sendMessage(sender, {
                            react: { text: '✨', key: msg.key }
                        });

                        let mediaData = null;
                        let mediaType = null;
                        let caption = '';

                        // Check for viewonce media
                        if (quotedMsg.imageMessage?.viewOnce) {
                            mediaData = quotedMsg.imageMessage;
                            mediaType = 'image';
                            caption = mediaData.caption || '';
                        } else if (quotedMsg.videoMessage?.viewOnce) {
                            mediaData = quotedMsg.videoMessage;
                            mediaType = 'video';
                            caption = mediaData.caption || '';
                        } else if (quotedMsg.viewOnceMessage?.message?.imageMessage) {
                            mediaData = quotedMsg.viewOnceMessage.message.imageMessage;
                            mediaType = 'image';
                            caption = mediaData.caption || '';
                        } else if (quotedMsg.viewOnceMessage?.message?.videoMessage) {
                            mediaData = quotedMsg.viewOnceMessage.message.videoMessage;
                            mediaType = 'video';
                            caption = mediaData.caption || '';
                        } else if (quotedMsg.viewOnceMessageV2?.message?.imageMessage) {
                            mediaData = quotedMsg.viewOnceMessageV2.message.imageMessage;
                            mediaType = 'image';
                            caption = mediaData.caption || '';
                        } else if (quotedMsg.viewOnceMessageV2?.message?.videoMessage) {
                            mediaData = quotedMsg.viewOnceMessageV2.message.videoMessage;
                            mediaType = 'video';
                            caption = mediaData.caption || '';
                        } else {
                            return await socket.sendMessage(sender, {
                                text: '❌ *This is not a ViewOnce message or it has already been viewed!*'
                            }, { quoted: myquoted });
                        }

                        if (mediaData && mediaType) {
                            await socket.sendMessage(sender, {
                                text: '⏳ *Retrieving ViewOnce media...*'
                            }, { quoted: myquoted });

                            const buffer = await downloadAndSaveMedia(mediaData, mediaType);

                            const messageContent = caption ?
                                `✅ *ViewOnce ${mediaType} Retrieved*\n\n📝 Caption: ${caption}` :
                                `✅ *ViewOnce ${mediaType} Retrieved*`;

                            if (mediaType === 'image') {
                                await socket.sendMessage(sender, {
                                    image: buffer,
                                    caption: messageContent
                                }, { quoted: myquoted });
                            } else if (mediaType === 'video') {
                                await socket.sendMessage(sender, {
                                    video: buffer,
                                    caption: messageContent
                                }, { quoted: myquoted });
                            }

                            await socket.sendMessage(sender, {
                                react: { text: '✅', key: msg.key }
                            });

                            console.log(`✅ ViewOnce ${mediaType} retrieved for ${sender}`);
                        }

                    } catch (error) {
                        console.error('ViewOnce Error:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to retrieve ViewOnce*`
                        }, { quoted: myquoted });
                    }
                    break;
                }

                case 'ping': {
                    const start = Date.now();
                    await socket.sendMessage(sender, { text: '```*Measuring*...```' }, { quoted: myquoted });
                    const end = Date.now();
                    const responseTime = end - start;

                    await socket.sendMessage(sender, {
                        image: { url: config.IMAGE_PATH },
                        caption: formatMessage(
                            'sᴘᴇᴇᴅ',
                            `🏓 *Pong!*\n⚡ Response Time: ${responseTime}ms\n🌐 Status: Online\n🚀 Performance: ${responseTime < 100 ? 'Excellent' : responseTime < 300 ? 'Good' : 'Average'}`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    }, { quoted: myquoted });
                    break;
                }

                case 'owner': {
                    const ownerVCard = `BEGIN:VCARD\nVERSION:3.0\nFN:Marisel\nTEL;type=CELL;type=VOICE;waid=254740007567:+254740007567\nEND:VCARD`;

                    await socket.sendMessage(sender, {
                        contacts: {
                            displayName: 'ᴍᴀʀɪsᴇʟ',
                            contacts: [{ vcard: ownerVCard }]
                        }
                    }, { quoted: myquoted });

                    await socket.sendMessage(sender, {
                        image: { url: config.IMAGE_PATH },
                        caption: formatMessage(
                            'ᴍᴀʀɪsᴇʟ ɪɴғᴏ',
                            `👤 *Name:*ᴍᴀʀɪsᴇʟ\n📱 *Number:* +254740007567\n🌐 *Website:* https://up-tlm1.onrender.com/`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    }, { quoted: myquoted });
                    break;
                }

                case 'menu': {
                    const menuText = `*┏────〘 ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ 〙───⊷*
*┃* ${config.PREFIX}alive  - Check bot status
*┃* ${config.PREFIX}ping   - Test response speed
*┃* ${config.PREFIX}menu   - Show this menu
*┃* ${config.PREFIX}owner  - Bot owner info
*┗──────────────⊷*

*┏────〘 ᴍᴇᴅɪᴀ ᴅᴏᴡɴʟᴏᴀᴅᴇʀs 〙───⊷*
*┃* ${config.PREFIX}song [name/url] - Download audio
*┃* ${config.PREFIX}video [name/url] - Download video
*┃* ${config.PREFIX}vv - ViewOnce media saver
*┗──────────────⊷*

*┏────〘 sᴇᴛᴛɪɴɢs 〙───⊷*
*┃* Auto View Status: ${config.AUTO_VIEW_STATUS === 'true' ? '✅ ON' : '❌ OFF'}
*┃* Auto Like Status: ${config.AUTO_LIKE_STATUS === 'true' ? '✅ ON' : '❌ OFF'}
*┃* Auto Like Emojis: ${config.AUTO_LIKE_EMOJI.join(', ')}
*┗──────────────⊷*`;

                    await socket.sendMessage(sender, {
                        image: { url: config.IMAGE_PATH },
                        caption: menuText
                    }, { quoted: myquoted });
                    break;
                }

                case 'settings': {
                    const settingsText = `*ᴄᴜʀʀᴇɴᴛ sᴇᴛᴛɪɴɢs*

*Prefix:* ${config.PREFIX}
*Auto View Status:* ${config.AUTO_VIEW_STATUS}
*Auto Like Status:* ${config.AUTO_LIKE_STATUS}
*Auto Like Emojis:* ${config.AUTO_LIKE_EMOJI.join(', ')}

*┏────〘 ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs 〙───⊷*
*┃* ${config.PREFIX}setprefix [new prefix]
*┃* ${config.PREFIX}autoview [on/off]
*┃* ${config.PREFIX}autolike [on/off]
*┃* ${config.PREFIX}setemojis [emoji1 emoji2...]
*┗──────────────⊷*`;

                    await socket.sendMessage(sender, {
                        image: { url: config.IMAGE_PATH },
                        caption: settingsText
                    }, { quoted: myquoted });
                    break;
                }

                case 'setprefix': {
                    if (!args[0]) {
                        return await socket.sendMessage(sender, {
                            text: `*Current prefix:* ${config.PREFIX}\n*Usage:* ${config.PREFIX}setprefix [new prefix]`
                        }, { quoted: msg });
                    }

                    const oldPrefix = config.PREFIX;
                    config.PREFIX = args[0];

                    await socket.sendMessage(sender, {
                        text: `✅ *Prefix changed*\n*Old:* ${oldPrefix}\n*New:* ${config.PREFIX}`
                    }, { quoted: msg });
                    break;
                }

                case 'autoview': {
                    if (!args[0] || !['on', 'off'].includes(args[0].toLowerCase())) {
                        return await socket.sendMessage(sender, {
                            text: `*Current:* ${config.AUTO_VIEW_STATUS}\n*Usage:* ${config.PREFIX}autoview [on/off]`
                        }, { quoted: msg });
                    }

                    config.AUTO_VIEW_STATUS = args[0].toLowerCase() === 'on' ? 'true' : 'false';

                    await socket.sendMessage(sender, {
                        text: `✅ *Auto View Status:* ${config.AUTO_VIEW_STATUS === 'true' ? '✅ ON' : '❌ OFF'}`
                    }, { quoted: msg });
                    break;
                }

                case 'autolike': {
                    if (!args[0] || !['on', 'off'].includes(args[0].toLowerCase())) {
                        return await socket.sendMessage(sender, {
                            text: `*Current:* ${config.AUTO_LIKE_STATUS}\n*Usage:* ${config.PREFIX}autolike [on/off]`
                        }, { quoted: msg });
                    }

                    config.AUTO_LIKE_STATUS = args[0].toLowerCase() === 'on' ? 'true' : 'false';

                    await socket.sendMessage(sender, {
                        text: `✅ *Auto Like Status:* ${config.AUTO_LIKE_STATUS === 'true' ? '✅ ON' : '❌ OFF'}`
                    }, { quoted: msg });
                    break;
                }

                case 'setemojis': {
                    if (args.length === 0) {
                        return await socket.sendMessage(sender, {
                            text: `*Current emojis:* ${config.AUTO_LIKE_EMOJI.join(', ')}\n*Usage:* ${config.PREFIX}setemojis 💗 🔥 ❤️`
                        }, { quoted: msg });
                    }

                    config.AUTO_LIKE_EMOJI = args;

                    await socket.sendMessage(sender, {
                        text: `✅ *Auto Like Emojis Updated:* ${config.AUTO_LIKE_EMOJI.join(', ')}`
                    }, { quoted: msg });
                    break;
                }

                default:
                    await socket.sendMessage(sender, { text: `*Command: ${command} is not available*\n> Type ${config.PREFIX}menu to see available commands` });
                    break;
            }
        } catch (error) {
            console.error('❌ Command handler error:', error);
            await socket.sendMessage(sender, {
                image: { url: config.IMAGE_PATH },
                caption: formatMessage(
                    '❌ ERROR',
                    'An error occurred. Please try again.',
                    'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                )
            });
        }
    });
}

function setupMessageHandlers(socket, number) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        sessionHealth.set(sanitizedNumber, 'active');
    });
}

function setupAutoRestart(socket, number) {
    socket.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        sessionConnectionStatus.set(sanitizedNumber, connection);

        if (qr) {
            console.log('QR Code received for:', sanitizedNumber);
        }

        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const errorMessage = lastDisconnect?.error?.message || '';
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

            disconnectionTime.set(sanitizedNumber, Date.now());
            sessionHealth.set(sanitizedNumber, 'disconnected');
            sessionConnectionStatus.set(sanitizedNumber, 'closed');

            if (statusCode === DisconnectReason.loggedOut || 
                statusCode === DisconnectReason.badSession ||
                errorMessage.includes('Bad MAC') || 
                errorMessage.includes('bad-mac') || 
                errorMessage.includes('decrypt')) {

                console.log(`❌ Bad MAC/Invalid session detected for ${number}, cleaning up...`);
                sessionHealth.set(sanitizedNumber, 'invalid');
                await updateSessionStatus(sanitizedNumber, 'invalid', new Date().toISOString());
                await updateSessionStatusInMongoDB(sanitizedNumber, 'invalid', 'invalid');

                setTimeout(async () => {
                    await handleBadMacError(sanitizedNumber);
                }, config.IMMEDIATE_DELETE_DELAY);
            } else if (shouldReconnect) {
                console.log(`🔄 Connection closed for ${number}, attempting reconnect...`);
                sessionHealth.set(sanitizedNumber, 'reconnecting');
                await updateSessionStatus(sanitizedNumber, 'failed', new Date().toISOString(), {
                    disconnectedAt: new Date().toISOString(),
                    reason: errorMessage
                });
                await updateSessionStatusInMongoDB(sanitizedNumber, 'disconnected', 'reconnecting');

                const attempts = reconnectionAttempts.get(sanitizedNumber) || 0;
                if (attempts < config.MAX_FAILED_ATTEMPTS) {
                    await delay(10000);
                    activeSockets.delete(sanitizedNumber);
                    stores.delete(sanitizedNumber);

                    const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
                    await EmpirePair(number, mockRes);
                } else {
                    console.log(`❌ Max reconnection attempts reached for ${number}, deleting...`);
                    setTimeout(async () => {
                        await deleteSessionImmediately(sanitizedNumber);
                    }, config.IMMEDIATE_DELETE_DELAY);
                }
            } else {
                console.log(`❌ Session logged out for ${number}, cleaning up...`);
                await deleteSessionImmediately(sanitizedNumber);
            }
        } else if (connection === 'open') {
            console.log(`✅ Connection open: ${number}`);
            sessionHealth.set(sanitizedNumber, 'active');
            sessionConnectionStatus.set(sanitizedNumber, 'open');
            reconnectionAttempts.delete(sanitizedNumber);
            disconnectionTime.delete(sanitizedNumber);
            await updateSessionStatus(sanitizedNumber, 'active', new Date().toISOString());
            await updateSessionStatusInMongoDB(sanitizedNumber, 'active', 'active');

            setTimeout(async () => {
                await autoSaveSession(sanitizedNumber);
            }, 5000);
        } else if (connection === 'connecting') {
            sessionHealth.set(sanitizedNumber, 'connecting');
            sessionConnectionStatus.set(sanitizedNumber, 'connecting');
        }
    });
}

// **MAIN PAIRING FUNCTION**

async function EmpirePair(number, res) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);

    console.log(`🔄 Connecting: ${sanitizedNumber}`);

    try {
        await fs.ensureDir(sessionPath);

        const existingCredsPath = path.join(sessionPath, 'creds.json');
        if (fs.existsSync(existingCredsPath)) {
            try {
                const existingCreds = JSON.parse(await fs.readFile(existingCredsPath, 'utf8'));
                if (!validateSessionData(existingCreds)) {
                    console.log(`⚠️ Invalid existing session, clearing: ${sanitizedNumber}`);
                    await handleBadMacError(sanitizedNumber);
                }
            } catch (error) {
                console.log(`⚠️ Corrupted session file, clearing: ${sanitizedNumber}`);
                await handleBadMacError(sanitizedNumber);
            }
        }

        const restoredCreds = await restoreSession(sanitizedNumber);
        if (restoredCreds && validateSessionData(restoredCreds)) {
            await fs.writeFile(
                path.join(sessionPath, 'creds.json'),
                JSON.stringify(restoredCreds, null, 2)
            );
            console.log(`✅ Session restored: ${sanitizedNumber}`);
        }

        const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
        const { version } = await fetchLatestBaileysVersion();
        const logger = pino({ level: 'silent' });

        const socket = makeWASocket({
            version,
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, logger),
            },
            printQRInTerminal: false,
            logger,
            browser: Browsers.macOS('Safari'),
            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: 60000,
            keepAliveIntervalMs: 30000,
            retryRequestDelayMs: 2000,
            maxRetries: 5,
            syncFullHistory: false,
            generateHighQualityLinkPreview: false
        });

        socketCreationTime.set(sanitizedNumber, Date.now());
        sessionHealth.set(sanitizedNumber, 'connecting');
        sessionConnectionStatus.set(sanitizedNumber, 'connecting');

        setupStatusHandlers(socket);
        setupCommandHandlers(socket, sanitizedNumber);
        setupMessageHandlers(socket, sanitizedNumber);
        setupAutoRestart(socket, sanitizedNumber);

        if (!socket.authState.creds.registered) {
            let retries = config.MAX_RETRIES;
            let code;

            while (retries > 0) {
                try {
                    await delay(1500);
                    const pair = "MARISELA";
                    code = await socket.requestPairingCode(sanitizedNumber, pair);
                    console.log(`📱 Generated pairing code for ${sanitizedNumber}: ${code}`);
                    break;
                } catch (error) {
                    retries--;
                    console.warn(`⚠️ Pairing code generation failed, retries: ${retries}`);

                    if (error.message?.includes('MAC')) {
                        await handleBadMacError(sanitizedNumber);
                        throw new Error('Session corrupted, please try again');
                    }

                    if (retries === 0) throw error;
                    await delay(2000 * (config.MAX_RETRIES - retries));
                }
            }

            if (!res.headersSent && code) {
                res.send({ code });
            }
        }

        socket.ev.on('creds.update', async () => {
            try {
                await saveCreds();

                if (isSessionActive(sanitizedNumber)) {
                    const fileContent = await fs.readFile(
                        path.join(sessionPath, 'creds.json'),
                        'utf8'
                    );
                    const credData = JSON.parse(fileContent);

                    if (validateSessionData(credData)) {
                        await saveSessionToMongoDB(sanitizedNumber, credData);
                        console.log(`💾 Valid session credentials updated: ${sanitizedNumber}`);
                    } else {
                        console.warn(`⚠️ Invalid credentials update for ${sanitizedNumber}`);
                    }
                }
            } catch (error) {
                console.error(`❌ Failed to save credentials for ${sanitizedNumber}:`, error);

                if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
                    await handleBadMacError(sanitizedNumber);
                }
            }
        });

        socket.ev.on('connection.update', async (update) => {
            const { connection } = update;

            if (connection === 'open') {
                try {
                    await delay(3000);
                    const userJid = jidNormalizedUser(socket.user.id);

                    activeSockets.set(sanitizedNumber, socket);
                    sessionHealth.set(sanitizedNumber, 'active');
                    sessionConnectionStatus.set(sanitizedNumber, 'open');
                    disconnectionTime.delete(sanitizedNumber);
                    restoringNumbers.delete(sanitizedNumber);

                    await socket.sendMessage(userJid, {
                        image: { url: config.IMAGE_PATH },
                        caption: formatMessage(
                            'ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ʙᴏᴛ',
                            `ᴄᴏɴɴᴇᴄᴛ - https://up-tlm1.onrender.com/\n🤖 Auto-connected successfully!\n\n🔢 Number: ${sanitizedNumber}\n🔄 Auto-Reconnect: Active\n☁️ Storage: MongoDB (${mongoConnected ? 'Connected' : 'Connecting...'})\n📋 Pending Saves: ${pendingSaves.size}`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    });

                    await updateSessionStatus(sanitizedNumber, 'active', new Date().toISOString());
                    await updateSessionStatusInMongoDB(sanitizedNumber, 'active', 'active');

                    let numbers = [];
                    if (fs.existsSync(config.NUMBER_LIST_PATH)) {
                        numbers = JSON.parse(await fs.readFile(config.NUMBER_LIST_PATH, 'utf8'));
                    }
                    if (!numbers.includes(sanitizedNumber)) {
                        numbers.push(sanitizedNumber);
                        await fs.writeFile(config.NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
                    }

                    console.log(`✅ Session fully connected and active: ${sanitizedNumber}`);
                } catch (error) {
                    console.error('❌ Connection setup error:', error);
                    sessionHealth.set(sanitizedNumber, 'error');

                    if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
                        await handleBadMacError(sanitizedNumber);
                    }
                }
            }
        });

        return socket;
    } catch (error) {
        console.error(`❌ Pairing error for ${sanitizedNumber}:`, error);

        if (error.message?.includes('Bad MAC') || 
            error.message?.includes('bad-mac') || 
            error.message?.includes('decrypt')) {

            await handleBadMacError(sanitizedNumber);

            if (!res.headersSent) {
                res.status(400).send({
                    error: 'Session corrupted',
                    message: 'Session has been cleared. Please try pairing again.',
                    action: 'retry_pairing'
                });
            }
        } else {
            sessionHealth.set(sanitizedNumber, 'failed');
            sessionConnectionStatus.set(sanitizedNumber, 'failed');
            disconnectionTime.set(sanitizedNumber, Date.now());
            restoringNumbers.delete(sanitizedNumber);

            if (!res.headersSent) {
                res.status(503).send({ error: 'Service Unavailable', details: error.message });
            }
        }

        throw error;
    }
}

// **API ROUTES**

router.get('/', async (req, res) => {
    const { number } = req.query;
    if (!number) {
        return res.status(400).send({ error: 'Number parameter is required' });
    }

    const sanitizedNumber = number.replace(/[^0-9]/g, '');

    if (activeSockets.has(sanitizedNumber)) {
        const isActive = isSessionActive(sanitizedNumber);
        return res.status(200).send({
            status: isActive ? 'already_connected' : 'reconnecting',
            message: isActive ? 'This number is already connected and active' : 'Session is reconnecting',
            health: sessionHealth.get(sanitizedNumber) || 'unknown',
            connectionStatus: sessionConnectionStatus.get(sanitizedNumber) || 'unknown',
            storage: 'MongoDB'
        });
    }

    await EmpirePair(number, res);
});

router.get('/ping', (req, res) => {
    const activeCount = Array.from(activeSockets.keys()).filter(num => isSessionActive(num)).length;

    res.status(200).send({
        status: 'active',
        message: 'AUTO SESSION MANAGER is running with MongoDB',
        activeSessions: activeCount,
        totalSockets: activeSockets.size,
        storage: `MongoDB (${mongoConnected ? 'Connected' : 'Not Connected'})`,
        pendingSaves: pendingSaves.size
    });
});

router.get('/mongodb-status', async (req, res) => {
    try {
        const mongoStatus = mongoose.connection.readyState;
        const states = {
            0: 'disconnected',
            1: 'connected',
            2: 'connecting',
            3: 'disconnecting'
        };

        const sessionCount = await getMongoSessionCount();

        res.status(200).send({
            status: 'success',
            mongodb: {
                status: states[mongoStatus],
                connected: mongoConnected,
                sessionCount: sessionCount
            }
        });
    } catch (error) {
        res.status(500).send({
            status: 'error',
            message: 'Failed to get MongoDB status',
            error: error.message
        });
    }
});

// **CLEANUP AND PROCESS HANDLERS**

process.on('exit', async () => {
    console.log('🛑 Shutting down auto-management...');

    if (autoSaveInterval) clearInterval(autoSaveInterval);
    if (autoCleanupInterval) clearInterval(autoCleanupInterval);
    if (autoReconnectInterval) clearInterval(autoReconnectInterval);
    if (autoRestoreInterval) clearInterval(autoRestoreInterval);
    if (mongoSyncInterval) clearInterval(mongoSyncInterval);

    await syncPendingSavesToMongoDB().catch(console.error);

    for (const [number, socket] of activeSockets) {
        try {
            if (socket?.ws) {
                socket.ws.close();
            } else if (socket?.end) {
                socket.end();
            } else if (socket?.logout) {
                await socket.logout();
            }
        } catch (error) {
            console.error(`Failed to close socket for ${number}:`, error);
        }
    }

    await mongoose.connection.close();

    console.log('✅ Shutdown complete');
});

process.on('SIGINT', async () => {
    console.log('\n🛑 Received SIGINT, shutting down gracefully...');
    await autoSaveAllActiveSessions();
    await syncPendingSavesToMongoDB();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
    await autoSaveAllActiveSessions();
    await syncPendingSavesToMongoDB();
    process.exit(0);
});

process.on('uncaughtException', (err) => {
    console.error('❌ Uncaught exception:', err);
    syncPendingSavesToMongoDB().catch(console.error);

    setTimeout(() => {
        if (process.env.PM2_NAME) {
            exec(`pm2 restart ${process.env.PM2_NAME}`);
        } else {
            process.exit(1);
        }
    }, 5000);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

// MongoDB connection event handlers
mongoose.connection.on('connected', () => {
    console.log('✅ MongoDB connected');
    mongoConnected = true;
});

mongoose.connection.on('error', (err) => {
    console.error('❌ MongoDB connection error:', err);
    mongoConnected = false;
});

mongoose.connection.on('disconnected', () => {
    console.log('⚠️ MongoDB disconnected');
    mongoConnected = false;

    setTimeout(() => {
        initializeMongoDB();
    }, 5000);
});

// Initialize auto-management on module load
initializeAutoManagement();

// Log startup status
console.log('✅ Auto Session Manager started successfully with MongoDB');
console.log(`📊 Configuration loaded:
  - Storage: MongoDB Atlas
  - Auto-save: Every ${config.AUTO_SAVE_INTERVAL / 60000} minutes
  - MongoDB sync: Every ${config.MONGODB_SYNC_INTERVAL / 60000} minutes
  - Auto-restore: Every ${config.AUTO_RESTORE_INTERVAL / 3600000} hour(s)
  - Auto-cleanup: Every ${config.AUTO_CLEANUP_INTERVAL / 60000} minutes
`);

// Export the router
module.exports = router;
