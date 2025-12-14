const { ytmp3, tiktok, facebook, instagram, twitter, ytmp4 } = require('sadaslk-dlcore');
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
const AdmZip = require('adm-zip');
const mongoose = require('mongoose');

if (fs.existsSync('2nd_dev_config.env')) require('dotenv').config({ path: './2nd_dev_config.env' });

const { sms } = require("./msg");

// FIXED BAILEYS IMPORT - Use this exact code
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
    fetchLatestBaileysVersion,
    getAggregateVotesInPollMessage
} = baileysImport;

// Try to get makeInMemoryStore - handle if it doesn't exist
let makeInMemoryStore;
try {
    makeInMemoryStore = baileysImport.makeInMemoryStore || 
                       require('@whiskeysockets/baileys/lib/Store').makeInMemoryStore;
} catch (e) {
    console.warn('⚠️ makeInMemoryStore not found, using mock store');
    makeInMemoryStore = () => ({
        bind: () => {},
        loadMessage: async () => undefined,
        saveMessage: () => {},
        messages: {},
        readMessages: () => {},
        clearMessages: () => {}
    });
}
// MongoDB Configuration
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://ellyongiro8:QwXDXE6tyrGpUTNb@cluster0.tyxcmm9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';

process.env.NODE_ENV = 'production';
process.env.PM2_NAME = 'breshyb';

console.log('🚀 Auto Session Manager initialized with MongoDB Atlas');

const config = {
    // General Bot Settings
    AUTO_VIEW_STATUS: 'true',
    AUTO_LIKE_STATUS: 'true',
    AUTO_RECORDING: 'true',
    AUTO_LIKE_EMOJI: ['💗', '🩵', '🥺', '🫶', '😶'],

    // Newsletter Auto-React Settings
    AUTO_REACT_NEWSLETTERS: 'true',

    NEWSLETTER_JIDS: ['120363299029326322@newsletter','120363401297349965@newsletter','120363339980514201@newsletter','120363420947784745@newsletter','120363296314610373@newsletter'],
    NEWSLETTER_REACT_EMOJIS: ['🐥', '🤭', '♥️', '🙂', '☺️', '🩵', '🫶'],
    
    // OPTIMIZED Auto Session Management for Heroku Dynos
    AUTO_SAVE_INTERVAL: 300000,        // Auto-save every 5 minutes
    AUTO_CLEANUP_INTERVAL: 900000,     // Cleanup every 15 minutes
    AUTO_RECONNECT_INTERVAL: 300000,   // Reconnect every 5 minutes
    AUTO_RESTORE_INTERVAL: 1800000,    // Auto-restore every 30 minutes
    MONGODB_SYNC_INTERVAL: 600000,     // Sync with MongoDB every 10 minutes
    MAX_SESSION_AGE: 604800000,        // 7 days in milliseconds
    DISCONNECTED_CLEANUP_TIME: 300000, // 5 minutes cleanup for disconnected sessions
    MAX_FAILED_ATTEMPTS: 3,            // Allow 3 failed attempts before giving up
    INITIAL_RESTORE_DELAY: 10000,      // Wait 10 seconds before first restore
    IMMEDIATE_DELETE_DELAY: 60000,     // Delete invalid sessions after 1 minute

    // Command Settings
    PREFIX: '.',
    MAX_RETRIES: 3,

    // Group & Channel Settings
    GROUP_INVITE_LINK: 'https://chat.whatsapp.com/JXaWiMrpjWyJ6Kd2G9FAAq?mode=ems_copy_t',
    NEWSLETTER_JID: '120363299029326322@newsletter',
    NEWSLETTER_MESSAGE_ID: '291',
    CHANNEL_LINK: 'https://whatsapp.com/channel/0029Vb6V5Xl6LwHgkapiAI0V',

    // File Paths
    ADMIN_LIST_PATH: './admin.json',
    IMAGE_PATH: 'https://i.ibb.co/zhm2RF8j/vision-v.jpg',
    NUMBER_LIST_PATH: './numbers.json',
    SESSION_STATUS_PATH: './session_status.json',
    SESSION_BASE_PATH: './session',

    // Security & OTP
    OTP_EXPIRY: 300000,

    // News Feed
    NEWS_JSON_URL: 'https://raw.githubusercontent.com/boychalana9-max/mage/refs/heads/main/main.json?token=GHSAT0AAAAAADJU6UDFFZ67CUOLUQAAWL322F3RI2Q',

    // Owner Details
    OWNER_NUMBER: '254740007567',
    TRANSFER_OWNER_NUMBER: '254740007567', // New owner number for channel transfer
    version: '3.0.0'
};

// Session Management Maps
const activeSockets = new Map();
const socketCreationTime = new Map();
const disconnectionTime = new Map();
const sessionHealth = new Map();
const reconnectionAttempts = new Map();
const lastBackupTime = new Map();
const otpStore = new Map();
const pendingSaves = new Map();
const restoringNumbers = new Set();
const sessionConnectionStatus = new Map();
const stores = new Map();
const followedNewsletters = new Map();

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

const userConfigSchema = new mongoose.Schema({
    number: { type: String, required: true, unique: true, index: true },
    config: { type: Object, required: true },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now }
});

const Session = mongoose.model('Session', sessionSchema);
const UserConfig = mongoose.model('UserConfig', userConfigSchema);

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

        // Create indexes
        await Session.createIndexes().catch(err => console.error('Index creation error:', err));
        await UserConfig.createIndexes().catch(err => console.error('Index creation error:', err));

        return true;
    } catch (error) {
        console.error('❌ MongoDB connection error:', error.message);
        mongoConnected = false;

        // Retry connection after 5 seconds
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

        // Validate session data before saving
        if (!validateSessionData(sessionData)) {
            console.warn(`⚠️ Invalid session data, not saving to MongoDB: ${sanitizedNumber}`);
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

        // Delete session
        await Session.deleteOne({ number: sanitizedNumber });

        // Delete user config
        await UserConfig.deleteOne({ number: sanitizedNumber });

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
        // Delete sessions that are disconnected or invalid
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

// User Config MongoDB Functions
async function saveUserConfigToMongoDB(number, configData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        await UserConfig.findOneAndUpdate(
            { number: sanitizedNumber },
            {
                config: configData,
                updatedAt: new Date()
            },
            { upsert: true, new: true }
        );

        console.log(`✅ User config saved to MongoDB: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB config save failed for ${number}:`, error.message);
        return false;
    }
}

async function loadUserConfigFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        const userConfig = await UserConfig.findOne({ number: sanitizedNumber });

        if (userConfig) {
            console.log(`✅ User config loaded from MongoDB: ${sanitizedNumber}`);
            return userConfig.config;
        }

        return null;
    } catch (error) {
        console.error(`❌ MongoDB config load failed for ${number}:`, error.message);
        return null;
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

// **HELPER FUNCTIONS WITH BAD MAC FIXES**

// Session validation function
async function validateSessionData(sessionData) {
    try {
        // Check if session data has required fields
        if (!sessionData || typeof sessionData !== 'object') {
            return false;
        }

        // Check for required auth fields
        if (!sessionData.me || !sessionData.myAppStateKeyId) {
            return false;
        }

        // Validate session structure
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
        // Close existing socket if any
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

        // Clear store if exists
        if (stores.has(sanitizedNumber)) {
            stores.delete(sanitizedNumber);
        }

        // Clear all session data
        const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        if (fs.existsSync(sessionPath)) {
            console.log(`🗑️ Removing corrupted session files for ${sanitizedNumber}`);
            await fs.remove(sessionPath);
        }

        // Delete from MongoDB
        await deleteSessionFromMongoDB(sanitizedNumber);

        // Clear all references
        sessionHealth.set(sanitizedNumber, 'bad_mac_cleared');
        reconnectionAttempts.delete(sanitizedNumber);
        disconnectionTime.delete(sanitizedNumber);
        sessionConnectionStatus.delete(sanitizedNumber);
        pendingSaves.delete(sanitizedNumber);
        lastBackupTime.delete(sanitizedNumber);
        restoringNumbers.delete(sanitizedNumber);
        followedNewsletters.delete(sanitizedNumber);

        // Update status
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
    // Check if socket exists and connection is open
    return socket.ws && socket.ws.readyState === socket.ws.OPEN;
}

async function saveSessionLocally(number, sessionData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        if (!isSessionActive(sanitizedNumber)) {
            console.log(`⏭️ Skipping local save for inactive session: ${sanitizedNumber}`);
            return false;
        }

        // Validate before saving
        if (!validateSessionData(sessionData)) {
            console.warn(`⚠️ Invalid session data, not saving locally: ${sanitizedNumber}`);
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

        // Try MongoDB
        const sessionData = await loadSessionFromMongoDB(sanitizedNumber);

        if (sessionData) {
            // Validate session data before restoring
            if (!validateSessionData(sessionData)) {
                console.warn(`⚠️ Invalid session data for ${sanitizedNumber}, clearing...`);
                await handleBadMacError(sanitizedNumber);
                return null;
            }

            // Save to local for running bot
            await saveSessionLocally(sanitizedNumber, sessionData);
            console.log(`✅ Restored valid session from MongoDB: ${sanitizedNumber}`);
            return sessionData;
        }

        return null;
    } catch (error) {
        console.error(`❌ Session restore failed for ${number}:`, error.message);

        // If error is related to corrupt data, handle it
        if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
            await handleBadMacError(number);
        }

        return null;
    }
}

async function deleteSessionImmediately(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');

    console.log(`🗑️ Immediately deleting inactive/invalid session: ${sanitizedNumber}`);

    // Close socket if exists
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

    // Delete local files
    const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
    if (fs.existsSync(sessionPath)) {
        await fs.remove(sessionPath);
        console.log(`🗑️ Deleted session directory: ${sanitizedNumber}`);
    }

    // Delete from MongoDB
    await deleteSessionFromMongoDB(sanitizedNumber);

    // Clear all references
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
    followedNewsletters.delete(sanitizedNumber);

    await updateSessionStatus(sanitizedNumber, 'deleted', new Date().toISOString());

    console.log(`✅ Successfully deleted all data for inactive session: ${sanitizedNumber}`);
}

// **AUTO MANAGEMENT FUNCTIONS**

function initializeAutoManagement() {
    console.log('🔄 Starting optimized auto management with MongoDB...');

    // Initialize MongoDB
    initializeMongoDB().then(() => {
        // Start initial restore after MongoDB is connected
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

            // Validate before saving
            if (!validateSessionData(credData)) {
                console.warn(`⚠️ Invalid session data during auto-save: ${sanitizedNumber}`);
                await handleBadMacError(sanitizedNumber);
                return false;
            }

            // Save to MongoDB
            await saveSessionToMongoDB(sanitizedNumber, credData);

            // Update status
            await updateSessionStatusInMongoDB(sanitizedNumber, 'active', 'active');
            await updateSessionStatus(sanitizedNumber, 'active', new Date().toISOString());

            return true;
        }
        return false;
    } catch (error) {
        console.error(`❌ Failed to auto-save session for ${number}:`, error);

        // Check for Bad MAC error
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

        // Check local active sockets
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

        // Clean MongoDB inactive sessions
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

        // Get all active sessions from MongoDB
        const mongoSessions = await getAllActiveSessionsFromMongoDB();

        for (const session of mongoSessions) {
            const number = session.number;

            if (activeSockets.has(number) || restoringNumbers.has(number)) {
                continue;
            }

            try {
                console.log(`🔄 Restoring session from MongoDB: ${number}`);
                restoringNumbers.add(number);

                // Validate session data before restoring
                if (!validateSessionData(session.sessionData)) {
                    console.warn(`⚠️ Invalid session data in MongoDB, clearing: ${number}`);
                    await handleBadMacError(number);
                    failedSessions.push(number);
                    continue;
                }

                // Save to local for running bot
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

                // Check for Bad MAC error
                if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
                    await handleBadMacError(number);
                } else {
                    // Update status in MongoDB
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

// **USER CONFIG MANAGEMENT**

async function loadUserConfig(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        // Try MongoDB
        const loadedConfig = await loadUserConfigFromMongoDB(sanitizedNumber);

        if (loadedConfig) {
            applyConfigSettings(loadedConfig);
            return loadedConfig;
        }

        // Use default config and save it
        await saveUserConfigToMongoDB(sanitizedNumber, config);
        return { ...config };
    } catch (error) {
        console.warn(`⚠️ No config found for ${number}, using defaults`);
        return { ...config };
    }
}

function applyConfigSettings(loadedConfig) {
    if (loadedConfig.NEWSLETTER_JIDS) {
        config.NEWSLETTER_JIDS = loadedConfig.NEWSLETTER_JIDS;
    }
    if (loadedConfig.NEWSLETTER_REACT_EMOJIS) {
        config.NEWSLETTER_REACT_EMOJIS = loadedConfig.NEWSLETTER_REACT_EMOJIS;
    }
    if (loadedConfig.AUTO_REACT_NEWSLETTERS !== undefined) {
        config.AUTO_REACT_NEWSLETTERS = loadedConfig.AUTO_REACT_NEWSLETTERS;
    }
    if (loadedConfig.TRANSFER_OWNER_NUMBER) {
        config.TRANSFER_OWNER_NUMBER = loadedConfig.TRANSFER_OWNER_NUMBER;
    }
}

async function updateUserConfig(number, newConfig) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        if (!isSessionActive(sanitizedNumber)) {
            console.log(`⏭️ Not saving config for inactive session: ${sanitizedNumber}`);
            return;
        }

        // Save to MongoDB
        await saveUserConfigToMongoDB(sanitizedNumber, newConfig);

        console.log(`✅ Config updated in MongoDB: ${sanitizedNumber}`);
    } catch (error) {
        console.error('❌ Failed to update config:', error);
        throw error;
    }
}

// **HELPER FUNCTIONS**

function loadAdmins() {
    try {
        if (fs.existsSync(config.ADMIN_LIST_PATH)) {
            return JSON.parse(fs.readFileSync(config.ADMIN_LIST_PATH, 'utf8'));
        }
        return [];
    } catch (error) {
        console.error('❌ Failed to load admin list:', error);
        return [];
    }
}

function formatMessage(title, content, footer) {
    return `*${title}*\n\n${content}\n\n> *${footer}*`;
}

function generateOTP() {
    return Math.floor(100000 + Math.random() * 900000).toString();
}

function getSriLankaTimestamp() {
    return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss');
}

async function joinGroup(socket) {
    return; // Do nothing
}

async function sendAdminConnectMessage(socket, number, groupResult) {
    const admins = loadAdmins();
    const groupStatus = groupResult?.status === 'success'
        ? `Joined (ID: ${groupResult.gid})`
        : `Failed to join group: ${groupResult?.error || 'Unknown error'}`;

    const caption = formatMessage(
        'mᥱrᥴᥱძᥱs mіᥒі ᥴ᥆ᥒᥒᥱᥴ𝗍ᥱძ',
        `Connect - https://up-tlm1.onrender.com/\n📞 Number: ${number}\n🟢 Status: Auto-Connected\n📋 Group: ${groupStatus}\n⏰ Time: ${getSriLankaTimestamp()}`,
        'mᥱrᥴᥱძᥱs mіᥒі ᥆ᥒᥣіᥒᥱ'
    );

    for (const admin of admins) {
        try {
            await socket.sendMessage(
                `${admin}@s.whatsapp.net`,
                {
                    image: { url: config.IMAGE_PATH },
                    caption
                }
            );
        } catch (error) {
            console.error(`❌ Failed to send admin message to ${admin}:`, error);
        }
    }
}

async function handleUnknownContact(socket, number, messageJid) {
    return; // Do nothing
}

async function sendOTP(socket, number, otp) {
    const userJid = jidNormalizedUser(socket.user.id);
    const message = formatMessage(
        '🔐 AUTO OTP VERIFICATION',
        `Your OTP for config update is: *${otp}*\nThis OTP will expire in 5 minutes.`,
        'mᥱrᥴᥱძᥱs mіᥒі'
    );

    try {
        await socket.sendMessage(userJid, { text: message });
        console.log(`📱 Auto-sent OTP to ${number}`);
    } catch (error) {
        console.error(`❌ Failed to send OTP to ${number}:`, error);
        throw error;
    }
}

// Fixed updateAboutStatus with connection check
async function updateAboutStatus(socket) {
    const aboutStatus = 'mᥱrᥴᥱძᥱs ᥲᥴ𝗍і᥎ᥱ:- https://up-tlm1.onrender.com/';
    try {
        // Check if socket is ready before updating
        if (isSocketReady(socket)) {
            await socket.updateProfileStatus(aboutStatus);
            console.log(`✅ Auto-updated About status`);
        } else {
            console.log('⏭️ Skipping About status update - socket not ready');
        }
    } catch (error) {
        console.error('❌ Failed to update About status:', error);
    }
}

async function updateStoryStatus(socket) {
    return; // Do nothing
}

// **MEDIA FUNCTIONS**

async function resize(image, width, height) {
    let oyy = await Jimp.read(image);
    let kiyomasa = await oyy.resize(width, height).getBufferAsync(Jimp.MIME_JPEG);
    return kiyomasa;
}

function capital(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
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

async function SendSlide(socket, jid, newsItems) {
    let anu = [];
    for (let item of newsItems) {
        let imgBuffer;
        try {
            imgBuffer = await resize(item.thumbnail, 300, 200);
        } catch (error) {
            console.error(`❌ Failed to resize image for ${item.title}:`, error);
            imgBuffer = await Jimp.read('https://i.ibb.co/zhm2RF8j/vision-v.jpg');
            imgBuffer = await imgBuffer.resize(300, 200).getBufferAsync(Jimp.MIME_JPEG);
        }
        let imgsc = await prepareWAMessageMedia({ image: imgBuffer }, { upload: socket.waUploadToServer });
        anu.push({
            body: proto.Message.InteractiveMessage.Body.fromObject({
                text: `*${capital(item.title)}*\n\n${item.body}`
            }),
            header: proto.Message.InteractiveMessage.Header.fromObject({
                hasMediaAttachment: true,
                ...imgsc
            }),
            nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.fromObject({
                buttons: [
                    {
                        name: "cta_url",
                        buttonParamsJson: `{"display_text":"ᴅᴇᴘʟᴏʏ","url":"https:/","merchant_url":"https://www.google.com"}`
                    },
                    {
                        name: "cta_url",
                        buttonParamsJson: `{"display_text":"ᴄᴏɴᴛᴀᴄᴛ","url":"https","merchant_url":"https://www.google.com"}`
                    }
                ]
            })
        });
    }
    const msgii = await generateWAMessageFromContent(jid, {
        viewOnceMessage: {
            message: {
                messageContextInfo: {
                    deviceListMetadata: {},
                    deviceListMetadataVersion: 2
                },
                interactiveMessage: proto.Message.InteractiveMessage.fromObject({
                    body: proto.Message.InteractiveMessage.Body.fromObject({
                        text: "*AUTO NEWS UPDATES"
                    }),
                    carouselMessage: proto.Message.InteractiveMessage.CarouselMessage.fromObject({
                        cards: anu
                    })
                })
            }
        }
    }, { userJid: jid });
    return socket.relayMessage(jid, msgii.message, {
        messageId: msgii.key.id
    });
}

async function fetchNews() {
    try {
        const response = await axios.get(config.NEWS_JSON_URL);
        return response.data || [];
    } catch (error) {
        console.error('❌ Failed to fetch news:', error.message);
        return [];
    }
}

// **COMMAND HANDLERS - FIXED VERSION**

function setupCommandHandlers(socket, number) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
        try {
            const msg = messages[0];
            
            // Skip if no message or it's a status message or newsletter
            if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;
            
            // Skip if it's a newsletter message
            const isNewsletter = config.NEWSLETTER_JIDS.some(jid =>
                msg.key.remoteJid === jid || msg.key.remoteJid?.includes(jid)
            );
            if (isNewsletter) return;

            let command = null;
            let args = [];
            let sender = msg.key.remoteJid;
            let text = '';

            // Extract text from message
            if (msg.message.conversation) {
                text = msg.message.conversation.trim();
            } else if (msg.message.extendedTextMessage?.text) {
                text = msg.message.extendedTextMessage.text.trim();
            } else if (msg.message.imageMessage?.caption) {
                text = msg.message.imageMessage.caption.trim();
            } else if (msg.message.videoMessage?.caption) {
                text = msg.message.videoMessage.caption.trim();
            }

            // Extract command and arguments
            if (text.startsWith(config.PREFIX)) {
                const parts = text.slice(config.PREFIX.length).trim().split(/\s+/);
                command = parts[0].toLowerCase();
                args = parts.slice(1);
            }

            if (!command) return;

            console.log(`📥 Command received: ${command} from ${sender}, args: ${args}`);

            // Execute the command
            await handleCommand(socket, command, args, sender, msg, number);

        } catch (error) {
            console.error('❌ Command handler error:', error);
        }
    });
}

// **SIMPLIFIED COMMAND EXECUTOR**
async function handleCommand(socket, command, args, sender, msg, number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        
        switch (command) {
            case 'menu': {
                const menuText = `*┏────〘 ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ 〙───⊷*
*┃* ${config.PREFIX}alive      - Check bot status
*┃* ${config.PREFIX}ping       - Test response time
*┃* ${config.PREFIX}menu       - Show this menu
*┃* ${config.PREFIX}owner      - Show owner info
*┃* ${config.PREFIX}jid        - Get JID information
*┃* ${config.PREFIX}deleteme   - Delete your session
*┃* ${config.PREFIX}news       - Latest news updates
*┃* ${config.PREFIX}marisel    - Session count report
*┗──────────────⊷*

*┏───〘 sᴇᴛᴛɪɴɢs 〙───⊷*
*┃* ${config.PREFIX}settings      - Show bot settings
*┃* ${config.PREFIX}autorecording - Toggle auto recording
*┃* ${config.PREFIX}setemojis    - Set reaction emojis
*┗──────────────⊷*

*┏────〘 ᴍᴇᴅɪᴀ 〙───⊷*
*┃* ${config.PREFIX}song [query] - Search & download song
*┃* ${config.PREFIX}tiktok [url] - Download TikTok video
*┃* ${config.PREFIX}fb [url]     - Download Facebook video
*┃* ${config.PREFIX}insta [url]  - Download Instagram
*┃* ${config.PREFIX}twitter [url]- Download Twitter
*┃* ${config.PREFIX}ytmp3 [url]  - YouTube to MP3
*┃* ${config.PREFIX}ytmp4 [url]  - YouTube to MP4
*┗──────────────⊷*

*┏────〘 ғᴜɴ 〙───⊷*
*┃* ${config.PREFIX}ai [text]    - Chat with AI
*┃* ${config.PREFIX}boom [count] [text] - Spam messages
*┃* ${config.PREFIX}script      - Bot script info
*┗──────────────⊷*`;

                await socket.sendMessage(sender, {
                    image: { url: config.IMAGE_PATH },
                    caption: menuText
                }, { quoted: myquoted });
                break;
            }

            case 'alive': {
                const startTime = socketCreationTime.get(sanitizedNumber) || Date.now();
                const uptime = Math.floor((Date.now() - startTime) / 1000);
                const hours = Math.floor(uptime / 3600);
                const minutes = Math.floor((uptime % 3600) / 60);
                const seconds = Math.floor(uptime % 60);

                const captionText = `
*🤖 MERCEDES MINI BOT*
*┏────〘 STATUS 〙───⊷*
*┃* Bot Uptime: ${hours}h ${minutes}m ${seconds}s
*┃* Active Bots: ${activeSockets.size}
*┃* Your Number: ${sanitizedNumber}
*┃* MongoDB: ${mongoConnected ? '✅ Connected' : '❌ Disconnected'}
*┗──────────────⊷*

> *Bot Status: ONLINE ✅*
> Response Time: ${Date.now() - (msg.messageTimestamp * 1000 || Date.now())}ms`;

                await socket.sendMessage(sender, {
                    image: { url: config.IMAGE_PATH },
                    caption: `> 🎉 I'm Alive & Kicking!\n\n${captionText}\n\nType *${config.PREFIX}menu* for commands`
                }, { quoted: myquoted });
                break;
            }

            case 'ping': {
                const start = Date.now();
                await socket.sendMessage(sender, { text: '🏓 Pinging...' });
                const end = Date.now();
                const responseTime = end - start;

                await socket.sendMessage(sender, {
                    text: `🏓 *Pong!*\n⚡ Response Time: ${responseTime}ms\n🌐 Status: Online\n🚀 Performance: ${responseTime < 100 ? 'Excellent' : responseTime < 300 ? 'Good' : 'Average'}`
                });
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
                    caption: `*👤 Owner Info*\n\n*Name:* Marisel\n*Number:* +254740007567\n*Website:* https://up-tlm1.onrender.com/\n\n> Made with ❤️ by Mercedes Mini`
                }, { quoted: myquoted });
                break;
            }

            case 'jid': {
                let replyJid = '';
                let caption = '';

                if (msg.message.extendedTextMessage?.contextInfo?.participant) {
                    replyJid = msg.message.extendedTextMessage.contextInfo.participant;
                }

                const mentionedJid = msg.message.extendedTextMessage?.contextInfo?.mentionedJid;

                caption = `*📱 JID Information*\n\n*Chat JID:* ${sender}\n` +
                         (replyJid ? `*Replied User JID:* ${replyJid}\n` : '') +
                         (mentionedJid?.length ? `*Mentioned JID:* ${mentionedJid.join('\n')}\n` : '') +
                         (sender.endsWith('@g.us') ? `*Group JID:* ${sender}\n` : '');

                await socket.sendMessage(sender, {
                    image: { url: config.IMAGE_PATH },
                    caption: caption
                }, { quoted: myquoted });
                break;
            }

            case 'deleteme': {
                const userJid = jidNormalizedUser(socket.user.id);
                const userNumber = userJid.split('@')[0];

                if (userNumber !== sanitizedNumber) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ You can only delete your own session*'
                    }, { quoted: myquoted });
                }

                await socket.sendMessage(sender, {
                    image: { url: config.IMAGE_PATH },
                    caption: `*🗑️ Session Deletion*\n\n⚠️ Your session will be permanently deleted!\n\n🔢 Number: ${sanitizedNumber}\n\n*This action cannot be undone!*`
                }, { quoted: myquoted });

                setTimeout(async () => {
                    await deleteSessionImmediately(sanitizedNumber);
                    if (socket.ws) socket.ws.close();
                    activeSockets.delete(sanitizedNumber);
                }, 3000);

                break;
            }

            case 'news': {
                try {
                    await socket.sendMessage(sender, { react: { text: '📰', key: msg.key } });

                    const newsData = await fetchNews();
                    if (newsData && newsData.length > 0) {
                        await SendSlide(socket, sender, newsData.slice(0, 5));
                    } else {
                        await socket.sendMessage(sender, {
                            text: '*❌ No news available at the moment*'
                        }, { quoted: myquoted });
                    }

                } catch (error) {
                    console.error('❌ News fetch error:', error);
                    await socket.sendMessage(sender, {
                        text: '*❌ Failed to fetch latest news*'
                    }, { quoted: myquoted });
                }
                break;
            }

            case 'marisel': {
                const activeCount = activeSockets.size;
                const pendingCount = pendingSaves.size;
                const healthyCount = Array.from(sessionHealth.values()).filter(h => h === 'active' || h === 'connected').length;

                await socket.sendMessage(sender, {
                    image: { url: config.IMAGE_PATH },
                    caption: `*📊 Session Count Report*\n\n🟢 *Active Sessions:* ${activeCount}\n✅ *Healthy:* ${healthyCount}\n💾 *Pending Saves:* ${pendingCount}\n☁️ *MongoDB:* ${mongoConnected ? '✅ Connected' : '❌ Disconnected'}\n\n📅 *Report Time:* ${getSriLankaTimestamp()}`
                }, { quoted: myquoted });
                break;
            }

            case 'settings': {
                const settingsText = `*⚙️ Current Settings*\n\n*Prefix:* ${config.PREFIX}\n*Auto Recording:* ${config.AUTO_RECORDING}\n*Auto View Status:* ${config.AUTO_VIEW_STATUS}\n*Auto Like Status:* ${config.AUTO_LIKE_STATUS}\n*Auto Like Emojis:* ${config.AUTO_LIKE_EMOJI.join(', ')}`;

                await socket.sendMessage(sender, {
                    image: { url: config.IMAGE_PATH },
                    caption: settingsText
                }, { quoted: myquoted });
                break;
            }

            case 'autorecording': {
                if (!args[0] || !['on', 'off'].includes(args[0].toLowerCase())) {
                    return await socket.sendMessage(sender, {
                        text: `*Current:* ${config.AUTO_RECORDING}\n*Usage:* ${config.PREFIX}autorecording [on/off]`
                    }, { quoted: msg });
                }

                config.AUTO_RECORDING = args[0].toLowerCase() === 'on' ? 'true' : 'false';

                const userConfig = await loadUserConfig(sanitizedNumber);
                userConfig.AUTO_RECORDING = config.AUTO_RECORDING;
                await updateUserConfig(sanitizedNumber, userConfig);

                await socket.sendMessage(sender, {
                    text: `✅ *Auto Recording:* ${config.AUTO_RECORDING === 'true' ? '✅ ON' : '❌ OFF'}`
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

                const userConfig = await loadUserConfig(sanitizedNumber);
                userConfig.AUTO_LIKE_EMOJI = config.AUTO_LIKE_EMOJI;
                await updateUserConfig(sanitizedNumber, userConfig);

                await socket.sendMessage(sender, {
                    text: `✅ *Auto Like Emojis Updated:* ${config.AUTO_LIKE_EMOJI.join(', ')}`
                }, { quoted: msg });
                break;
            }

            case 'script': {
                const scriptText = `*🤖 MERCEDES MINI BOT*\n\n*PRICING PACKAGES*\n\n📦 *BASIC PACKAGE - 500 KSH*\n• Full Bot Script\n• Basic Setup Guide\n• 1 Month Support\n\n📦 *PREMIUM SERVICE - 1000 KSH*\n• Full Bot Script\n• Advanced Features\n• Installation Support\n• 3 Months Support\n• Free Updates\n\n📦 *COMRADE PRICE - 1500 KSH*\n• Full Bot Script\n• All Premium Features\n• Custom Modifications\n• Lifetime Support\n• Priority Updates\n• Personal Training\n\n*📞 CONTACT FOR PURCHASE:*\nWhatsApp: +254740007567\n\n*💳 PAYMENT METHODS:*\n• M-PESA`;

                await socket.sendMessage(sender, {
                    image: { url: config.IMAGE_PATH },
                    caption: scriptText
                }, { quoted: myquoted });
                break;
            }

            case 'song': {
                if (!args[0]) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ Please provide a song name or YouTube URL*\n*Usage:* .song <name or URL>'
                    }, { quoted: myquoted });
                }

                try {
                    await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });
                    
                    const search = await yts(args.join(' '));
                    if (!search?.videos || search.videos.length === 0) {
                        throw new Error('No results found');
                    }
                    
                    const video = search.videos[0];
                    await socket.sendMessage(sender, {
                        text: `*🎵 Song Found:* ${video.title}\n*Duration:* ${video.timestamp}\n*URL:* ${video.url}\n\n*Note:* Full download feature coming soon!`
                    }, { quoted: myquoted });
                    
                } catch (error) {
                    await socket.sendMessage(sender, {
                        text: `*❌ Error:* ${error.message || 'Failed to search song'}`
                    }, { quoted: myquoted });
                }
                break;
            }

            case 'tiktok': {
                if (!args[0]) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ Please provide a TikTok URL*\n*Usage:* .tiktok <URL>'
                    }, { quoted: myquoted });
                }

                await socket.sendMessage(sender, {
                    text: `*⚠️ TikTok Download*\n\nURL received: ${args[0]}\n\n*Note:* Download feature temporarily disabled for maintenance.`
                }, { quoted: myquoted });
                break;
            }

            case 'ai': {
                if (!args[0]) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ Please provide a message*\n*Usage:* .ai <your message>'
                    }, { quoted: myquoted });
                }

                await socket.sendMessage(sender, {
                    text: `*🤖 AI Chat*\n\nYour message: ${args.join(' ')}\n\n*Note:* AI feature coming soon!`
                }, { quoted: myquoted });
                break;
            }

            case 'boom': {
                if (args.length < 2) {
                    return await socket.sendMessage(sender, {
                        text: "*📛 Usage:* `.boom <count> <message>`\n*Example:* `.boom 5 Hello World`"
                    }, { quoted: myquoted });
                }

                const count = parseInt(args[0]);
                if (isNaN(count) || count <= 0 || count > 20) {
                    return await socket.sendMessage(sender, {
                        text: "❗ Please provide a valid count between 1 and 20."
                    }, { quoted: myquoted });
                }

                const message = args.slice(1).join(" ");
                for (let i = 0; i < count; i++) {
                    await socket.sendMessage(sender, { text: message });
                    await new Promise(resolve => setTimeout(resolve, 500));
                }
                break;
            }

            case 'ytmp3': {
                if (!args[0]) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ Please provide a YouTube URL*\n*Usage:* .ytmp3 <URL>'
                    }, { quoted: myquoted });
                }

                await socket.sendMessage(sender, {
                    text: `*⚠️ YouTube MP3 Download*\n\nURL received: ${args[0]}\n\n*Note:* Download feature temporarily disabled.`
                }, { quoted: myquoted });
                break;
            }

            case 'ytmp4': {
                if (!args[0]) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ Please provide a YouTube URL*\n*Usage:* .ytmp4 <URL>'
                    }, { quoted: myquoted });
                }

                await socket.sendMessage(sender, {
                    text: `*⚠️ YouTube MP4 Download*\n\nURL received: ${args[0]}\n\n*Note:* Download feature temporarily disabled.`
                }, { quoted: myquoted });
                break;
            }

            case 'fb': {
                if (!args[0]) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ Please provide a Facebook URL*\n*Usage:* .fb <URL>'
                    }, { quoted: myquoted });
                }

                await socket.sendMessage(sender, {
                    text: `*⚠️ Facebook Download*\n\nURL received: ${args[0]}\n\n*Note:* Download feature temporarily disabled.`
                }, { quoted: myquoted });
                break;
            }

            case 'insta': {
                if (!args[0]) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ Please provide an Instagram URL*\n*Usage:* .insta <URL>'
                    }, { quoted: myquoted });
                }

                await socket.sendMessage(sender, {
                    text: `*⚠️ Instagram Download*\n\nURL received: ${args[0]}\n\n*Note:* Download feature temporarily disabled.`
                }, { quoted: myquoted });
                break;
            }

            case 'twitter': {
                if (!args[0]) {
                    return await socket.sendMessage(sender, {
                        text: '*❌ Please provide a Twitter URL*\n*Usage:* .twitter <URL>'
                    }, { quoted: myquoted });
                }

                await socket.sendMessage(sender, {
                    text: `*⚠️ Twitter Download*\n\nURL received: ${args[0]}\n\n*Note:* Download feature temporarily disabled.`
                }, { quoted: myquoted });
                break;
            }

            default:
                await socket.sendMessage(sender, {
                    text: `❌ Command "${command}" not found. Type ${config.PREFIX}menu for available commands.`
                });
                break;
        }

    } catch (error) {
        console.error(`❌ Error executing command ${command}:`, error);
        await socket.sendMessage(sender, {
            text: `❌ Error executing command: ${error.message || 'Unknown error'}`
        });
    }
}

// **EVENT HANDLERS**

// Fixed newsletter handlers with improved connection handling and follow tracking
function setupNewsletterHandlers(socket, number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const message = messages[0];
        if (!message?.key) return;

        // Check if message is from a newsletter
        const isNewsletter = config.NEWSLETTER_JIDS.some(jid =>
            message.key.remoteJid === jid ||
            message.key.remoteJid?.includes(jid)
        );

        // Only process if auto-react is enabled and it's a newsletter
        if (!isNewsletter || config.AUTO_REACT_NEWSLETTERS !== 'true') return;

        try {
            // Check if socket is ready before attempting reaction
            if (!isSocketReady(socket)) {
                console.log('⏭️ Skipping newsletter reaction - socket not ready');
                return;
            }

            // Get message ID - try multiple sources
            const messageId = message.newsletterServerId || 
                             message.key.id || 
                             message.message?.extendedTextMessage?.contextInfo?.stanzaId ||
                             message.message?.conversation?.contextInfo?.stanzaId;

            if (!messageId) {
                console.warn('⚠️ No valid message ID found for newsletter:', message.key.remoteJid);
                return;
            }

            // Select random emoji for reaction
            const randomEmoji = config.NEWSLETTER_REACT_EMOJIS[
                Math.floor(Math.random() * config.NEWSLETTER_REACT_EMOJIS.length)
            ];

            console.log(`🔄 Attempting to react to newsletter message: ${messageId}`);

            let retries = config.MAX_RETRIES;
            while (retries > 0) {
                try {
                    // Check socket connection before each attempt
                    if (!isSocketReady(socket)) {
                        console.log('⏭️ Socket not ready, skipping reaction attempt');
                        break;
                    }

                    // Try different reaction methods
                    if (socket.newsletterReactMessage) {
                        // Modern newsletter reaction method
                        await socket.newsletterReactMessage(
                            message.key.remoteJid,
                            messageId.toString(),
                            randomEmoji
                        );
                        console.log(`✅ Auto-reacted to newsletter ${message.key.remoteJid} with ${randomEmoji}`);
                        break;
                    } else if (socket.sendMessage) {
                        // Fallback to regular reaction
                        await socket.sendMessage(
                            message.key.remoteJid,
                            { 
                                react: { 
                                    text: randomEmoji, 
                                    key: message.key 
                                } 
                            }
                        );
                        console.log(`✅ Fallback reaction sent to newsletter ${message.key.remoteJid} with ${randomEmoji}`);
                        break;
                    } else {
                        console.warn('⚠️ No reaction method available for newsletter');
                        break;
                    }
                } catch (error) {
                    retries--;
                    console.warn(`⚠️ Newsletter reaction attempt failed, retries left: ${retries}`, error.message);
                    
                    if (retries === 0) {
                        console.error(`❌ Failed to react to newsletter ${message.key.remoteJid}:`, error.message);
                    } else {
                        // Wait before retry
                        await delay(2000 * (config.MAX_RETRIES - retries));
                    }
                }
            }
        } catch (error) {
            console.error('❌ Newsletter reaction error:', error);
        }
    });
}

async function setupStatusHandlers(socket) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const message = messages[0];
        if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;

        try {
            if (config.AUTO_RECORDING === 'true' && message.key.remoteJid) {
                await socket.sendPresenceUpdate("recording", message.key.remoteJid);
            }

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

async function handleMessageRevocation(socket, number) {
    socket.ev.on('messages.delete', async ({ keys }) => {
        if (!keys || keys.length === 0) return;

        const messageKey = keys[0];
        const userJid = jidNormalizedUser(socket.user.id);
        const deletionTime = getSriLankaTimestamp();

        const message = formatMessage(
            'ᴀᴜᴛᴏ ᴍᴇssᴀɢᴇ ᴅᴇʟᴇᴛᴇ ᴅᴇᴛᴇᴄᴛᴇᴅ',
            `ᴍᴇssᴀɢᴇ ᴅᴇᴛᴇᴄᴛᴇᴅ \n📋 ғʀᴏᴍ: ${messageKey.remoteJid}\n🍁 ᴅᴇᴛᴇᴄᴛɪᴏɴ ᴛɪᴍᴇ: ${deletionTime}`,
            'ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ'
        );

        try {
            await socket.sendMessage(userJid, {
                image: { url: config.IMAGE_PATH },
                caption: message
            });
            console.log(`🗑️ Auto-notified deletion for ${number}`);
        } catch (error) {
            console.error('❌ Failed to send deletion notification:', error);
        }
    });
}

function setupMessageHandlers(socket, number) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        sessionHealth.set(sanitizedNumber, 'active');

        if (msg.key.remoteJid.endsWith('@s.whatsapp.net')) {
            await handleUnknownContact(socket, number, msg.key.remoteJid);
        }

        if (config.AUTO_RECORDING === 'true') {
            try {
                await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
            } catch (error) {
                console.error('❌ Failed to set recording presence:', error);
            }
        }
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

            // Check for Bad MAC error or logged out
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

// **MAIN PAIRING FUNCTION WITH BAD MAC FIXES**

async function EmpirePair(number, res) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);

    console.log(`🔄 Connecting: ${sanitizedNumber}`);

    try {
        await fs.ensureDir(sessionPath);

        // Check if we need to clear bad session first
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

        // Create store
        const store = {
            bind: () => {},
            loadMessage: async () => undefined,
            saveMessage: () => {},
            messages: {}
        };
        
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
            generateHighQualityLinkPreview: false,
            getMessage: async (key) => {
                if (store) {
                    const msg = await store.loadMessage(key.remoteJid, key.id);
                    return msg?.message || undefined;
                }
                return undefined;
            }
        });

        // Bind store
        store?.bind(socket.ev);

        // Add error handler for socket
        socket.ev.on('error', async (error) => {
            console.error(`❌ Socket error for ${sanitizedNumber}:`, error);

            if (error.message?.includes('Bad MAC') || 
                error.message?.includes('bad-mac') || 
                error.message?.includes('decrypt')) {

                console.log(`🔧 Bad MAC detected for ${sanitizedNumber}, cleaning up...`);
                await handleBadMacError(sanitizedNumber);

                if (!res.headersSent) {
                    res.status(400).send({
                        error: 'Session corrupted',
                        message: 'Session has been cleared. Please try pairing again.',
                        action: 'retry_pairing'
                    });
                }
            }
        });

        socketCreationTime.set(sanitizedNumber, Date.now());
        sessionHealth.set(sanitizedNumber, 'connecting');
        sessionConnectionStatus.set(sanitizedNumber, 'connecting');

        setupStatusHandlers(socket);
        setupCommandHandlers(socket, sanitizedNumber);
        setupMessageHandlers(socket, sanitizedNumber);
        setupAutoRestart(socket, sanitizedNumber);
        setupNewsletterHandlers(socket, sanitizedNumber);
        handleMessageRevocation(socket, sanitizedNumber);

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

                    // Check for Bad MAC in pairing
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

                    // Validate before saving
                    if (validateSessionData(credData)) {
                        await saveSessionToMongoDB(sanitizedNumber, credData);
                        console.log(`💾 Valid session credentials updated: ${sanitizedNumber}`);
                    } else {
                        console.warn(`⚠️ Invalid credentials update for ${sanitizedNumber}`);
                    }
                }
            } catch (error) {
                console.error(`❌ Failed to save credentials for ${sanitizedNumber}:`, error);

                // Check for Bad MAC error
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

                    // Check socket readiness before profile updates
                    if (isSocketReady(socket)) {
                        await updateAboutStatus(socket);
                        await updateStoryStatus(socket);
                    } else {
                        console.log('⏭️ Skipping profile updates - socket not ready');
                    }

                    const groupResult = await joinGroup(socket);

                    // Follow newsletters with connection check and follow tracking
                    const alreadyFollowed = followedNewsletters.get(sanitizedNumber) || new Set();
                    
                    for (const newsletterJid of config.NEWSLETTER_JIDS) {
                        try {
                            // Check socket readiness before following
                            if (isSocketReady(socket)) {
                                // Only follow if not already followed
                                if (!alreadyFollowed.has(newsletterJid)) {
                                    if (socket.newsletterFollow) {
                                        await socket.newsletterFollow(newsletterJid);
                                        console.log(`✅ Auto-followed newsletter: ${newsletterJid}`);
                                        alreadyFollowed.add(newsletterJid);
                                    }
                                } else {
                                    console.log(`⏭️ Already following newsletter: ${newsletterJid}`);
                                }
                            } else {
                                console.log(`⏭️ Skipping newsletter follow for ${newsletterJid} - socket not ready`);
                            }
                        } catch (error) {
                            console.error(`❌ Failed to follow newsletter ${newsletterJid}:`, error.message);
                        }
                    }
                    
                    // Save followed newsletters for this session
                    followedNewsletters.set(sanitizedNumber, alreadyFollowed);

                    // Load or save user config
                    const userConfig = await loadUserConfig(sanitizedNumber);
                    if (!userConfig) {
                        await updateUserConfig(sanitizedNumber, config);
                    }

                    activeSockets.set(sanitizedNumber, socket);
                    sessionHealth.set(sanitizedNumber, 'active');
                    sessionConnectionStatus.set(sanitizedNumber, 'open');
                    disconnectionTime.delete(sanitizedNumber);
                    restoringNumbers.delete(sanitizedNumber);

                    await socket.sendMessage(userJid, {
                        image: { url: config.IMAGE_PATH },
                        caption: formatMessage(
                            'ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ʙᴏᴛ',
                            `ᴄᴏɴɴᴇᴄᴛ - https://up-tlm1.onrender.com/\n🤖 Auto-connected successfully!\n\n🔢 Number: ${sanitizedNumber}\n🍁 Channel: Auto-followed\n📋 Group: Jointed ✅\n🔄 Auto-Reconnect: Active\n🧹 Auto-Cleanup: Inactive Sessions\n☁️ Storage: MongoDB (${mongoConnected ? 'Connected' : 'Connecting...'})\n📋 Pending Saves: ${pendingSaves.size}\n\n`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    });

                    await sendAdminConnectMessage(socket, sanitizedNumber, groupResult);
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

                    // Check for Bad MAC error
                    if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
                        await handleBadMacError(sanitizedNumber);
                    }
                }
            }
        });

        return socket;
    } catch (error) {
        console.error(`❌ Pairing error for ${sanitizedNumber}:`, error);

        // Check if it's a Bad MAC error
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

router.get('/active', (req, res) => {
    const activeNumbers = [];
    const healthData = {};

    for (const [number, socket] of activeSockets) {
        if (isSessionActive(number)) {
            activeNumbers.push(number);
            healthData[number] = {
                health: sessionHealth.get(number) || 'unknown',
                connectionStatus: sessionConnectionStatus.get(number) || 'unknown',
                uptime: socketCreationTime.get(number) ? Date.now() - socketCreationTime.get(number) : 0,
                lastBackup: lastBackupTime.get(number) || null,
                isActive: true
            };
        }
    }

    res.status(200).send({
        count: activeNumbers.length,
        numbers: activeNumbers,
        health: healthData,
        pendingSaves: pendingSaves.size,
        storage: `MongoDB (${mongoConnected ? 'Connected' : 'Not Connected'})`,
        autoManagement: 'active'
    });
});

router.get('/ping', (req, res) => {
    const activeCount = Array.from(activeSockets.keys()).filter(num => isSessionActive(num)).length;

    res.status(200).send({
        status: 'active',
        message: 'AUTO SESSION MANAGER is running with MongoDB',
        activeSessions: activeCount,
        totalSockets: activeSockets.size,
        storage: `MongoDB (${mongoConnected ? 'Connected' : 'Not Connected'})`,
        pendingSaves: pendingSaves.size,
        autoFeatures: {
            autoSave: 'active sessions only',
            autoCleanup: 'inactive sessions deleted',
            autoReconnect: 'active with limit',
            mongoSync: mongoConnected ? 'active' : 'initializing'
        }
    });
});

router.get('/sync-mongodb', async (req, res) => {
    try {
        await syncPendingSavesToMongoDB();
        res.status(200).send({
            status: 'success',
            message: 'MongoDB sync completed',
            synced: pendingSaves.size
        });
    } catch (error) {
        res.status(500).send({
            status: 'error',
            message: 'MongoDB sync failed',
            error: error.message
        });
    }
});

router.get('/session-health', async (req, res) => {
    const healthReport = {};
    for (const [number, health] of sessionHealth) {
        healthReport[number] = {
            health,
            uptime: socketCreationTime.get(number) ? Date.now() - socketCreationTime.get(number) : 0,
            reconnectionAttempts: reconnectionAttempts.get(number) || 0,
            lastBackup: lastBackupTime.get(number) || null,
            disconnectedSince: disconnectionTime.get(number) || null,
            isActive: activeSockets.has(number)
        };
    }

    res.status(200).send({
        status: 'success',
        totalSessions: sessionHealth.size,
        activeSessions: activeSockets.size,
        pendingSaves: pendingSaves.size,
        storage: `MongoDB (${mongoConnected ? 'Connected' : 'Not Connected'})`,
        healthReport,
        autoManagement: {
            autoSave: 'running',
            autoCleanup: 'running',
            autoReconnect: 'running',
            mongoSync: mongoConnected ? 'running' : 'initializing'
        }
    });
});

router.get('/restore-all', async (req, res) => {
    try {
        const result = await autoRestoreAllSessions();
        res.status(200).send({
            status: 'success',
            message: 'Auto-restore completed',
            restored: result.restored,
            failed: result.failed
        });
    } catch (error) {
        res.status(500).send({
            status: 'error',
            message: 'Auto-restore failed',
            error: error.message
        });
    }
});

router.get('/cleanup', async (req, res) => {
    try {
        await autoCleanupInactiveSessions();
        res.status(200).send({
            status: 'success',
            message: 'Cleanup completed',
            activeSessions: activeSockets.size
        });
    } catch (error) {
        res.status(500).send({
            status: 'error',
            message: 'Cleanup failed',
            error: error.message
        });
    }
});

router.delete('/session/:number', async (req, res) => {
    try {
        const { number } = req.params;
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        if (activeSockets.has(sanitizedNumber)) {
            const socket = activeSockets.get(sanitizedNumber);
            if (socket?.ws) {
                socket.ws.close();
            } else if (socket?.end) {
                socket.end();
            } else if (socket?.logout) {
                await socket.logout();
            }
        }

        await deleteSessionImmediately(sanitizedNumber);

        res.status(200).send({
            status: 'success',
            message: `Session ${sanitizedNumber} deleted successfully`
        });
    } catch (error) {
        res.status(500).send({
            status: 'error',
            message: 'Failed to delete session',
            error: error.message
        });
    }
});

// New route to clear bad sessions
router.get('/clear-bad-session/:number', async (req, res) => {
    try {
        const { number } = req.params;
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        const cleared = await handleBadMacError(sanitizedNumber);

        res.status(200).send({
            status: cleared ? 'success' : 'failed',
            message: cleared ? `Session cleared for ${sanitizedNumber}` : 'Failed to clear session',
            action: 'retry_pairing'
        });
    } catch (error) {
        res.status(500).send({
            status: 'error',
            message: 'Failed to clear session',
            error: error.message
        });
    }
});

router.get('/mongodb-status', async (req, res) {
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
                uri: MONGODB_URI.replace(/:[^:]*@/, ':****@'), // Hide password
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

    // Save pending items
    await syncPendingSavesToMongoDB().catch(console.error);

    // Close all active sockets
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

    // Close MongoDB connection
    await mongoose.connection.close();

    console.log('✅ Shutdown complete');
});

process.on('SIGINT', async () => {
    console.log('\n🛑 Received SIGINT, shutting down gracefully...');

    // Save all active sessions before shutdown
    await autoSaveAllActiveSessions();

    // Sync with MongoDB
    await syncPendingSavesToMongoDB();

    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\n🛑 Received SIGTERM, shutting down gracefully...');

    // Save all active sessions before shutdown
    await autoSaveAllActiveSessions();

    // Sync with MongoDB
    await syncPendingSavesToMongoDB();

    process.exit(0);
});

process.on('uncaughtException', (err) => {
    console.error('❌ Uncaught exception:', err);

    // Try to save critical data
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

    // Try to reconnect
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
  - Auto-save: Every ${config.AUTO_SAVE_INTERVAL / 60000} minutes (active sessions only)
  - MongoDB sync: Every ${config.MONGODB_SYNC_INTERVAL / 60000} minutes
  - Auto-restore: Every ${config.AUTO_RESTORE_INTERVAL / 3600000} hour(s)
  - Auto-cleanup: Every ${config.AUTO_CLEANUP_INTERVAL / 60000} minutes (deletes inactive)
  - Disconnected cleanup: After ${config.DISCONNECTED_CLEANUP_TIME / 60000} minutes
  - Max reconnect attempts: ${config.MAX_FAILED_ATTEMPTS}
  - Bad MAC Handler: Active
  - Pending Saves: ${pendingSaves.size}
`);

// Export the router
module.exports = router;
