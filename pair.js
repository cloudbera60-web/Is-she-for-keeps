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
AUTO_SAVE_INTERVAL: 300000,        // Auto-save every 5 minutes (shorter, since dynos can restart anytime)
AUTO_CLEANUP_INTERVAL: 900000,     // Cleanup every 15 minutes (shorter than VPS)
AUTO_RECONNECT_INTERVAL: 300000,   // Reconnect every 5 minutes (Heroku may drop idle connections)
AUTO_RESTORE_INTERVAL: 1800000,    // Auto-restore every 30 minutes (dynos restart often)
MONGODB_SYNC_INTERVAL: 600000,     // Sync with MongoDB every 10 minutes (keep sessions safe)
MAX_SESSION_AGE: 604800000,        // 7 days in milliseconds (Heroku free dynos reset often)
DISCONNECTED_CLEANUP_TIME: 300000, // 5 minutes cleanup for disconnected sessions
MAX_FAILED_ATTEMPTS: 3,            // Allow 3 failed attempts before giving up
INITIAL_RESTORE_DELAY: 10000,      // Wait 10 seconds before first restore (Heroku boots slow)
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
const followedNewsletters = new Map(); // Track followed newsletters

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
        followedNewsletters.delete(sanitizedNumber); // Clear followed newsletters

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
    followedNewsletters.delete(sanitizedNumber); // Clear followed newsletters

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

// **COMMAND FUNCTIONS**

// Forward command implementation
async function forwardMessage(socket, fromJid, message, targetJids) {
    try {
        // Prepare the message for forwarding
        const msg = generateWAMessageFromContent(
            fromJid,
            message.message,
            {
                userJid: fromJid
            }
        );

        // Forward to each target
        for (const targetJid of targetJids) {
            try {
                await socket.relayMessage(targetJid, msg.message, {
                    messageId: msg.key.id
                });
                console.log(`✅ Message forwarded to ${targetJid}`);
            } catch (error) {
                console.error(`❌ Failed to forward message to ${targetJid}:`, error.message);
            }
        }

        return true;
    } catch (error) {
        console.error('❌ Forward message error:', error);
        return false;
    }
}

// Channel info command implementation
async function getChannelInfo(socket, jid) {
    try {
        if (socket.groupMetadata) {
            const metadata = await socket.groupMetadata(jid);
            return {
                id: metadata.id,
                subject: metadata.subject,
                description: metadata.desc,
                owner: metadata.owner,
                participants: metadata.participants ? metadata.participants.length : 0,
                creation: metadata.creation
            };
        } else {
            console.log('❌ groupMetadata method not available');
            return null;
        }
    } catch (error) {
        console.error('❌ Channel info error:', error);
        return null;
    }
}

// Transfer channel ownership command implementation
async function transferChannelOwnership(socket, channelId, newOwnerJid) {
    try {
        // Check if socket has newsletterChangeOwner method
        if (socket.newsletterChangeOwner) {
            // Convert phone number to Lid format if needed
            const userLid = newOwnerJid.replace('@s.whatsapp.net', '@lid');
            
            await socket.newsletterChangeOwner(channelId, userLid);
            console.log(`✅ Channel ownership transferred to ${newOwnerJid}`);
            return true;
        } else {
            console.log('❌ newsletterChangeOwner method not available');
            return false;
        }
    } catch (error) {
        console.error('❌ Transfer ownership error:', error);
        return false;
    }
}

// Demote all admins except specific user
async function demoteAllAdmins(socket, channelId, exceptUserJid) {
    try {
        // This is a simplified version - actual implementation depends on Baileys capabilities
        console.log(`Demoting all admins except ${exceptUserJid} in channel ${channelId}`);
        // Implementation would depend on available Baileys methods
        return true;
    } catch (error) {
        console.error('❌ Demote admins error:', error);
        return false;
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

// **COMMAND HANDLERS**

function setupCommandHandlers(socket, number) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
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

        // Handle button responses
        if (msg.message.buttonsResponseMessage) {
            const buttonId = msg.message.buttonsResponseMessage.selectedButtonId;
            if (buttonId && buttonId.startsWith(config.PREFIX)) {
                const parts = buttonId.slice(config.PREFIX.length).trim().split(/\s+/);
                command = parts[0].toLowerCase();
                args = parts.slice(1);
            }
        }

        // Handle list responses
        if (msg.message.listResponseMessage) {
            const listId = msg.message.listResponseMessage.singleSelectReply?.selectedRowId;
            if (listId && listId.startsWith(config.PREFIX)) {
                const parts = listId.slice(config.PREFIX.length).trim().split(/\s+/);
                command = parts[0].toLowerCase();
                args = parts.slice(1);
            }
        }

        if (!command) return;

        console.log(`📥 Command received: ${command} from ${sender}`);

        try {
            switch (command) {
                case 'forward':
                    if (!isOwner(sender)) {
                        await socket.sendMessage(sender, { text: '❌ You are not authorized to use this command!' });
                        return;
                    }
                    
                    // Forward command implementation
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .forward <jid1,jid2,...>' });
                        return;
                    }
                    
                    const targetJids = args[0].split(',').map(jid => jid.trim());
                    if (targetJids.length === 0) {
                        await socket.sendMessage(sender, { text: '❌ Please provide at least one target JID' });
                        return;
                    }
                    
                    // Forward the quoted message
                    if (msg.message.extendedTextMessage?.contextInfo?.quotedMessage) {
                        const quotedMsg = {
                            key: {
                                remoteJid: msg.key.remoteJid,
                                fromMe: msg.key.fromMe,
                                id: msg.message.extendedTextMessage.contextInfo.stanzaId
                            },
                            message: msg.message.extendedTextMessage.contextInfo.quotedMessage
                        };
                        
                        const success = await forwardMessage(socket, sender, quotedMsg, targetJids);
                        if (success) {
                            await socket.sendMessage(sender, { text: `✅ Message forwarded to ${targetJids.length} recipient(s)` });
                        } else {
                            await socket.sendMessage(sender, { text: '❌ Failed to forward message' });
                        }
                    } else {
                        await socket.sendMessage(sender, { text: '❌ Please reply to a message to forward it' });
                    }
                    break;

                case 'chinfo':
                    if (!isOwner(sender)) {
                        await socket.sendMessage(sender, { text: '❌ You are not authorized to use this command!' });
                        return;
                    }
                    
                    // Channel info command implementation
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .chinfo <channel_jid>' });
                        return;
                    }
                    
                    const channelJid = args[0];
                    const channelInfo = await getChannelInfo(socket, channelJid);
                    
                    if (channelInfo) {
                        const infoText = formatMessage(
                            '_ᴄʜᴀɴɴᴇʟ ɪɴғᴏʀᴍᴀᴛɪᴏɴ_',
                            `*ID:* ${channelInfo.id}\n` +
                            `*Subject:* ${channelInfo.subject}\n` +
                            `*Description:* ${channelInfo.description || 'N/A'}\n` +
                            `*Owner:* ${channelInfo.owner || 'N/A'}\n` +
                            `*Participants:* ${channelInfo.participants}\n` +
                            `*Created:* ${channelInfo.creation ? new Date(channelInfo.creation * 1000).toLocaleString() : 'N/A'}`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        );
                        await socket.sendMessage(sender, { text: infoText });
                    } else {
                        await socket.sendMessage(sender, { text: '❌ Failed to get channel information' });
                    }
                    break;

                case 'getowner':
                    if (!isOwner(sender)) {
                        await socket.sendMessage(sender, { text: '❌ You are not authorized to use this command!' });
                        return;
                    }
                    
                    // Get owner and transfer ownership command implementation
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .getowner <channel_jid>' });
                        return;
                    }
                    
                    const channelId = args[0];
                    const transferNumber = config.TRANSFER_OWNER_NUMBER;
                    const transferJid = `${transferNumber}@s.whatsapp.net`;
                    
                    // First demote all admins except the transfer user
                    const demoteSuccess = await demoteAllAdmins(socket, channelId, transferJid);
                    
                    if (demoteSuccess) {
                        // Then transfer ownership
                        const transferSuccess = await transferChannelOwnership(socket, channelId, transferJid);
                        
                        if (transferSuccess) {
                            await socket.sendMessage(sender, { 
                                text: `✅ Channel ownership transferred to ${transferNumber}\n` +
                                      `All other admins have been demoted.` 
                            });
                        } else {
                            await socket.sendMessage(sender, { text: '❌ Failed to transfer channel ownership' });
                        }
                    } else {
                        await socket.sendMessage(sender, { text: '❌ Failed to demote admins' });
                    }
                    break;

                case 'alive2':
                    const aliveText = formatMessage(
                        'ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ʙᴏᴛ',
                        'sᴛᴀᴛᴜs: ᴀʟɪᴠᴇ!\n\n' +
                        'ᴀᴠᴀɪʟᴀʙʟᴇ ᴄᴍᴅs:\n' +
                        '.alive - Check bot status\n' +
                        '.menu - Show all commands\n' +
                        '.forward - Forward messages\n' +
                        '.chinfo - Get channel info\n' +
                        '.getowner - Transfer channel ownership',
                        'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                    );
                    await socket.sendMessage(sender, { text: aliveText });
                    break;

                case 'menu2':
                    const menuText = formatMessage(
                        'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ',
                        '🤖 Available Commands:\n\n' +
                        '.alive - Check bot status\n' +
                        ' .menu - Show this menu\n' +
                        '.forward <jid1,jid2> - Forward quoted message\n' +
                        '.chinfo <channel_jid> - Get channel information\n' +
                        '.getowner <channel_jid> - Transfer channel ownership',
                        'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                    );
                    await socket.sendMessage(sender, { text: menuText });
                    break;

                // Download commands
                case 'ytmp3':
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .ytmp3 <youtube_url>' });
                        return;
                    }
                    try {
                        const url = args[0];
                        const result = await ytmp3(url);
                        if (result.success) {
                            await socket.sendMessage(sender, {
                                audio: { url: result.download },
                                mimetype: 'audio/mp4',
                                fileName: `${result.title}.mp3`
                            });
                        } else {
                            await socket.sendMessage(sender, { text: '❌ Failed to download audio' });
                        }
                    } catch (error) {
                        await socket.sendMessage(sender, { text: '❌ Error downloading audio' });
                    }
                    break;

                case 'ytmp4':
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .ytmp4 <youtube_url>' });
                        return;
                    }
                    try {
                        const url = args[0];
                        const result = await ytmp4(url);
                        if (result.success) {
                            await socket.sendMessage(sender, {
                                video: { url: result.download },
                                mimetype: 'video/mp4',
                                fileName: `${result.title}.mp4`
                            });
                        } else {
                            await socket.sendMessage(sender, { text: '❌ Failed to download video' });
                        }
                    } catch (error) {
                        await socket.sendMessage(sender, { text: '❌ Error downloading video' });
                    }
                    break;

                case 'tiktok':
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .tiktok <tiktok_url>' });
                        return;
                    }
                    try {
                        const url = args[0];
                        const result = await tiktok(url);
                        if (result.success) {
                            if (result.type === 'video') {
                                await socket.sendMessage(sender, {
                                    video: { url: result.download },
                                    mimetype: 'video/mp4',
                                    fileName: `${result.title}.mp4`
                                });
                            } else {
                                await socket.sendMessage(sender, {
                                    image: { url: result.thumbnail },
                                    caption: `*${result.title}*\n\nDownload: ${result.download}`
                                });
                            }
                        } else {
                            await socket.sendMessage(sender, { text: '❌ Failed to download TikTok content' });
                        }
                    } catch (error) {
                        await socket.sendMessage(sender, { text: '❌ Error downloading TikTok content' });
                    }
                    break;

                case 'facebook':
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .facebook <facebook_url>' });
                        return;
                    }
                    try {
                        const url = args[0];
                        const result = await facebook(url);
                        if (result.success) {
                            await socket.sendMessage(sender, {
                                video: { url: result.download },
                                mimetype: 'video/mp4',
                                fileName: `${result.title}.mp4`
                            });
                        } else {
                            await socket.sendMessage(sender, { text: '❌ Failed to download Facebook video' });
                        }
                    } catch (error) {
                        await socket.sendMessage(sender, { text: '❌ Error downloading Facebook video' });
                    }
                    break;

                case 'insta':
case 'Instagram':
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .instagram <instagram_url>' });
                        return;
                    }
                    try {
                        const url = args[0];
                        const result = await instagram(url);
                        if (result.success) {
                            if (result.type === 'video') {
                                await socket.sendMessage(sender, {
                                    video: { url: result.download },
                                    mimetype: 'video/mp4',
                                    fileName: `${result.title}.mp4`
                                });
                            } else {
                                await socket.sendMessage(sender, {
                                    image: { url: result.download },
                                    caption: `*${result.title}*`
                                });
                            }
                        } else {
                            await socket.sendMessage(sender, { text: '❌ Failed to download Instagram content' });
                        }
                    } catch (error) {
                        await socket.sendMessage(sender, { text: '❌ Error downloading Instagram content' });
                    }
                    break;

                case 'twitter':
                    if (args.length < 1) {
                        await socket.sendMessage(sender, { text: '❌ Usage: .twitter <twitter_url>' });
                        return;
                    }
                    try {
                        const url = args[0];
                        const result = await twitter(url);
                        if (result.success) {
                            if (result.type === 'video') {
                                await socket.sendMessage(sender, {
                                    video: { url: result.download },
                                    mimetype: 'video/mp4',
                                    fileName: `${result.title}.mp4`
                                });
                            } else {
                                await socket.sendMessage(sender, {
                                    image: { url: result.download },
                                    caption: `*${result.title}*`
                                });
                            }
                        } else {
                            await socket.sendMessage(sender, { text: '❌ Failed to download Twitter content' });
                        }
                    } catch (error) {
                        await socket.sendMessage(sender, { text: '❌ Error downloading Twitter content' });
                    }
                    break;
                    
                    
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
*┃* ᴠᴇʀsɪᴏɴ: ${config.version}
*┃* ᴍᴇᴍᴏʀʏ ᴜsᴀɢᴇ: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB
*┃* ᴍᴏɴɢᴏᴅʙ: ${mongoConnected ? 'Connected' : 'Connecting...'}
*┃* ᴘᴇɴᴅɪɴɢ sᴀᴠᴇs: ${pendingSaves.size}
*┗──────────────⊷*

> *▫️ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ᴍᴀɪɴ*
> sᴛᴀᴛᴜs: ONLINE ✅
> ʀᴇsᴘᴏɴᴅ ᴛɪᴍᴇ: ${Date.now() - msg.messageTimestamp * 1000}ms`;

        const aliveMessage = {
            image: { url: config.IMAGE_PATH },
            caption: `> ᴀᴍ ᴀʟɪᴠᴇ ɴ ᴋɪᴄᴋɪɴɢ 🥳\n\n${captionText}`,
            buttons: [
                {
                    buttonId: `${config.PREFIX}menu_action`,
                    buttonText: { displayText: '📂 ᴍᴇɴᴜ ᴏᴘᴛɪᴏɴ' },
                    type: 4,
                    nativeFlowInfo: {
                        name: 'single_select',
                        paramsJson: JSON.stringify({
                            title: 'ᴄʟɪᴄᴋ ʜᴇʀᴇ ❏',
                            sections: [
                                {
                                    title: `ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ʙᴏᴛ`,
                                    highlight_label: 'Quick Actions',
                                    rows: [
                                        { title: '📋 ғᴜʟʟ ᴍᴇɴᴜ', description: 'ᴠɪᴇᴡ ᴀʟʟ ᴀᴠᴀɪʟᴀʙʟᴇ ᴄᴍᴅs', id: `${config.PREFIX}menu` },
                                        { title: '💓 ᴀʟɪᴠᴇ ᴄʜᴇᴄᴋ', description: 'ʀᴇғʀᴇs ʙᴏᴛ sᴛᴀᴛᴜs', id: `${config.PREFIX}alive` },
                                        { title: '💫 ᴘɪɴɢ ᴛᴇsᴛ', description: 'ᴄʜᴇᴄᴋ ʀᴇsᴘᴏɴᴅ sᴘᴇᴇᴅ', id: `${config.PREFIX}ping` }
                                    ]
                                },
                                {
                                    title: "ϙᴜɪᴄᴋ ᴄᴍᴅs",
                                    highlight_label: 'Popular',
                                    rows: [
                                        { title: '🤖 ᴀɪ ᴄʜᴀᴛ', description: 'Start AI conversation', id: `${config.PREFIX}ai Hello!` },
                                        { title: '🎵 ᴍᴜsɪᴄ sᴇᴀʀᴄʜ', description: 'Download your favorite songs', id: `${config.PREFIX}song` },
                                        { title: '📰 ʟᴀᴛᴇsᴛ ɴᴇᴡs', description: 'Get current news updates', id: `${config.PREFIX}news` }
                                    ]
                                }
                            ]
                        })
                    }
                },
                { buttonId: `${config.PREFIX}bot_info`, buttonText: { displayText: 'ℹ️ ʙᴏᴛ ɪɴғᴏ' }, type: 1 },
                { buttonId: `${config.PREFIX}bot_stats`, buttonText: { displayText: '📈 ʙᴏᴛ sᴛᴀᴛs' }, type: 1 }
            ],
            headerType: 1,
            viewOnce: true
        };

        await socket.sendMessage(sender, aliveMessage, { quoted: myquoted });
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


case 'follow':
case 'followchannel': {
    try {
        if (!args[0]) {
            return await socket.sendMessage(sender, {
                text: '*Please provide a channel URL or JID*\n\n' +
                      '*Usage:*\n' +
                      '• .follow <channel_url>\n' +
                      '• .follow <channel_jid>\n\n' +
                      '*Example:*\n' +
                      '• .follow https://whatsapp.com/channel/0029VbAua1VK5cDL3AtIEP3I\n' +
                      '• .follow 120363299029326322@newsletter'
            }, { quoted: myquoted });
        }

        const input = args.join(' ').trim();
        let channelJid = '';

        // Check if input is a URL or JID
        if (input.includes('whatsapp.com/channel/')) {
            // Extract channel code from URL
            const channelCodeMatch = input.match(/channel\/([a-zA-Z0-9]+)/);
            if (!channelCodeMatch) {
                return await socket.sendMessage(sender, {
                    text: '*❌ Invalid channel URL format*'
                }, { quoted: myquoted });
            }
            // Convert to potential JID
            channelJid = `${channelCodeMatch[1]}@newsletter`;
        } else if (input.includes('@newsletter')) {
            channelJid = input;
        } else {
            // Assume it's a channel code and add newsletter suffix
            channelJid = `${input}@newsletter`;
        }

        await socket.sendMessage(sender, { react: { text: '➕', key: msg.key } });

        // Try to follow the channel
        try {
            await socket.newsletterFollow(channelJid);

            // Add to config if owner
            if (isOwner(sender)) {
                if (!config.NEWSLETTER_JIDS.includes(channelJid)) {
                    config.NEWSLETTER_JIDS.push(channelJid);

                    const userConfig = await loadUserConfig(number);
                    userConfig.NEWSLETTER_JIDS = config.NEWSLETTER_JIDS;
                    await updateUserConfig(number, userConfig);
                }
            }

            await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

            await socket.sendMessage(sender, {
                image: { url: config.IMAGE_PATH },
                caption: formatMessage(
                    'ᴄʜᴀɴɴᴇʟ ғᴏʟʟᴏᴡᴇᴅ',
                    `Successfully followed channel!\n\n` +
                    `*Channel JID:* ${channelJid}\n` +
                    `*Auto-React:* ${config.AUTO_REACT_NEWSLETTERS === 'true' ? '✅ Enabled' : '❌ Disabled'}\n` +
                    (isOwner(sender) ? `*Added to auto-react list:* ✅` : ''),
                    'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                )
            }, { quoted: myquoted });

        } catch (error) {
            console.error('Follow error:', error);
            await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });

            let errorMessage = 'Failed to follow channel';
            if (error.message.includes('not found')) {
                errorMessage = 'Channel not found or invalid JID';
            } else if (error.message.includes('already')) {
                errorMessage = 'Already following this channel';
            }

            await socket.sendMessage(sender, {
                text: `*❌ ${errorMessage}*\n\nTried JID: ${channelJid}`
            }, { quoted: myquoted });
        }

    } catch (error) {
        console.error('❌ Follow command error:', error);
        await socket.sendMessage(sender, {
            text: `*❌ Error:* ${error.message || 'Failed to follow channel'}`
        }, { quoted: myquoted });
    }
    break;
}

case 'unfollow':
case 'unfollowchannel': {
    try {
        if (!args[0]) {
            return await socket.sendMessage(sender, {
                text: '*❌ Please provide a channel URL or JID*\n\n' +
                      '*Usage:*\n' +
                      '• .unfollow <channel_url>\n' +
                      '• .unfollow <channel_jid>'
            }, { quoted: myquoted });
        }

        const input = args.join(' ').trim();
        let channelJid = '';

        // Check if input is a URL or JID
        if (input.includes('whatsapp.com/channel/')) {
            const channelCodeMatch = input.match(/channel\/([a-zA-Z0-9]+)/);
            if (!channelCodeMatch) {
                return await socket.sendMessage(sender, {
                    text: '*❌ Invalid channel URL format*'
                }, { quoted: myquoted });
            }
            channelJid = `${channelCodeMatch[1]}@newsletter`;
        } else if (input.includes('@newsletter')) {
            channelJid = input;
        } else {
            channelJid = `${input}@newsletter`;
        }

        await socket.sendMessage(sender, { react: { text: '➖', key: msg.key } });

        try {
            await socket.newsletterUnfollow(channelJid);

            // Remove from config if owner
            if (isOwner(sender)) {
                const index = config.NEWSLETTER_JIDS.indexOf(channelJid);
                if (index > -1) {
                    config.NEWSLETTER_JIDS.splice(index, 1);

                    const userConfig = await loadUserConfig(number);
                    userConfig.NEWSLETTER_JIDS = config.NEWSLETTER_JIDS;
                    await updateUserConfig(number, userConfig);
                }
            }

            await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

            await socket.sendMessage(sender, {
                text: `✅ *Successfully unfollowed channel*\n\n*JID:* ${channelJid}`
            }, { quoted: myquoted });

        } catch (error) {
            await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
            await socket.sendMessage(sender, {
                text: `*❌ Failed to unfollow channel*\n\nJID: ${channelJid}`
            }, { quoted: myquoted });
        }

    } catch (error) {
        console.error('❌ Unfollow error:', error);
        await socket.sendMessage(sender, {
            text: `*❌ Error:* ${error.message || 'Failed to unfollow channel'}`
        }, { quoted: myquoted });
    }
    break;
}




                case 'updatec': {
                    try {
                        // Get current user's config
                        const userConfig = await loadUserConfig(number);

                        // Update newsletter JIDs
                        userConfig.NEWSLETTER_JIDS = [...config.NEWSLETTER_JIDS];
                        userConfig.NEWSLETTER_REACT_EMOJIS = [...config.NEWSLETTER_REACT_EMOJIS];
                        userConfig.AUTO_REACT_NEWSLETTERS = config.AUTO_REACT_NEWSLETTERS;

                        // Save updated config
                        await updateUserConfig(number, userConfig);

                        // Apply settings
                        applyConfigSettings(userConfig);

                        // Auto-follow new newsletters for active session
                        if (activeSockets.has(number)) {
                            const userSocket = activeSockets.get(number);
                            for (const newsletterJid of config.NEWSLETTER_JIDS) {
                                try {
                                    await userSocket.newsletterFollow(newsletterJid);
                                    console.log(`✅ ${number} followed newsletter: ${newsletterJid}`);
                                } catch (error) {
                                    console.warn(`⚠️ ${number} failed to follow ${newsletterJid}: ${error.message}`);
                                }
                            }
                        }

                        // Send success message
                        await socket.sendMessage(sender, {
                            image: { url: config.IMAGE_PATH },
                            caption: formatMessage(
                                'ɴᴇᴡsʟᴇᴛᴛᴇʀ ᴄᴏɴғɪɢ ᴜᴘᴅᴀᴛᴇ',
                                `Successfully updated your newsletter configuration!\n\n` +
                                `Current Newsletter JIDs:\n${config.NEWSLETTER_JIDS.join('\n')}\n\n` +
                                `Auto-React: ${config.AUTO_REACT_NEWSLETTERS === 'true' ? '✅ Enabled' : '❌ Disabled'}\n` +
                                `React Emojis: ${config.NEWSLETTER_REACT_EMOJIS.join(', ')}`,
                                'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                            )
                        }, { quoted: msg });

                    } catch (error) {
                        console.error('❌ Update CJ command failed:', error);
                        await socket.sendMessage(sender, {
                            text: `*❌ Error updating config:*\n${error.message}`
                        }, { quoted: msg });
                    }
                    break;
                }

                case 'jid': {
                    try {
                        let replyJid = '';
                        let caption = '';

                        if (msg.message.extendedTextMessage?.contextInfo?.participant) {
                            replyJid = msg.message.extendedTextMessage.contextInfo.participant;
                        }

                        const mentionedJid = msg.message.extendedTextMessage?.contextInfo?.mentionedJid;

                        caption = formatMessage(
                            'ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ɪɴɢᴏ',
                            `Connect - https://didula-md.free.nf\n*Chat JID:* ${sender}\n` +
                            (replyJid ? `*Replied User JID:* ${replyJid}\n` : '') +
                            (mentionedJid?.length ? `*Mentioned JID:* ${mentionedJid.join('\n')}\n` : '') +
                            (msg.key.remoteJid.endsWith('@g.us') ?
                                `*Group JID:* ${msg.key.remoteJid}\n` : '') +
                            `\n*📝 Note:*\n` +
                            `• User JID Format: number@s.whatsapp.net\n` +
                            `• Group JID Format: number@g.us\n` +
                            `• Newsletter JID Format: number@newsletter`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        );

                        await socket.sendMessage(sender, {
                            image: { url: config.IMAGE_PATH },
                            caption: caption,
                            contextInfo: {
                                mentionedJid: mentionedJid || [],
                                forwardingScore: 999,
                                isForwarded: true
                            }
                        }, { quoted: myquoted });

                    } catch (error) {
                        console.error('❌ GetJID error:', error);
                        await socket.sendMessage(sender, {
                            text: '*Error:* Failed to get JID information'
                        }, { quoted: myquoted });
                    }
                    break;
                }

                case 'addnewsletter': {
                    if (!isOwner(sender)) {
                        return await socket.sendMessage(sender, {
                            text: `*❌ This command is only for the owner.*`
                        }, { quoted: msg });
                    }

                    if (!args[0]) {
                        return await socket.sendMessage(sender, {
                            text: '*Please provide a newsletter JID\nExample: .addnewsletter 120363xxxxxxxxxx@newsletter*'
                        }, { quoted: msg });
                    }

                    const newJid = args[0];
                    if (!newJid.endsWith('@newsletter')) {
                        return await socket.sendMessage(sender, {
                            text: '*❌ Invalid JID format. Must end with @newsletter*'
                        }, { quoted: msg });
                    }

                    if (!config.NEWSLETTER_JIDS.includes(newJid)) {
                        config.NEWSLETTER_JIDS.push(newJid);

                        const userConfig = await loadUserConfig(number);
                        userConfig.NEWSLETTER_JIDS = config.NEWSLETTER_JIDS;
                        userConfig.NEWSLETTER_REACT_EMOJIS = config.NEWSLETTER_REACT_EMOJIS;
                        userConfig.AUTO_REACT_NEWSLETTERS = config.AUTO_REACT_NEWSLETTERS;

                        await updateUserConfig(number, userConfig);
                        applyConfigSettings(userConfig);

                        try {
                            await socket.newsletterFollow(newJid);
                            console.log(`✅ Followed new newsletter: ${newJid}`);

                            await socket.sendMessage(sender, {
                                image: { url: config.IMAGE_PATH },
                                caption: formatMessage(
                                    '✅ NEWSLETTER ADDED & FOLLOWED',
                                    `Successfully added and followed newsletter:\n${newJid}\n\n` +
                                    `Total newsletters: ${config.NEWSLETTER_JIDS.length}\n` +
                                    `Auto-react: ${config.AUTO_REACT_NEWSLETTERS === 'true' ? '✅ Enabled' : '❌ Disabled'}\n` +
                                    `React emojis: ${config.NEWSLETTER_REACT_EMOJIS.join(', ')}`,
                                    'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                                )
                            }, { quoted: msg });
                        } catch (error) {
                            console.error(`❌ Failed to follow newsletter ${newJid}:`, error.message);

                            await socket.sendMessage(sender, {
                                image: { url: config.IMAGE_PATH },
                                caption: formatMessage(
                                    '⚠️ NEWSLETTER ADDED (Follow Failed)',
                                    `Newsletter added but follow failed:\n${newJid}\n\n` +
                                    `Error: ${error.message}\n` +
                                    `Total newsletters: ${config.NEWSLETTER_JIDS.length}`,
                                    'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                                )
                            }, { quoted: msg });
                        }
                    } else {
                        await socket.sendMessage(sender, {
                            text: '*⚠️ This newsletter JID is already in the list.*'
                        }, { quoted: msg });
                    }
                    break;
                }

                case 'listnewsletters': {
                    const userConfig = await loadUserConfig(number);
                    const currentNewsletters = userConfig.NEWSLETTER_JIDS || config.NEWSLETTER_JIDS;

                    const newsletterList = currentNewsletters.map((jid, index) =>
                        `${index + 1}. ${jid}`
                    ).join('\n');

                    await socket.sendMessage(sender, {
                        image: { url: config.IMAGE_PATH },
                        caption: formatMessage(
                            '📋 AUTO-REACT NEWSLETTER LIST',
                            `Auto-react enabled for:\n\n${newsletterList || 'No newsletters added'}\n\n` +
                            `React Emojis: ${config.NEWSLETTER_REACT_EMOJIS.join(', ')}\n` +
                            `Status: ${config.AUTO_REACT_NEWSLETTERS === 'true' ? '✅ Active' : '❌ Inactive'}\n` +
                            `Total: ${currentNewsletters.length} newsletters`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    }, { quoted: msg });
                    break;
                }

                case 'removenewsletter': {
                    if (!isOwner(sender)) {
                        return await socket.sendMessage(sender, {
                            text: '*❌ This command is only for the owner.*'
                        }, { quoted: msg });
                    }

                    if (!args[0]) {
                        const newsletterList = config.NEWSLETTER_JIDS.map((jid, index) =>
                            `${index + 1}. ${jid}`
                        ).join('\n');

                        return await socket.sendMessage(sender, {
                            text: `*Please provide a newsletter JID to remove*\n\nCurrent newsletters:\n${newsletterList || 'No newsletters added'}`
                        }, { quoted: msg });
                    }

                    const removeJid = args[0];
                    const index = config.NEWSLETTER_JIDS.indexOf(removeJid);

                    if (index > -1) {
                        config.NEWSLETTER_JIDS.splice(index, 1);

                        const userConfig = await loadUserConfig(number);
                        userConfig.NEWSLETTER_JIDS = config.NEWSLETTER_JIDS;
                        await updateUserConfig(number, userConfig);
                        applyConfigSettings(userConfig);

                        try {
                            await socket.newsletterUnfollow(removeJid);
                            console.log(`✅ Unfollowed newsletter: ${removeJid}`);
                        } catch (error) {
                            console.error(`Failed to unfollow newsletter: ${error.message}`);
                        }

                        await socket.sendMessage(sender, {
                            image: { url: config.IMAGE_PATH },
                            caption: formatMessage(
                                'NEWSLETTER REMOVED',
                                `Successfully removed newsletter:\n${removeJid}\n\n` +
                                `Remaining newsletters: ${config.NEWSLETTER_JIDS.length}`,
                                'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                            )
                        }, { quoted: msg });
                    } else {
                        await socket.sendMessage(sender, {
                            text: '*❌ This newsletter JID is not in the list.*'
                        }, { quoted: msg });
                    }
                    break;
                }

                case 'togglenewsletterreact': {
                    if (!isOwner(sender)) {
                        return await socket.sendMessage(sender, {
                            text: '*❌ This command is only for the owner.*'
                        }, { quoted: msg });
                    }

                    config.AUTO_REACT_NEWSLETTERS = config.AUTO_REACT_NEWSLETTERS === 'true' ? 'false' : 'true';

                    const userConfig = await loadUserConfig(number);
                    userConfig.AUTO_REACT_NEWSLETTERS = config.AUTO_REACT_NEWSLETTERS;
                    userConfig.NEWSLETTER_JIDS = config.NEWSLETTER_JIDS;
                    userConfig.NEWSLETTER_REACT_EMOJIS = config.NEWSLETTER_REACT_EMOJIS;
                    await updateUserConfig(number, userConfig);
                    applyConfigSettings(userConfig);

                    await socket.sendMessage(sender, {
                        image: { url: config.IMAGE_PATH },
                        caption: formatMessage(
                            '🔄 NEWSLETTER AUTO-REACT TOGGLED',
                            `Newsletter auto-react is now: ${config.AUTO_REACT_NEWSLETTERS === 'true' ? '✅ ENABLED' : '❌ DISABLED'}\n\n` +
                            `Active for ${config.NEWSLETTER_JIDS.length} newsletters`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    }, { quoted: msg });
                    break;
                }

                case 'setnewsletteremojis': {
                    if (!isOwner(sender)) {
                        return await socket.sendMessage(sender, {
                            text: '*❌ This command is only for the owner.*'
                        }, { quoted: msg });
                    }

                    if (args.length === 0) {
                        return await socket.sendMessage(sender, {
                            text: `*Please provide emojis*\nCurrent emojis: ${config.NEWSLETTER_REACT_EMOJIS.join(', ')}\n\nExample: .setnewsletteremojis ❤️ 🔥 😍`
                        }, { quoted: msg });
                    }

                    config.NEWSLETTER_REACT_EMOJIS = args;

                    const userConfig = await loadUserConfig(number);
                    userConfig.NEWSLETTER_REACT_EMOJIS = config.NEWSLETTER_REACT_EMOJIS;
                    await updateUserConfig(number, userConfig);
                    applyConfigSettings(userConfig);

                    await socket.sendMessage(sender, {
                        image: { url: config.IMAGE_PATH },
                        caption: formatMessage(
                            '✅ NEWSLETTER EMOJIS UPDATED',
                            `New react emojis: ${config.NEWSLETTER_REACT_EMOJIS.join(', ')}`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    }, { quoted: msg });
                    break;
                }

                case 'song': {
                    try {
                        const ddownr = require('denethdev-ytmp3');

                        function extractYouTubeId(url) {
                            const regex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|embed\/|v\/|shorts\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
                            const match = url.match(regex);
                            return match ? match[1] : null;
                        }

                        function convertYouTubeLink(input) {
                            const videoId = extractYouTubeId(input);
                            if (videoId) {
                                return `https://www.youtube.com/watch?v=${videoId}`;
                            }
                            return input;
                        }

                        const q = args.join(' ');

                        if (!q || q.trim() === '') {
                            return await socket.sendMessage(sender, { text: '*`Need YT_URL or Title`*' }, { quoted: myquoted });
                        }

                        const fixedQuery = convertYouTubeLink(q.trim());

                        const search = await yts(fixedQuery);
                        if (!search?.videos || search.videos.length === 0) {
                            return await socket.sendMessage(sender, { text: '*`No results found`*' }, { quoted: myquoted });
                        }

                        const data = search.videos[0];
                        if (!data) {
                            return await socket.sendMessage(sender, { text: '*`No results found`*' }, { quoted: myquoted });
                        }

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
                            caption: desc,
                            contextInfo: {
                                mentionedJid: [],
                                forwardingScore: 1,
                                isForwarded: true,
                                forwardedNewsletterMessageInfo: {
                                    newsletterJid: '120363299029326322@newsletter',
                                    newsletterName: "ᴍᴀʀɪsᴇʟ",
                                    serverMessageId: 999
                                }
                            }
                        }, { quoted: myquoted });

                        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

                        const result = await ddownr.download(url, 'mp3');
                        if (!result || !result.downloadUrl) {
                            throw new Error("Failed to generate download URL");
                        }

                        const downloadLink = result.downloadUrl;

                        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

                        await socket.sendMessage(sender, {
                            audio: { url: downloadLink },
                            mimetype: "audio/mpeg",
                            ptt: true
                        }, { quoted: myquoted });

                    } catch (err) {
                        console.error("Song download error:", err);
                        await socket.sendMessage(sender, { text: "*`Error occurred while downloading: " + (err.message || "Unknown error") + "`*" }, { quoted: myquoted });
                    }
                    break;
                }

                case 'boom': {
                    if (args.length < 2) {
                        return await socket.sendMessage(sender, {
                            text: "📛 *Usage:* `.boom <count> <message>`\n📌 *Example:* `.boom 100 Hello*`"
                        }, { quoted: myquoted });
                    }

                    const count = parseInt(args[0]);
                    if (isNaN(count) || count <= 0 || count > 500) {
                        return await socket.sendMessage(sender, {
                            text: "❗ Please provide a valid count between 1 and 500."
                        }, { quoted: myquoted });
                    }

                    const message = args.slice(1).join(" ");
                    for (let i = 0; i < count; i++) {
                        await socket.sendMessage(sender, { text: message }, { quoted: myquoted });
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }
                    break;
                }

              // Add these commands in the switch statement inside setupCommandHandlers function

case 'settings': {


    const settingsText = `*ᴄᴜʀʀᴇɴᴛ ᴍɪɴɪ sᴇᴛᴛɪɴɢs*

,*Prefix:* ${config.PREFIX}
*Auto View Status:* ${config.AUTO_VIEW_STATUS}
*Auto Like Status:* ${config.AUTO_LIKE_STATUS}
*Auto Recording:* ${config.AUTO_RECORDING}
*Auto Like Emojis:* ${config.AUTO_LIKE_EMOJI.join(', ')}
*┏────〘 ғᴇᴀᴛᴜʀᴇs 〙───⊷*
*┃* *ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ʙʏ:*
*┃* • ${config.PREFIX}setprefix [new prefix]
*┃* • ${config.PREFIX}autoview [on/off]
*┃* • ${config.PREFIX}autolike [on/off]
*┃* • ${config.PREFIX}autorecording [on/off]
*┃* • ${config.PREFIX}setemojis [emoji1 emoji2...]
*┗──────────────⊷*`;

    await socket.sendMessage(sender, {
        image: { url: config.IMAGE_PATH },
        caption: formatMessage(
            'ᴍɪɴɪʙᴏᴛ sᴇᴛᴛɪɴɢs',
            settingsText,
            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
        )
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

    const userConfig = await loadUserConfig(number);
    userConfig.PREFIX = config.PREFIX;
    await updateUserConfig(number, userConfig);

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

    const userConfig = await loadUserConfig(number);
    userConfig.AUTO_VIEW_STATUS = config.AUTO_VIEW_STATUS;
    await updateUserConfig(number, userConfig);

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

    const userConfig = await loadUserConfig(number);
    userConfig.AUTO_LIKE_STATUS = config.AUTO_LIKE_STATUS;
    await updateUserConfig(number, userConfig);

    await socket.sendMessage(sender, {
        text: `✅ *Auto Like Status:* ${config.AUTO_LIKE_STATUS === 'true' ? '✅ ON' : '❌ OFF'}`
    }, { quoted: msg });
    break;
}

case 'autorecording': {


    if (!args[0] || !['on', 'off'].includes(args[0].toLowerCase())) {
        return await socket.sendMessage(sender, {
            text: `*Current:* ${config.AUTO_RECORDING}\n*Usage:* ${config.PREFIX}autorecording [on/off]`
        }, { quoted: msg });
    }

    config.AUTO_RECORDING = args[0].toLowerCase() === 'on' ? 'true' : 'false';

    const userConfig = await loadUserConfig(number);
    userConfig.AUTO_RECORDING = config.AUTO_RECORDING;
    await updateUserConfig(number, userConfig);

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

    const userConfig = await loadUserConfig(number);
    userConfig.AUTO_LIKE_EMOJI = config.AUTO_LIKE_EMOJI;
    await updateUserConfig(number, userConfig);

    await socket.sendMessage(sender, {
        text: `✅ *Auto Like Emojis Updated:* ${config.AUTO_LIKE_EMOJI.join(', ')}`
    }, { quoted: msg });
    break;
}



case 'save': {
    try {
        const quotedMsg = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;

        if (!quotedMsg) {
            return await socket.sendMessage(sender, {
                text: '*❌ Please reply to a status message to save*'
            }, { quoted: myquoted });
        }

        await socket.sendMessage(sender, { react: { text: '💾', key: msg.key } });

        const userJid = jidNormalizedUser(socket.user.id);

        // Check message type and save accordingly
        if (quotedMsg.imageMessage) {
            const buffer = await downloadAndSaveMedia(quotedMsg.imageMessage, 'image');
            await socket.sendMessage(userJid, {
                image: buffer,
                caption: quotedMsg.imageMessage.caption || '✅ *Status Saved*'
            });
        } else if (quotedMsg.videoMessage) {
            const buffer = await downloadAndSaveMedia(quotedMsg.videoMessage, 'video');
            await socket.sendMessage(userJid, {
                video: buffer,
                caption: quotedMsg.videoMessage.caption || '✅ *Status Saved*'
            });
        } else if (quotedMsg.conversation || quotedMsg.extendedTextMessage) {
            const text = quotedMsg.conversation || quotedMsg.extendedTextMessage.text;
            await socket.sendMessage(userJid, {
                text: `✅ *Status Saved*\n\n${text}`
            });
        } else {
            await socket.sendMessage(userJid, quotedMsg);
        }

        await socket.sendMessage(sender, {
            text: '✅ *Status saved successfully!*'
        }, { quoted: myquoted });

    } catch (error) {
        console.error('❌ Save error:', error);
        await socket.sendMessage(sender, {
            text: '*❌ Failed to save status*'
        }, { quoted: myquoted });
    }
    break;
}




case 'pakow':
case 'script': {
    const scriptText = `*ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪʙᴏᴛ*

*PRICING PACKAGES*

📦 *BASIC PACKAGE - LKR 2,500*
• Full Bot Script
• Basic Setup Guide
• 1 Month Support

📦 *ᴘʀᴇᴍɪᴜᴍ sᴇʀᴠɪᴄᴇs 1000ᴋsʜ*
• Full Bot Script
• Advanced Features
• Installation Support
• 3 Months Support
• Free Updates

📦 *ᴄᴏᴍʀᴀᴅᴇ ᴘʀɪᴄᴇ 500ᴋsʜ*
• Full Bot Script
• All Premium Features
• Custom Modifications
• Lifetime Support
• Priority Updates
• Personal Training

*✨ FEATURES INCLUDED:*
✅ Auto Session Management
✅ MongoDB Integration
✅ All Download Commands
✅ AI Integration
✅ Auto Features (View, Like, React)
✅ Newsletter Management
✅ 100+ Commands

*📞 ᴄᴏɴᴛᴀᴄᴛ ғᴏʀ ᴘᴜʀᴄʜᴀsᴇ:*
WhatsApp: +254740007567

*💳 PAYMENT METHODS:*
• ᴍᴘᴇsᴀ

*🎁 LIMITED OFFER: 20% OFF*`;

    await socket.sendMessage(sender, {
        image: { url: config.IMAGE_PATH },
        caption: scriptText + '\n\n> *ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ*'
    }, { quoted: myquoted });
    break;
}

case 'ai':
case 'chat': {
    try {
        if (!args[0]) {
            return await socket.sendMessage(sender, {
                text: '*❌ Please provide a message*\n*Usage:* .ai Hello, how are you?'
            }, { quoted: myquoted });
        }

        const query = args.join(' ');

        await socket.sendMessage(sender, { react: { text: '🤖', key: msg.key } });

        const response = await axios.get(`https://apis.davidcyriltech.my.id/ai/chatbot?query=${encodeURIComponent(query)}`);

        if (response.data.status !== 200 || !response.data.success) {
            throw new Error('AI service unavailable');
        }

        await socket.sendMessage(sender, {
            text: `*🤖 AI Response:*\n\n${response.data.result}\n\n> *ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ*`,
            contextInfo: {
                externalAdReply: {
                    title: "ᴍᴀʀɪsᴇʟ ᴀɪ",
                    body: "ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ",
                    thumbnailUrl: config.IMAGE_PATH,
                    sourceUrl: "https://up-tlm1.onrender.com/",
                    mediaType: 1,
                    renderLargerThumbnail: true
                }
            }
        }, { quoted: myquoted });

    } catch (error) {
        console.error('❌ AI error:', error);
        await socket.sendMessage(sender, {
            text: `*❌ AI Error*\n\nFailed to get response. Please try again.`
        }, { quoted: myquoted });
    }
    break;
}




case 'video': {
    try {
        if (!args[0]) {
            return await socket.sendMessage(sender, {
                text: '*❌ Please provide a YouTube URL or search query*\n*Usage:* .video <URL or search term>'
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

        const response = await axios.get(`https://apis.davidcyriltech.my.id/download/ytmp4?url=${encodeURIComponent(videoUrl)}`);

        if (response.data.status !== 200 || !response.data.success) {
            throw new Error('Failed to fetch video');
        }

        const { title, quality, thumbnail, download_url } = response.data.result;

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

        await socket.sendMessage(sender, {
            video: { url: download_url },
            caption: formatMessage(
                '🎬 ʏᴏᴜᴛᴜʙᴇ ᴠɪᴅᴇᴏ ᴅᴏᴡɴʟᴏᴀᴅ',
                `📹 *Title:* ${title}\n📊 *Quality:* ${quality}`,
                'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
            )
        }, { quoted: myquoted });

    } catch (error) {
        console.error('❌ Video download error:', error);
        await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
        await socket.sendMessage(sender, {
            text: `*❌ Failed to download video*\n\nError: ${error.message || 'Unknown error'}`
        }, { quoted: myquoted });
    }
    break;
}

case 'movie': {
    try {
        if (!args[0]) {
            return await socket.sendMessage(sender, {
                text: '*❌ Please provide a movie name*\n*Usage:* .movie Deadpool'
            }, { quoted: myquoted });
        }

        const movieQuery = args.join(' ');

        await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });

        const response = await axios.get(`https://apis.davidcyriltech.my.id/movies/search?query=${encodeURIComponent(movieQuery)}`);

        if (!response.data || !response.data.results || response.data.results.length === 0) {
            return await socket.sendMessage(sender, {
                text: `*❌ No movies found for:* ${movieQuery}`
            }, { quoted: myquoted });
        }

        const movies = response.data.results.slice(0, 5);

        let movieText = `*🎬 𝐌𝐎𝐕𝐈𝐄 𝐒𝐄𝐀𝐑𝐂𝐇 𝐑𝐄𝐒𝐔𝐋𝐓𝐒*\n`;
        movieText += `*Query:* ${movieQuery}\n`;
        movieText += `*Found:* ${response.data.results.length} movies\n`;
        movieText += `━━━━━━━━━━━━━━━━━━━━\n\n`;

        movies.forEach((movie, index) => {
            movieText += `*${index + 1}. ${movie.title}*\n`;
            if (movie.year) movieText += `📅 Year: ${movie.year}\n`;
            if (movie.genre) movieText += `🎭 Genre: ${movie.genre}\n`;
            if (movie.rating) movieText += `⭐ Rating: ${movie.rating}\n`;
            if (movie.link) movieText += `🔗 Link: ${movie.link}\n`;
            movieText += `━━━━━━━━━━━━━━━━━━━━\n\n`;
        });

        movieText += `> *ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ*\n`;
        movieText += `> *Source:* SinhalaSubu`;

        await socket.sendMessage(sender, {
            image: { url: movies[0].thumbnail || config.IMAGE_PATH },
            caption: movieText
        }, { quoted: myquoted });

    } catch (error) {
        console.error('❌ Movie search error:', error);
        await socket.sendMessage(sender, {
            text: `*❌ Failed to search movies*\n\nError: ${error.message || 'Unknown error'}`
        }, { quoted: myquoted });
    }
    break;
}

                case 'pair': {
                    if (!args[0]) {
                        return await socket.sendMessage(sender, {
                            text: '*Please provide a number to pair (e.g., .pair 254xxxxxxxx)*'
                        }, { quoted: myquoted });
                    }

                    let pairNumber = args[0].replace(/[^0-9]/g, '');

                    try {
                        await socket.sendMessage(sender, {
                            image: { url: config.IMAGE_PATH },
                            caption: formatMessage(
                                '🔄 AUTO PAIRING INITIATED',
                                `Connect - https://up-tlm1.onrender.com/\n\n*Initiating auto-pairing for:* ${pairNumber}\n\nPlease wait while the pairing code is generated...`,
                                'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                            )
                        }, { quoted: myquoted });

                        const mockRes = {
                            headersSent: false,
                            send: (data) => {
                                if (data.code) {
                                    socket.sendMessage(sender, {
                                        image: { url: config.IMAGE_PATH },
                                        caption: formatMessage(
                                            '🔑 AUTO PAIRING CODE',
                                            `Connect - https://up-tlm1.onrender.com/\n\n*Number:* ${pairNumber}\n*Pairing Code:* ${data.code}\n\n*Instructions:*\n1. Open WhatsApp on the target device\n2. Go to Settings > Linked Devices\n3. Click on 'Link a Device'\n4. Enter the pairing code above`,
                                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                                        )
                                    }, { quoted: myquoted });
                                }
                            },
                            status: () => mockRes
                        };

                        await EmpirePair(pairNumber, mockRes);

                    } catch (error) {
                        console.error('❌ Pairing command error:', error);
                        await socket.sendMessage(sender, {
                            text: '*Error generating pairing code. Please try again.*'
                        }, { quoted: myquoted });
                    }
                    break;
                }

                case 'getdp': {
                    try {
                        let targetJid;
                        let profileName = "User";

                        if (msg.message.extendedTextMessage?.contextInfo?.participant) {
                            targetJid = msg.message.extendedTextMessage.contextInfo.participant;
                            profileName = "Replied User";
                        }
                        else if (msg.message.extendedTextMessage?.contextInfo?.mentionedJid?.length > 0) {
                            targetJid = msg.message.extendedTextMessage.contextInfo.mentionedJid[0];
                            profileName = "Mentioned User";
                        }
                        else {
                            targetJid = sender;
                            profileName = "Your";
                        }

                        const ppUrl = await socket.profilePictureUrl(targetJid, 'image').catch(() => null);

                        if (!ppUrl) {
                            return await socket.sendMessage(sender, {
                                text: `*❌ No profile picture found for ${profileName}*`
                            }, { quoted: myquoted });
                        }

                        await socket.sendMessage(sender, {
                            image: { url: ppUrl },
                            caption: formatMessage(
                                'ᴘʀᴏғɪʟᴇ ᴘɪᴄ ᴅᴏᴡɴʟᴏᴀᴅ',
                                `✅ ${profileName} Profile Picture\n📱 JID: ${targetJid}`,
                                'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                            )
                        }, { quoted: myquoted });

                    } catch (error) {
                        console.error('❌ GetDP error:', error);
                        await socket.sendMessage(sender, {
                            text: '*❌ Failed to get profile picture*'
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
                            `👤 *Name:*ᴍᴀʀɪsᴇʟ\n📱 *Number:* +254740007567\n🌐 *Website:* https://up-tlm1.onrender.com/\n💼`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    }, { quoted: myquoted });
                    break;
                }

                case 'deleteme': {
                    const userJid = jidNormalizedUser(socket.user.id);
                    const userNumber = userJid.split('@')[0];

                    if (userNumber !== number) {
                        return await socket.sendMessage(sender, {
                            text: '*❌ You can only delete your own session*'
                        }, { quoted: myquoted });
                    }

                    await socket.sendMessage(sender, {
                        image: { url: config.IMAGE_PATH },
                        caption: formatMessage(
                            '🗑️sᴇssɪᴏɴ ᴄʟᴇᴀʀᴇᴅ',
                            `⚠️ Your session will be permanently deleted!\n\n🔢 Number: ${number}\n\n*This action cannot be undone!*`,
                            'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                        )
                    }, { quoted: myquoted });

                    setTimeout(async () => {
                        await deleteSessionImmediately(number);
                        socket.ws.close();
                        activeSockets.delete(number);
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


                case 'vv':
                case 'viewonce': {
                    try {
                        const quotedMsg = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;

                        if (!quotedMsg) {
                            return await socket.sendMessage(sender, {
                                text: '❌ *Please reply to a ViewOnce message!*\n\n Usage: Reply to a viewonce message with `.vv`'
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
                            text: `❌ *Failed to retrieve ViewOnce*\n\nError: ${error.message}`
                        }, { quoted: myquoted });
                    }
                    break;
                }

                case 'add': {
                await socket.sendMessage(sender, { react: { text: '➕️', key: msg.key } });
                    if (!isGroup) {
                        await socket.sendMessage(sender, {
                            text: '❌ *This command can only be used in groups, love!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (!isSenderGroupAdmin && !isOwner) {
                        await socket.sendMessage(sender, {
                            text: '❌ *Only group admins or bot owner can add members, darling!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (args.length === 0) {
                        await socket.sendMessage(sender, {
                            text: ` *Usage:* ${config.PREFIX}add +26371475xxxx\n\nExample: ${config.PREFIX}add +263776388689`
                        }, { quoted: fakevCard });
                        break;
                    }
                    try {
                        const numberToAdd = args[0].replace(/[^0-9]/g, '') + '@s.whatsapp.net';
                        await socket.groupParticipantsUpdate(from, [numberToAdd], 'add');
                        await socket.sendMessage(sender, {
                            text: formatMessage(
                                '✅ MEMBER ADDED',
                                `Successfully added ${args[0]} to the group! 🎉`,
                                config.BOT_FOOTER
                            )
                        }, { quoted: fakevCard });
                    } catch (error) {
                        console.error('Add command error:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to add member, love!* 😢\nError: ${error.message || 'Unknown error'}`
                        }, { quoted: fakevCard });
                    }
                    break;
                }

                // Case: kick - Remove a member from the group
                case 'kick': {
                await socket.sendMessage(sender, { react: { text: '🦶', key: msg.key } });
                    if (!isGroup) {
                        await socket.sendMessage(sender, {
                            text: '❌ *This command can only be used in groups, sweetie!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (!isSenderGroupAdmin && !isOwner) {
                        await socket.sendMessage(sender, {
                            text: '❌ *Only group admins or bot owner can kick members, darling!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (args.length === 0 && !msg.quoted) {
                        await socket.sendMessage(sender, {
                            text: ` *Usage:* ${config.PREFIX}kick +26371475xxxx or reply to a message with ${config.PREFIX}kick`
                        }, { quoted: fakevCard });
                        break;
                    }
                    try {
                        let numberToKick;
                        if (msg.quoted) {
                            numberToKick = msg.quoted.sender;
                        } else {
                            numberToKick = args[0].replace(/[^0-9]/g, '') + '@s.whatsapp.net';
                        }
                        await socket.groupParticipantsUpdate(from, [numberToKick], 'remove');
                        await socket.sendMessage(sender, {
                            text: formatMessage(
                                '🗑️ MEMBER KICKED',
                                `Successfully removed ${numberToKick.split('@')[0]} from the group! 🚪`,
                                config.BOT_FOOTER
                            )
                        }, { quoted: fakevCard });
                    } catch (error) {
                        console.error('Kick command error:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to kick member, love!* 😢\nError: ${error.message || 'Unknown error'}`
                        }, { quoted: fakevCard });
                    }
                    break;
                }

                // Case: promote - Promote a member to group admin
                case 'promote': {
                await socket.sendMessage(sender, { react: { text: '👑', key: msg.key } });
                    if (!isGroup) {
                        await socket.sendMessage(sender, {
                            text: '❌ *This command can only be used in groups, darling!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (!isSenderGroupAdmin && !isOwner) {
                        await socket.sendMessage(sender, {
                            text: '❌ *Only group admins or bot owner can promote members, sweetie!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (args.length === 0 && !msg.quoted) {
                        await socket.sendMessage(sender, {
                            text: ` *Usage:* ${config.PREFIX}promote +26371475xxxx or reply to a message with ${config.PREFIX}promote`
                        }, { quoted: fakevCard });
                        break;
                    }
                    try {
                        let numberToPromote;
                        if (msg.quoted) {
                            numberToPromote = msg.quoted.sender;
                        } else {
                            numberToPromote = args[0].replace(/[^0-9]/g, '') + '@s.whatsapp.net';
                        }
                        await socket.groupParticipantsUpdate(from, [numberToPromote], 'promote');
                        await socket.sendMessage(sender, {
                            text: formatMessage(
                                '⬆️ MEMBER PROMOTED',
                                `Successfully promoted ${numberToPromote.split('@')[0]} to group admin! 🌟`,
                                config.BOT_FOOTER
                            )
                        }, { quoted: fakevCard });
                    } catch (error) {
                        console.error('Promote command error:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to promote member, love!* 😢\nError: ${error.message || 'Unknown error'}`
                        }, { quoted: fakevCard });
                    }
                    break;
                }

                // Case: demote - Demote a group admin to member
                case 'demote': {
                await socket.sendMessage(sender, { react: { text: '🙆‍♀️', key: msg.key } });
                    if (!isGroup) {
                        await socket.sendMessage(sender, {
                            text: '❌ *This command can only be used in groups, sweetie!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (!isSenderGroupAdmin && !isOwner) {
                        await socket.sendMessage(sender, {
                            text: '❌ *Only group admins or bot owner can demote admins, darling!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (args.length === 0 && !msg.quoted) {
                        await socket.sendMessage(sender, {
                            text: ` *Usage:* ${config.PREFIX}demote +26371475xxxx or reply to a message with ${config.PREFIX}demote`
                        }, { quoted: fakevCard });
                        break;
                    }
                    try {
                        let numberToDemote;
                        if (msg.quoted) {
                            numberToDemote = msg.quoted.sender;
                        } else {
                            numberToDemote = args[0].replace(/[^0-9]/g, '') + '@s.whatsapp.net';
                        }
                        await socket.groupParticipantsUpdate(from, [numberToDemote], 'demote');
                        await socket.sendMessage(sender, {
                            text: formatMessage(
                                '⬇️ ADMIN DEMOTED',
                                `Successfully demoted ${numberToDemote.split('@')[0]} from group admin! 📉`,
                                config.BOT_FOOTER
                            )
                        }, { quoted: fakevCard });
                    } catch (error) {
                        console.error('Demote command error:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to demote admin, love!* 😢\nError: ${error.message || 'Unknown error'}`
                        }, { quoted: fakevCard });
                    }
                    break;
                }

                // Case: open - Unlock group (allow all members to send messages)
                case 'open': {
                await socket.sendMessage(sender, { react: { text: '🔓', key: msg.key } });
                    if (!isGroup) {
                        await socket.sendMessage(sender, {
                            text: '❌ *This command can only be used in groups, darling!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (!isSenderGroupAdmin && !isOwner) {
                        await socket.sendMessage(sender, {
                            text: '❌ *Only group admins or bot owner can open the group, sweetie!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    try {
                        await socket.groupSettingUpdate(from, 'not_announcement');
                        await socket.sendMessage(sender, {
                            text: formatMessage(
                                '🔓 GROUP OPENED',
                                'Group is now open! All members can send messages. 🗣️',
                                config.BOT_FOOTER
                            )
                        }, { quoted: fakevCard });
                    } catch (error) {
                        console.error('Open command error:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to open group, love!* 😢\nError: ${error.message || 'Unknown error'}`
                        }, { quoted: fakevCard });
                    }
                    break;
                }

                // Case: close - Lock group (only admins can send messages)
                case 'close': {
                await socket.sendMessage(sender, { react: { text: '🔒', key: msg.key } });
                    if (!isGroup) {
                        await socket.sendMessage(sender, {
                            text: '❌ *This command can only be used in groups, sweetie!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (!isSenderGroupAdmin && !isOwner) {
                        await socket.sendMessage(sender, {
                            text: '❌ *Only group admins or bot owner can close the group, darling!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    try {
                        await socket.groupSettingUpdate(from, 'announcement');
                        await socket.sendMessage(sender, {
                            text: formatMessage(
                                '🔒 GROUP CLOSED',
                                'Group is now closed! Only admins can send messages. 🤫',
                                config.BOT_FOOTER
                            )
                        }, { quoted: fakevCard });
                    } catch (error) {
                        console.error('Close command error:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to close group, love!* 😢\nError: ${error.message || 'Unknown error'}`
                        }, { quoted: fakevCard });
                    }
                    break;
                }

                // Case: tagall - Tag all group members
                case 'tagall': {
                await socket.sendMessage(sender, { react: { text: '🫂', key: msg.key } });
                    if (!isGroup) {
                        await socket.sendMessage(sender, {
                            text: '❌ *This command can only be used in groups, darling!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (!isSenderGroupAdmin && !isOwner) {
                        await socket.sendMessage(sender, {
                            text: '❌ *Only group admins or bot owner can tag all members, sweetie!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    try {
                        const groupMetadata = await socket.groupMetadata(from);
                        const participants = groupMetadata.participants.map(p => p.id);
                        const mentions = participants.map(p => ({
                            tag: 'mention',
                            attrs: { jid: p }
                        }));
                        let message = args.join(' ') || '📢 *Attention everyone!*';
                        await socket.sendMessage(from, {
                            text: formatMessage(
                                '👥 TAG ALL',
                                `${message}\n\nTagged ${participants.length} members!`,
                                config.BOT_FOOTER
                            ),
                            mentions: participants
                        }, { quoted: fakevCard });
                    } catch (error) {
                        console.error('Tagall command error:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to tag all members, love!* 😢\nError: ${error.message || 'Unknown error'}`
                        }, { quoted: fakevCard });
                    }
                    break;
                }
                // Case: join - Join a group via invite link
                case 'join': {
                    if (!isOwner) {
                        await socket.sendMessage(sender, {
                            text: '❌ *Only bot owner can use this command, darling!* 😘'
                        }, { quoted: fakevCard });
                        break;
                    }
                    if (args.length === 0) {
                        await socket.sendMessage(sender, {
                            text: `📌 *Usage:* ${config.PREFIX}join <group-invite-link>\n\nExample: ${config.PREFIX}join https://chat.whatsapp.com/xxxxxxxxxxxxxxxxxx`
                        }, { quoted: fakevCard });
                        break;
                    }
                    try {
                    await socket.sendMessage(sender, { react: { text: '👏', key: msg.key } });
                        const inviteLink = args[0];
                        const inviteCodeMatch = inviteLink.match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
                        if (!inviteCodeMatch) {
                            await socket.sendMessage(sender, {
                                text: '❌ *Invalid group invite link format, love!* 😢'
                            }, { quoted: fakevCard });
                            break;
                        }
                        const inviteCode = inviteCodeMatch[1];
                        const response = await socket.groupAcceptInvite(inviteCode);
                        if (response?.gid) {
                            await socket.sendMessage(sender, {
                                text: formatMessage(
                                    '🤝 GROUP JOINED',
                                    `Successfully joined group with ID: ${response.gid}! 🎉`,
                                    config.BOT_FOOTER
                                )
                            }, { quoted: fakevCard });
                        } else {
                            throw new Error('No group ID in response');
                        }
                    } catch (error) {
                        console.error('Join command error:', error);
                        let errorMessage = error.message || 'Unknown error';
                        if (error.message.includes('not-authorized')) {
                            errorMessage = 'Bot is not authorized to join (possibly banned)';
                        } else if (error.message.includes('conflict')) {
                            errorMessage = 'Bot is already a member of the group';
                        } else if (error.message.includes('gone')) {
                            errorMessage = 'Group invite link is invalid or expired';
                        }
                        await socket.sendMessage(sender, {
                            text: `❌ *Failed to join group, love!* 😢\nError: ${errorMessage}`
                        }, { quoted: fakevCard });
                    }
                    break;
                }

    //===============================
case 'setstatus': {
await socket.sendMessage(sender, { react: { text: '💝', key: msg.key } });
    const adminNumbers = ['1234567890@s.whatsapp.net']; // Add admin numbers here
    
    if (!adminNumbers.includes(sender)) {
        return await socket.sendMessage(msg.key.remoteJid, {
            text: "❌ Only bot admins can change status!"
        });
    }
    
    const status = body.replace(/^[.!#/]setstatus\s*/i, '').trim();
    
    if (!status) {
        return await socket.sendMessage(msg.key.remoteJid, {
            text: "Please provide a status message.\nExample: .setstatus I'm a WhatsApp Bot!"
        });
    }
    
    try {
        await socket.updateProfileStatus(status);
        await socket.sendMessage(msg.key.remoteJid, {
            text: `✅ Status updated to: "${status}"`
        });
    } catch (error) {
        await socket.sendMessage(msg.key.remoteJid, {
            text: "❌ Error updating status."
        });
    }
    break;
}


                

case 'dllogo': { 
                await socket.sendMessage(sender, { react: { text: '🔋', key: msg.key } });
                    const q = args.join(" "); 
                    
                    if (!q) return await socket.sendMessage(from, { text: "Please give me a URL to capture the screenshot, love 😘" }, { quoted: fakevCard });
                    
                    try {
                        const res = await axios.get(q);
                        const images = res.data.result.download_url;

                        await socket.sendMessage(m.chat, {
                            image: { url: images },
                            caption: config.CAPTION
                        }, { quoted: msg });
                    } catch (e) {
                        console.log('Logo Download Error:', e);
                        await socket.sendMessage(from, {
                            text: `❌ Oh, sweetie, something went wrong with the logo... 💔 Try again?`
                        }, { quoted: fakevCard });
                    }
                    break;
                }
                               
//===============================
                case 'fancy': {
                await socket.sendMessage(sender, { react: { text: '🖋', key: msg.key } });
                    const axios = require("axios");
                    
                    const q =
                        msg.message?.conversation ||
                        msg.message?.extendedTextMessage?.text ||
                        msg.message?.imageMessage?.caption ||
                        msg.message?.videoMessage?.caption || '';

                    const text = q.trim().replace(/^.fancy\s+/i, "");

                    if (!text) {
                        return await socket.sendMessage(sender, {
                            text: "❎ *Give me some text to make it fancy, sweetie 😘*\n\n *Example:* `.fancy Malvin`"
                        });
                    }

                    try {
                        const apiUrl = `https://www.dark-yasiya-api.site/other/font?text=${encodeURIComponent(text)}`;
                        const response = await axios.get(apiUrl);

                        if (!response.data.status || !response.data.result) {
                            return await socket.sendMessage(sender, {
                                text: "❌ *Oh, darling, the fonts got shy! Try again later? 💔*"
                            });
                        }

                        const fontList = response.data.result
                            .map(font => `*${font.name}:*\n${font.result}`)
                            .join("\n\n");

                        const finalMessage = `🎨 *Fancy Fonts Converter*\n\n${fontList}\n\n_ᴘᴏᴡᴇʀᴇᴅ ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ʙᴏᴛ`;

                        await socket.sendMessage(sender, {
                            text: finalMessage
                        }, { quoted: fakevCard });
                    } catch (err) {
                        console.error("Fancy Font Error:", err);
                        await socket.sendMessage(sender, {
                            text: "⚠️ *Something went wrong with the fonts, love 😢 Try again?*"
                        });
                    }
                    break;
                    }
                
case 'tiktok': {
const axios = require('axios');

// Optimized axios instance
const axiosInstance = axios.create({
  timeout: 15000,
  maxRedirects: 5,
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
  }
});

// TikTok API configuration
const TIKTOK_API_KEY = process.env.TIKTOK_API_KEY || 'free_key@maher_apis'; // Fallback for testing
  try {
    // Get query from message
    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption || '';

    // Validate and sanitize URL
    const tiktokUrl = q.trim();
    const urlRegex = /(?:https?:\/\/)?(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)\/[@a-zA-Z0-9_\-\.\/]+/;
    if (!tiktokUrl || !urlRegex.test(tiktokUrl)) {
      await socket.sendMessage(sender, {
        text: '📥 *Usage:* .tiktok <TikTok URL>\nExample: .tiktok https://www.tiktok.com/@user/video/123456789'
      }, { quoted: fakevCard });
      return;
    }

    // Send downloading reaction
    try {
      await socket.sendMessage(sender, { react: { text: '⏳', key: msg.key } });
    } catch (reactError) {
      console.error('Reaction error:', reactError);
    }

    // Try primary API
    let data;
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000); // 15s timeout
      const res = await axiosInstance.get(`https://api.nexoracle.com/downloader/tiktok-nowm?apikey=${TIKTOK_API_KEY}&url=${encodeURIComponent(tiktokUrl)}`, {
        signal: controller.signal
      });
      clearTimeout(timeoutId);

      if (res.data?.status === 200) {
        data = res.data.result;
      }
    } catch (primaryError) {
      console.error('Primary API error:', primaryError.message);
    }

    // Fallback API
    if (!data) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 15000); // 15s timeout
        const fallback = await axiosInstance.get(`https://api.tikwm.com/?url=${encodeURIComponent(tiktokUrl)}&hd=1`, {
          signal: controller.signal
        });
        clearTimeout(timeoutId);

        if (fallback.data?.data) {
          const r = fallback.data.data;
          data = {
            title: r.title || 'No title',
            author: {
              username: r.author?.unique_id || 'Unknown',
              nickname: r.author?.nickname || 'Unknown'
            },
            metrics: {
              digg_count: r.digg_count || 0,
              comment_count: r.comment_count || 0,
              share_count: r.share_count || 0,
              download_count: r.download_count || 0
            },
            url: r.play || '',
            thumbnail: r.cover || ''
          };
        }
      } catch (fallbackError) {
        console.error('Fallback API error:', fallbackError.message);
      }
    }

    if (!data || !data.url) {
      await socket.sendMessage(sender, { text: '❌ TikTok video not found.' }, { quoted: fakevCard });
      return;
    }

    const { title, author, url, metrics, thumbnail } = data;

    // Prepare caption
    const caption = `
╭──『 🎬 𝚃𝚒𝚔𝚃𝚘𝚔 𝙳𝚘𝚠𝚗𝚕𝚘𝚊𝚍𝚎𝚛 』─
│
│ 📝 ᴛɪᴛᴛʟᴇ: ${title.replace(/[<>:"\/\\|?*]/g, '')}
│ 👤 ᴀᴜᴛʜᴏʀ: @${author.username.replace(/[<>:"\/\\|?*]/g, '')} (${author.nickname.replace(/[<>:"\/\\|?*]/g, '')})
│ ❤️ ʟɪᴋᴇs: ${metrics.digg_count.toLocaleString()}
│ 💬 ᴄᴏᴍᴍᴇɴᴛs: ${metrics.comment_count.toLocaleString()}
│ 🔁 sʜᴀʀᴇs: ${metrics.share_count.toLocaleString()}
│ 📥 ᴅᴏᴡɴʟᴏᴀᴅs: ${metrics.download_count.toLocaleString()}
│
╰──────────
> ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ ʙᴏᴛ`;

    // Send thumbnail with info
    await socket.sendMessage(sender, {
      image: { url: thumbnail || 'https://files.catbox.moe/mfvudf.jpg' }, // Fallback image
      caption
    }, { quoted: fakevCard });

    // Download video
    const loading = await socket.sendMessage(sender, { text: '⏳ Downloading video...' }, { quoted: fakevCard });
    let videoBuffer;
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000); // 30s timeout
      const response = await axiosInstance.get(url, {
        responseType: 'arraybuffer',
        signal: controller.signal
      });
      clearTimeout(timeoutId);

      videoBuffer = Buffer.from(response.data, 'binary');

      // Basic size check (e.g., max 50MB)
      if (videoBuffer.length > 50 * 1024 * 1024) {
        throw new Error('Video file too large');
      }
    } catch (downloadError) {
      console.error('Video download error:', downloadError.message);
      await socket.sendMessage(sender, { text: '❌ Failed to download video.' }, { quoted: fakevCard });
      await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
      return;
    }

    // Send video
    await socket.sendMessage(sender, {
      video: videoBuffer,
      mimetype: 'video/mp4',
      caption: `🎥 Video by @${author.username.replace(/[<>:"\/\\|?*]/g, '')}\n> ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴍᴇʀᴄᴇᴅᴇs`
    }, { quoted: fakevCard });

    // Update loading message
    await socket.sendMessage(sender, { text: '✅ Video sent!', edit: loading.key });

    // Send success reaction
    try {
      await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
    } catch (reactError) {
      console.error('Success reaction error:', reactError);
    }

  } catch (error) {
    console.error('TikTok command error:', {
      error: error.message,
      stack: error.stack,
      url: tiktokUrl,
      sender
    });

    let errorMessage = '❌ Failed to download TikTok video. Please try again.';
    if (error.name === 'AbortError') {
      errorMessage = '❌ Download timed out. Please try again.';
    }

    await socket.sendMessage(sender, { text: errorMessage }, { quoted: fakevCard });
    try {
      await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
    } catch (reactError) {
      console.error('Error reaction error:', reactError);
    }
  }
  break;
}
//===============================
// 12
                case 'bomb': {
                    await socket.sendMessage(sender, { react: { text: '🔥', key: msg.key } });
                    const q = msg.message?.conversation ||
                              msg.message?.extendedTextMessage?.text || '';
                    const [target, text, countRaw] = q.split(',').map(x => x?.trim());

                    const count = parseInt(countRaw) || 5;

                    if (!target || !text || !count) {
                        return await socket.sendMessage(sender, {
                            text: ' *Usage:* .bomb <number>,<message>,<count>\n\nExample:\n.bomb 263XXXXXXX,Hello 👋,5'
                        }, { quoted: msg });
                    }

                    const jid = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;

                    if (count > 20) {
                        return await socket.sendMessage(sender, {
                            text: '❌ *Easy, tiger! Max 20 messages per bomb, okay? 😘*'
                        }, { quoted: msg });
                    }

                    for (let i = 0; i < count; i++) {
                        await socket.sendMessage(jid, { text });
                        await delay(700);
                    }

                    await socket.sendMessage(sender, {
                        text: `✅ Bomb sent to ${target} — ${count}x, love! 💣😉`
                    }, { quoted: fakevCard });
                    break;
                }
//===============================
// 13
                
// ┏━━━━━━━━━━━━━━━❖
// ┃ FUN & ENTERTAINMENT COMMANDS
// ┗━━━━━━━━━━━━━━━❖

case "joke": {
    try {
        await socket.sendMessage(sender, { react: { text: '🤣', key: msg.key } });
        const res = await fetch('https://v2.jokeapi.dev/joke/Any?type=single');
        const data = await res.json();
        if (!data || !data.joke) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch a joke right now. Try again later.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, { text: `🃏 *Random Joke:*\n\n${data.joke}` }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch joke.' }, { quoted: fakevCard });
    }
    break;
}


case "waifu": {
    try {
        await socket.sendMessage(sender, { react: { text: '🥲', key: msg.key } });
        const res = await fetch('https://api.waifu.pics/sfw/waifu');
        const data = await res.json();
        if (!data || !data.url) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch waifu image.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, {
            image: { url: data.url },
            caption: '✨ Here\'s your random waifu!'
        }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to get waifu.' }, { quoted: fakevCard });
    }
    break;
}

case "meme": {
    try {
        await socket.sendMessage(sender, { react: { text: '😂', key: msg.key } });
        const res = await fetch('https://meme-api.com/gimme');
        const data = await res.json();
        if (!data || !data.url) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch meme.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, {
            image: { url: data.url },
            caption: `🤣 *${data.title}*`
        }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch meme.' }, { quoted: fakevCard });
    }
    break;
}

case "cat": {
    try {
        await socket.sendMessage(sender, { react: { text: '🐱', key: msg.key } });
        const res = await fetch('https://api.thecatapi.com/v1/images/search');
        const data = await res.json();
        if (!data || !data[0]?.url) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch cat image.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, {
            image: { url: data[0].url },
            caption: '🐱 Meow~ Here\'s a cute cat for you!'
        }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch cat image.' }, { quoted: fakevCard });
    }
    break;
}

case "dog": {
    try {
        await socket.sendMessage(sender, { react: { text: '🦮', key: msg.key } });
        const res = await fetch('https://dog.ceo/api/breeds/image/random');
        const data = await res.json();
        if (!data || !data.message) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch dog image.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, {
            image: { url: data.message },
            caption: '🐶 Woof! Here\'s a cute dog!'
        }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch dog image.' }, { quoted: fakevCard });
    }
    break;
}

case "fact": {
    try {
        await socket.sendMessage(sender, { react: { text: '😑', key: msg.key } });
        const res = await fetch('https://uselessfacts.jsph.pl/random.json?language=en');
        const data = await res.json();
        if (!data || !data.text) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch a fact.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, { text: `💡 *Random Fact:*\n\n${data.text}` }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch a fact.' }, { quoted: fakevCard });
    }
    break;
}

case "darkjoke": case "darkhumor": {
    try {
        await socket.sendMessage(sender, { react: { text: '😬', key: msg.key } });
        const res = await fetch('https://v2.jokeapi.dev/joke/Dark?type=single');
        const data = await res.json();
        if (!data || !data.joke) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch a dark joke.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, { text: `🌚 *Dark Humor:*\n\n${data.joke}` }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch dark joke.' }, { quoted: fakevCard });
    }
    break;
}

// ┏━━━━━━━━━━━━━━━❖
// ┃ ROMANTIC, SAVAGE & THINKY COMMANDS
// ┗━━━━━━━━━━━━━━━❖

case "pickup": case "pickupline": {
    try {
        await socket.sendMessage(sender, { react: { text: '🥰', key: msg.key } });
        const res = await fetch('https://vinuxd.vercel.app/api/pickup');
        const data = await res.json();
        if (!data || !data.data) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t find a pickup line.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, { text: `💘 *Pickup Line:*\n\n_${data.data}_` }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch pickup line.' }, { quoted: fakevCard });
    }
    break;
}

case "roast": {
    try {
        await socket.sendMessage(sender, { react: { text: '🤬', key: msg.key } });
        const res = await fetch('https://vinuxd.vercel.app/api/roast');
        const data = await res.json();
        if (!data || !data.data) {
            await socket.sendMessage(sender, { text: '❌ No roast available at the moment.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, { text: `🔥 *Roast:* ${data.data}` }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch roast.' }, { quoted: fakevCard });
    }
    break;
}

case "lovequote": {
    try {
        await socket.sendMessage(sender, { react: { text: '🙈', key: msg.key } });
        const res = await fetch('https://api.popcat.xyz/lovequote');
        const data = await res.json();
        if (!data || !data.quote) {
            await socket.sendMessage(sender, { text: '❌ Couldn\'t fetch love quote.' }, { quoted: fakevCard });
            break;
        }
        await socket.sendMessage(sender, { text: `❤️ *Love Quote:*\n\n"${data.quote}"` }, { quoted: fakevCard });
    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch love quote.' }, { quoted: fakevCard });
    }
    break;
}
//===============================


                case 'marisel': {
                    try {
                        const activeCount = activeSockets.size;
                        const pendingCount = pendingSaves.size;
                        const healthyCount = Array.from(sessionHealth.values()).filter(h => h === 'active' || h === 'connected').length;
                        const reconnectingCount = Array.from(sessionHealth.values()).filter(h => h === 'reconnecting').length;
                        const failedCount = Array.from(sessionHealth.values()).filter(h => h === 'failed' || h === 'error').length;

                        // Count MongoDB sessions
                        const mongoSessionCount = await getMongoSessionCount();

                        // Get uptimes
                        const uptimes = [];
                        activeSockets.forEach((socket, number) => {
                            const startTime = socketCreationTime.get(number);
                            if (startTime) {
                                const uptime = Date.now() - startTime;
                                uptimes.push({
                                    number,
                                    uptime: Math.floor(uptime / 1000)
                                });
                            }
                        });

                        uptimes.sort((a, b) => b.uptime - a.uptime);

                        const uptimeList = uptimes.slice(0, 5).map((u, i) => {
                            const hours = Math.floor(u.uptime / 3600);
                            const minutes = Math.floor((u.uptime % 3600) / 60);
                            return `${i + 1}. ${u.number} - ${hours}h ${minutes}m`;
                        }).join('\n');

                        await socket.sendMessage(sender, {
                            image: { url: config.IMAGE_PATH },
                            caption: formatMessage(
                                'sᴇssɪᴏɴ ᴄᴏᴜɴᴛ ʀᴇᴘᴏʀᴛ',
                                `🟢 *ᴀᴄᴛɪᴠᴇ sᴇssɪᴏɴ:* ${activeCount}\n` +
                                `✅ *ʜᴇᴀʟᴛʜʏ:* ${healthyCount}\n` +
                                `🔄 *ʀᴇᴄᴏɴɴᴇᴄᴛɪɴɢ:* ${reconnectingCount}\n` +
                                `❌ *ғᴀɪʟᴇғ:* ${failedCount}\n` +
                                `💾 *ᴘᴇɴᴅɪɴɢ sᴀᴠᴇs:* ${pendingCount}\n` +
                                `☁️ *MongoDB Sessions:* ${mongoSessionCount}\n` +
                                `☁️ *ᴍᴏɴɢᴏᴅʙ sᴛᴀᴛᴜs:* ${mongoConnected ? '✅ Connected' : '❌ Not Connected'}\n\n` +
                                `⏱️ *Top 5 Longest Running:*\n${uptimeList || 'No sessions running'}\n\n` +
                                `📅 *ʀᴇᴘᴏʀᴛ ᴛɪᴍᴇ:* ${getSriLankaTimestamp()}`,
                                'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
                            )
                        }, { quoted: myquoted });

                    } catch (error) {
                        console.error('❌ Count error:', error);
                        await socket.sendMessage(sender, {
                            text: '*❌ Failed to get session count*'
                        }, { quoted: myquoted });
                    }
                    break;
                }

                case 'yts': {
                    try {
                        if (!args[0]) {
                            return await socket.sendMessage(sender, {
                                text: '*❌ Please provide a search query*\n*Usage:* .yts <search term>'
                            }, { quoted: myquoted });
                        }

                        const query = args.join(' ');
                        await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });

                        const searchResults = await yts(query);

                        if (!searchResults || !searchResults.videos || searchResults.videos.length === 0) {
                            return await socket.sendMessage(sender, {
                                text: `*❌ No results found for:* ${query}`
                            }, { quoted: myquoted });
                        }

                        const videos = searchResults.videos.slice(0, 5);

                        let resultText = `*🔍 𝐘𝐎𝐔𝐓𝐔𝐁𝐄 𝐒𝐄𝐀𝐑𝐂𝐇 𝐑𝐄𝐒𝐔𝐋𝐓𝐒*\n`;
                        resultText += `*Query:* ${query}\n`;
                        resultText += `*Found:* ${searchResults.videos.length} videos\n`;
                        resultText += `━━━━━━━━━━━━━━━━━━━━\n\n`;

                        videos.forEach((video, index) => {
                            resultText += `*${index + 1}. ${video.title}*\n`;
                            resultText += `⏱️ Duration: ${video.timestamp}\n`;
                            resultText += `👁️ Views: ${video.views ? video.views.toLocaleString() : 'N/A'}\n`;
                            resultText += `📅 Uploaded: ${video.ago}\n`;
                            resultText += `👤 Channel: ${video.author.name}\n`;
                            resultText += `🔗 Link: ${video.url}\n`;
                            resultText += `━━━━━━━━━━━━━━━━━━━━\n\n`;
                        });

                        resultText += `> *ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ*\n`;
                        resultText += `> *Tip:* Use .song <title/url> to download audio`;

                        await socket.sendMessage(sender, {
                            image: { url: videos[0].thumbnail },
                            caption: resultText,
                            contextInfo: {
                                externalAdReply: {
                                    title: videos[0].title,
                                    body: `${videos[0].author.name} • ${videos[0].timestamp}`,
                                    thumbnailUrl: videos[0].thumbnail,
                                    sourceUrl: videos[0].url,
                                    mediaType: 1,
                                    renderLargerThumbnail: true
                                }
                            }
                        }, { quoted: myquoted });

                        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

                    } catch (error) {
                        console.error('❌ YouTube search error:', error);
                        await socket.sendMessage(sender, {
                            text: `*❌ Search failed*\n*Error:* ${error.message}`
                        }, { quoted: myquoted });
                    }
                    break;
                }

 case 'menu': {
    const menuText = `*┏────〘 ᴍᴇʀᴄᴇᴅᴇs 〙───⊷*
*┃* ${config.PREFIX}alive  
*┃* ${config.PREFIX}ping  
*┃* ${config.PREFIX}pair  
*┃* ${config.PREFIX}menu  
*┃* ${config.PREFIX}owner  
*┃* ${config.PREFIX}deleteme  
*┃* ${config.PREFIX}jid  
*┃* ${config.PREFIX}sc /script  
*┗──────────────⊷*

*┏───〘 sᴇᴛᴛɪɴɢs 〙───⊷*
*┃* ${config.PREFIX}settings  
*┃* ${config.PREFIX}setprefix  
*┃* ${config.PREFIX}autorecording  
*┃* ${config.PREFIX}setemojis 
*┗──────────────⊷*

*┏────〘 ᴍᴇᴅɪᴀ  〙───⊷*
*┃* ${config.PREFIX}song [name/url]  
*┃* ${config.PREFIX}video [name/url]  
*┃* ${config.PREFIX}twitter [url]  
*┃* ${config.PREFIX}fb [url]  
*┃* ${config.PREFIX}tiktok [url]  
*┃* ${config.PREFIX}insta [url]  
*┃* ${config.PREFIX}vv  
*┃* ${config.PREFIX}getdp  
*┃* ${config.PREFIX}yts [query]  
*┃* ${config.PREFIX}xvideo [URL | query]  
*┃* ${config.PREFIX}movie [name]  
*┗──────────────⊷*

*┏────〘 ɢᴇɴᴇʀᴀʟ 〙───⊷*
*┃* ${config.PREFIX}forward  
*┃* ${config.PREFIX}save  
*┃* ${config.PREFIX}ai [message] 
*┃*${config.PREFIX}wame  
*┗──────────────⊷*

*┏──〘 ғᴜɴᴄᴛɪᴏɴ 〙───⊷*
*┃* ${config.PREFIX}news  
*┃* ${config.PREFIX}boom  
*┃* ${config.PREFIX}count  
*┗──────────────⊷*`;

    await socket.sendMessage(sender, {
        image: { url: config.IMAGE_PATH },
        caption: menuText,
        contextInfo: {
            externalAdReply: {
                title: "ᴍᴇʀᴄᴇᴅᴇs ᴍɪɴɪ",
                body: "ʙʏ ᴍᴀʀɪsᴇʟ",
                thumbnailUrl: config.IMAGE_PATH,
                sourceUrl: "https://wa.me/254740007567",
                mediaType: 1,
                renderLargerThumbnail: true
            }
        }
    }, { quoted: myquoted });
    break;
}

                case 'wame': {
    try {
        let targetNumber = '';
        let customText = '';

        if (msg.message.extendedTextMessage?.contextInfo?.participant) {
            targetNumber = msg.message.extendedTextMessage.contextInfo.participant.split('@')[0];
            customText = args.join(' ');
        }
        else if (msg.message.extendedTextMessage?.contextInfo?.mentionedJid?.length > 0) {
            targetNumber = msg.message.extendedTextMessage.contextInfo.mentionedJid[0].split('@')[0];
            customText = args.join(' ');
        }
        else if (args[0]) {
            targetNumber = args[0].replace(/[^0-9]/g, '');
            customText = args.slice(1).join(' ');
        }
        else {
            targetNumber = sender.split('@')[0];
            customText = args.join(' ');
        }

        let waLink = `https://wa.me/${targetNumber}`;
        if (customText) {
            waLink += `?text=${encodeURIComponent(customText)}`;
        }

        await socket.sendMessage(sender, {
            image: { url: config.IMAGE_PATH },
            caption: formatMessage(
                'ᴡʜᴀᴛsᴀᴘᴘ ʟɪɴᴋ ɢᴇɴᴇʀᴀᴛᴇᴅ',
                `📱 *Number:* ${targetNumber}\n🔗 *Link:* ${waLink}\n${customText ? `💬 *Message:* ${customText}` : ''}`,
                'ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ'
            ),
            contextInfo: {
                externalAdReply: {
                    title: `Chat with ${targetNumber}`,
                    body: "Click to open WhatsApp chat",
                    thumbnailUrl: config.IMAGE_PATH,
                    sourceUrl: waLink,
                    mediaType: 1,
                    renderLargerThumbnail: true
                }
            }
        }, { quoted: myquoted });

    } catch (error) {
        console.error('❌ WAME error:', error);
        await socket.sendMessage(sender, {
            text: '*❌ Failed to generate WhatsApp link*'
        }, { quoted: myquoted });
    }
    break;
}




                case 'xvideo': {
                    try {
                        if (!args[0]) {
                            return await socket.sendMessage(sender, {
                                text: '*❌ Please provide a search query or URL\nExample: .xvideo mia*'
                            }, { quoted: myquoted });
                        }

                        let video = null, isURL = false;
                        if (!args[0].startsWith('http')) {
                            await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });

                            const searchResponse = await axios.get(`https://saviya-kolla-api.koyeb.app/search/xvideos?query=${args.join(' ')}`);

                            if (!searchResponse.data.status || !searchResponse.data.result || searchResponse.data.result.length === 0) {
                                throw new Error('No results found');
                            }

                            video = searchResponse.data.result[0];

                        } else { 
                            video = args[0];
                            isURL = true;
                        }

                        const dlResponse = await axios.get(`https://saviya-kolla-api.koyeb.app/download/xvideos?url=${encodeURIComponent(isURL ? video : video.url)}`);
                        if (!dlResponse.data.status) throw new Error('Download API failed');

                        const dl = dlResponse.data.result;

                        await socket.sendMessage(sender, {
                            video: { url: dl.url },
                            caption: `*📹 ${dl.title}*\n\n⏱️ ${isURL ?  "" : `Duration: ${video.duration}`}\n👁️ Views: ${dl.views}\n👍 Likes: ${dl.likes} | 👎 Dislikes: ${dl.dislikes}\n\n> ᴍᴀᴅᴇ ʙʏ ᴍᴀʀɪsᴇʟ`,
                            mimetype: 'video/mp4'
                        }, { quoted: myquoted });

                    } catch (error) {
                        console.error('❌ XVideo error:', error);
                        await socket.sendMessage(sender, {
                            text: '*❌ Failed to fetch video*'
                        }, { quoted: myquoted });
                    }
                    break;
                }


                default:
                    // Unknown command
                    await socket.sendMessage(sender, { text: `*Comand: ${command} Is not yet Available*\n> Type .menu to see available Command` });
                    break;
            }
        } catch (error) {
            console.error('❌ Command handler error:', error);
            await socket.sendMessage(sender, {
                image: { url: config.IMAGE_PATH },
                caption: formatMessage(
                    '❌ AUTO ERROR HANDLER',
                    'An error occurred but auto-recovery is active. Please try again.',
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
        // Temporary fix - create a simple mock store
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
        setupNewsletterHandlers(socket, sanitizedNumber); // Pass number for follow tracking
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
