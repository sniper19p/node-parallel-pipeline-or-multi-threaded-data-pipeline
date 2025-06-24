/**
 * Logger Module
 * Provides centralized logging functionality for the pipeline
 */

const pino = require('pino');
const path = require('path');
const fs = require('fs-extra');

// Ensure logs directory exists
const logsDir = path.join(process.cwd(), 'logs');
fs.ensureDirSync(logsDir);

// Define log file path
const logFile = path.join(logsDir, `pipeline-${new Date().toISOString().replace(/:/g, '-')}.log`);

// Create logger instance with transport configuration
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  timestamp: pino.stdTimeFunctions.isoTime,
  transport: {
    targets: [
      // Console output with pretty formatting
      {
        target: 'pino-pretty',
        level: process.env.CONSOLE_LOG_LEVEL || 'info',
        options: {
          colorize: true,
          translateTime: 'SYS:standard',
          ignore: 'pid,hostname'
        }
      },
      // File output with full detail
      {
        target: 'pino/file',
        level: process.env.FILE_LOG_LEVEL || 'debug',
        options: { destination: logFile }
      }
    ]
  }
});

// Export the logger
module.exports = logger;

// Log startup message
logger.info({
  message: 'Logger initialized',
  logFile,
  nodeVersion: process.version,
  platform: process.platform
});

// Handle uncaught exceptions and unhandled rejections
process.on('uncaughtException', (err) => {
  logger.fatal({
    message: 'Uncaught exception',
    error: err.message,
    stack: err.stack
  });
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error({
    message: 'Unhandled rejection',
    reason: reason?.message || String(reason),
    stack: reason?.stack
  });
});

// Clean up old logs on startup - keep last 10 log files
fs.readdir(logsDir)
  .then(files => {
    const logFiles = files
      .filter(file => file.startsWith('pipeline-') && file.endsWith('.log'))
      .map(file => ({ name: file, time: fs.statSync(path.join(logsDir, file)).mtime.getTime() }))
      .sort((a, b) => b.time - a.time);
      
    // Delete older logs (keep latest 10)
    if (logFiles.length > 10) {
      const filesToDelete = logFiles.slice(10);
      filesToDelete.forEach(file => {
        fs.unlink(path.join(logsDir, file.name))
          .catch(err => console.error(`Failed to delete old log file: ${file.name}`, err));
      });
    }
  })
  .catch(err => {
    console.error('Error cleaning up old log files:', err);
  });
