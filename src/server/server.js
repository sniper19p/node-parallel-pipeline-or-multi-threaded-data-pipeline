/**
 * Real-time Dashboard Server
 * Provides a web interface for monitoring the data processing pipeline
 */

const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');
const fs = require('fs-extra');
const logger = require('../lib/logger');

class DashboardServer {
  constructor(monitor, port = 3000) {
    this.monitor = monitor;
    this.port = port;
    this.app = express();
    this.server = http.createServer(this.app);
    this.io = socketIo(this.server, {
      cors: {
        origin: "*",
        methods: ["GET", "POST"]
      }
    });
    
    this.connectedClients = new Set();
    this.metricsHistory = [];
    this.maxHistorySize = 1000;
    
    this.setupMiddleware();
    this.setupRoutes();
    this.setupSocketHandlers();
    this.setupMonitorListeners();
  }

  /**
   * Setup Express middleware
   */
  setupMiddleware() {
    // Serve static files from public directory
    this.app.use(express.static(path.join(__dirname, 'public')));
    
    // Parse JSON bodies
    this.app.use(express.json());
    
    // CORS headers
    this.app.use((req, res, next) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
      next();
    });
    
    // Request logging
    this.app.use((req, res, next) => {
      logger.debug(`${req.method} ${req.url}`, { 
        ip: req.ip, 
        userAgent: req.get('User-Agent') 
      });
      next();
    });
  }

  /**
   * Setup Express routes
   */
  setupRoutes() {
    // Main dashboard page
    this.app.get('/', (req, res) => {
      res.sendFile(path.join(__dirname, 'public', 'index.html'));
    });
    
    // API endpoint for current metrics
    this.app.get('/api/metrics', (req, res) => {
      try {
        const metrics = this.monitor.getMetrics();
        res.json({
          success: true,
          data: metrics,
          timestamp: Date.now()
        });
      } catch (error) {
        logger.error('Error getting metrics:', error);
        res.status(500).json({
          success: false,
          error: error.message
        });
      }
    });
    
    // API endpoint for metrics history
    this.app.get('/api/metrics/history', (req, res) => {
      try {
        const limit = parseInt(req.query.limit) || 100;
        const history = this.metricsHistory.slice(-limit);
        
        res.json({
          success: true,
          data: history,
          count: history.length
        });
      } catch (error) {
        logger.error('Error getting metrics history:', error);
        res.status(500).json({
          success: false,
          error: error.message
        });
      }
    });
    
    // API endpoint for worker status
    this.app.get('/api/workers', (req, res) => {
      try {
        const workerStatus = this.monitor.getWorkerStatus('all');
        const workers = Array.from(workerStatus.entries()).map(([id, status]) => ({
          id,
          ...status
        }));
        
        res.json({
          success: true,
          data: workers,
          count: workers.length
        });
      } catch (error) {
        logger.error('Error getting worker status:', error);
        res.status(500).json({
          success: false,
          error: error.message
        });
      }
    });
    
    // API endpoint for pipeline control (future enhancement)
    this.app.post('/api/control/:action', (req, res) => {
      const { action } = req.params;
      
      // Placeholder for pipeline control actions
      logger.info(`Control action requested: ${action}`);
      
      res.json({
        success: true,
        message: `Action ${action} acknowledged`,
        timestamp: Date.now()
      });
    });
    
    // Health check endpoint
    this.app.get('/health', (req, res) => {
      res.json({
        status: 'healthy',
        uptime: process.uptime(),
        timestamp: Date.now(),
        version: require('../../package.json').version
      });
    });
    
    // 404 handler
    this.app.use((req, res) => {
      res.status(404).json({
        success: false,
        error: 'Not found'
      });
    });
    
    // Error handler
    this.app.use((error, req, res, next) => {
      logger.error('Server error:', error);
      res.status(500).json({
        success: false,
        error: 'Internal server error'
      });
    });
  }

  /**
   * Setup Socket.IO event handlers
   */
  setupSocketHandlers() {
    this.io.on('connection', (socket) => {
      this.connectedClients.add(socket);
      logger.info(`Client connected: ${socket.id}. Total clients: ${this.connectedClients.size}`);
      
      // Send current metrics to new client
      try {
        const currentMetrics = this.monitor.getMetrics();
        socket.emit('metrics_update', currentMetrics);
        
        // Send recent history
        const recentHistory = this.metricsHistory.slice(-50);
        socket.emit('metrics_history', recentHistory);
      } catch (error) {
        logger.error('Error sending initial data to client:', error);
      }
      
      // Handle client requests
      socket.on('request_metrics', () => {
        try {
          const metrics = this.monitor.getMetrics();
          socket.emit('metrics_update', metrics);
        } catch (error) {
          logger.error('Error sending metrics to client:', error);
        }
      });
      
      socket.on('request_workers', () => {
        try {
          const workerStatus = this.monitor.getWorkerStatus('all');
          const workers = Array.from(workerStatus.entries()).map(([id, status]) => ({
            id,
            ...status
          }));
          socket.emit('workers_update', workers);
        } catch (error) {
          logger.error('Error sending worker status to client:', error);
        }
      });
      
      // Handle disconnection
      socket.on('disconnect', () => {
        this.connectedClients.delete(socket);
        logger.info(`Client disconnected: ${socket.id}. Total clients: ${this.connectedClients.size}`);
      });
    });
  }

  /**
   * Setup monitor event listeners
   */
  setupMonitorListeners() {
    // Listen for metrics updates
    this.monitor.on('metrics_updated', (metrics) => {
      // Store in history
      this.metricsHistory.push({
        ...metrics,
        timestamp: Date.now()
      });
      
      // Limit history size
      if (this.metricsHistory.length > this.maxHistorySize) {
        this.metricsHistory.shift();
      }
      
      // Broadcast to all connected clients
      this.io.emit('metrics_update', metrics);
    });
    
    // Listen for status changes
    this.monitor.on('status_change', (statusData) => {
      logger.info(`Pipeline status changed: ${statusData.prevStatus} -> ${statusData.newStatus}`);
      this.io.emit('status_change', statusData);
    });
    
    // Listen for worker status updates
    this.monitor.on('worker_status', (workerData) => {
      this.io.emit('worker_update', workerData);
    });
    
    // Listen for pipeline completion
    this.monitor.on('pipeline_complete', (completionData) => {
      logger.info('Pipeline completed:', completionData);
      this.io.emit('pipeline_complete', completionData);
    });
  }

  /**
   * Start the server
   * @returns {Promise<void>}
   */
  async start() {
    return new Promise((resolve, reject) => {
      try {
        // Ensure public directory exists and create basic HTML if needed
        this.ensurePublicFiles();
        
        this.server.listen(this.port, () => {
          logger.info(`Dashboard server started on port ${this.port}`);
          logger.info(`Dashboard URL: http://localhost:${this.port}`);
          resolve();
        });
        
        this.server.on('error', (error) => {
          logger.error('Server error:', error);
          reject(error);
        });
        
      } catch (error) {
        logger.error('Error starting server:', error);
        reject(error);
      }
    });
  }

  /**
   * Stop the server
   * @returns {Promise<void>}
   */
  async stop() {
    return new Promise((resolve) => {
      this.server.close(() => {
        logger.info('Dashboard server stopped');
        resolve();
      });
    });
  }

  /**
   * Ensure public files exist
   */
  ensurePublicFiles() {
    const publicDir = path.join(__dirname, 'public');
    fs.ensureDirSync(publicDir);
    
    const indexPath = path.join(publicDir, 'index.html');
    
    if (!fs.existsSync(indexPath)) {
      const basicHtml = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .card { background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; }
        .metric { text-align: center; }
        .metric-value { font-size: 2em; font-weight: bold; color: #2196F3; }
        .metric-label { color: #666; margin-top: 5px; }
        .status { padding: 5px 15px; border-radius: 20px; color: white; font-weight: bold; }
        .status.running { background: #4CAF50; }
        .status.completed { background: #2196F3; }
        .status.failed { background: #f44336; }
        .workers { margin-top: 20px; }
        .worker { display: inline-block; margin: 5px; padding: 10px; background: #e3f2fd; border-radius: 5px; }
        #log { height: 200px; overflow-y: auto; background: #000; color: #0f0; padding: 10px; font-family: monospace; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Multi-Threaded Pipeline Dashboard</h1>
        
        <div class="card">
            <h2>Pipeline Status: <span id="status" class="status">initializing</span></h2>
            <p>Elapsed Time: <span id="elapsed">0s</span></p>
        </div>
        
        <div class="card">
            <h2>Performance Metrics</h2>
            <div class="metrics-grid">
                <div class="metric">
                    <div class="metric-value" id="throughput">0</div>
                    <div class="metric-label">Records/sec</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="processed">0</div>
                    <div class="metric-label">Processed</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="memory">0</div>
                    <div class="metric-label">Memory (MB)</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="cpu">0</div>
                    <div class="metric-label">CPU %</div>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h2>Workers</h2>
            <div id="workers" class="workers">No workers active</div>
        </div>
        
        <div class="card">
            <h2>Activity Log</h2>
            <div id="log"></div>
        </div>
    </div>

    <script src="/socket.io/socket.io.js"></script>
    <script>
        const socket = io();
        
        function updateMetrics(metrics) {
            document.getElementById('status').textContent = metrics.status;
            document.getElementById('status').className = 'status ' + metrics.status;
            document.getElementById('elapsed').textContent = Math.round(metrics.elapsedTime / 1000) + 's';
            document.getElementById('throughput').textContent = metrics.throughput || 0;
            document.getElementById('processed').textContent = metrics.recordsProcessed || 0;
            document.getElementById('memory').textContent = metrics.currentMemoryUsage || 0;
            document.getElementById('cpu').textContent = metrics.cpuUsage || 0;
        }
        
        function updateWorkers(workers) {
            const container = document.getElementById('workers');
            if (!workers || workers.length === 0) {
                container.innerHTML = 'No workers active';
                return;
            }
            
            container.innerHTML = workers.map(worker => 
                '<div class="worker">Worker ' + worker.id + ': ' + (worker.status || 'unknown') + '</div>'
            ).join('');
        }
        
        function addLog(message) {
            const log = document.getElementById('log');
            const time = new Date().toLocaleTimeString();
            log.innerHTML += '<div>[' + time + '] ' + message + '</div>';
            log.scrollTop = log.scrollHeight;
        }
        
        socket.on('metrics_update', updateMetrics);
        socket.on('workers_update', updateWorkers);
        socket.on('status_change', (data) => {
            addLog('Status changed: ' + data.prevStatus + ' → ' + data.newStatus);
        });
        socket.on('pipeline_complete', (data) => {
            addLog('Pipeline completed with status: ' + data.status);
        });
        
        // Request initial data
        socket.emit('request_metrics');
        socket.emit('request_workers');
        
        addLog('Dashboard connected');
    </script>
</body>
</html>`;
      
      fs.writeFileSync(indexPath, basicHtml);
      logger.info('Created basic dashboard HTML file');
    }
  }

  /**
   * Get server statistics
   * @returns {Object} - Server statistics
   */
  getStats() {
    return {
      connectedClients: this.connectedClients.size,
      metricsHistorySize: this.metricsHistory.length,
      uptime: process.uptime(),
      port: this.port
    };
  }
}

module.exports = { DashboardServer };
