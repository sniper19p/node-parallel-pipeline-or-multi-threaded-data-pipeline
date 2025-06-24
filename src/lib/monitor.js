/**
 * Pipeline Monitor
 * Tracks performance metrics and status for the data processing pipeline
 */

const EventEmitter = require('events');
const os = require('os');

class PipelineMonitor extends EventEmitter {
  constructor(updateInterval = 1000) {
    super();
    
    this.metrics = {
      // Pipeline status
      status: 'initializing', // initializing, running, paused, completed, failed
      startTime: null,
      endTime: null,
      elapsedTime: 0,
      
      // Thread metrics
      totalThreads: 0,
      activeWorkers: 0,
      idleWorkers: 0,
      
      // Task metrics
      tasksQueued: 0,
      tasksProcessing: 0,
      tasksCompleted: 0,
      taskErrors: 0,
      taskTimeouts: 0,
      queueSize: 0,
      lastTaskDuration: 0,
      
      // Data metrics
      totalRecords: 0,
      recordsRead: 0,
      recordsProcessed: 0,
      recordsWritten: 0,
      currentPosition: 0,
      
      // Performance metrics
      throughput: 0, // records per second
      avgProcessingTime: 0, // milliseconds per record
      peakMemoryUsage: 0,
      currentMemoryUsage: 0,
      cpuUsage: 0,
      
      // Custom metrics
      custom: {}
    };
    
    // Track task processing times for stats
    this.taskTimes = [];
    this.recentTaskTimes = []; // For moving average
    
    // Store worker status
    this.workerStatus = new Map();
    
    // For calculating throughput
    this.prevRecordsProcessed = 0;
    this.prevTimestamp = Date.now();
    
    // Update interval in ms
    this.updateInterval = updateInterval;
    this.updateTimer = null;
    this.isRunning = false;
  }

  /**
   * Start the monitoring system
   */
  start() {
    if (this.isRunning) return;
    
    this.metrics.startTime = Date.now();
    this.metrics.status = 'running';
    this.isRunning = true;
    this.prevTimestamp = Date.now();
    this.prevRecordsProcessed = 0;
    
    // Schedule periodic updates
    this.updateTimer = setInterval(() => this._updateMetrics(), this.updateInterval);
    
    this.emit('monitor_start', { timestamp: this.metrics.startTime });
  }

  /**
   * Stop the monitoring system
   */
  stop() {
    if (!this.isRunning) return;
    
    clearInterval(this.updateTimer);
    this.updateTimer = null;
    this.isRunning = false;
    
    // One final update
    this._updateMetrics();
    
    this.emit('monitor_stop', { timestamp: Date.now() });
  }

  /**
   * Set pipeline status
   * @param {string} status - New status
   */
  setStatus(status) {
    const validStatuses = ['initializing', 'running', 'paused', 'completed', 'failed'];
    if (!validStatuses.includes(status)) {
      throw new Error(`Invalid status: ${status}`);
    }
    
    const prevStatus = this.metrics.status;
    this.metrics.status = status;
    
    if (status === 'completed' || status === 'failed') {
      this.metrics.endTime = Date.now();
      this.metrics.elapsedTime = this.metrics.endTime - (this.metrics.startTime || this.metrics.endTime);
    }
    
    this.emit('status_change', { 
      prevStatus, 
      newStatus: status,
      timestamp: Date.now()
    });
  }

  /**
   * Update a metric value
   * @param {string} name - Metric name
   * @param {*} value - New value
   * @param {boolean} increment - Whether to increment instead of replace
   */
  updateMetric(name, value, increment = false) {
    // Handle nested metrics with dot notation (e.g., 'custom.myMetric')
    const parts = name.split('.');
    
    if (parts.length > 1) {
      let obj = this.metrics;
      for (let i = 0; i < parts.length - 1; i++) {
        const part = parts[i];
        if (!obj[part] || typeof obj[part] !== 'object') {
          obj[part] = {};
        }
        obj = obj[part];
      }
      
      const lastPart = parts[parts.length - 1];
      if (increment) {
        obj[lastPart] = (obj[lastPart] || 0) + value;
      } else {
        obj[lastPart] = value;
      }
    } else {
      // Direct metric
      if (increment) {
        this.metrics[name] = (this.metrics[name] || 0) + value;
      } else {
        this.metrics[name] = value;
      }
    }
    
    // Emit metric update event for real-time observers
    this.emit('metric_update', { 
      name, 
      value: increment ? (this.metrics[name] || 0) : value,
      timestamp: Date.now() 
    });
  }

  /**
   * Record task completion time
   * @param {number} processingTime - Time taken to process task in milliseconds
   */
  recordTaskCompletion(processingTime) {
    this.taskTimes.push(processingTime);
    this.recentTaskTimes.push(processingTime);
    
    // Keep recent tasks window to the last 100 tasks
    if (this.recentTaskTimes.length > 100) {
      this.recentTaskTimes.shift();
    }
    
    // Update metrics
    this.metrics.tasksCompleted++;
    this.metrics.tasksProcessing = Math.max(0, this.metrics.tasksProcessing - 1);
    
    // Calculate average processing time
    if (this.recentTaskTimes.length > 0) {
      const sum = this.recentTaskTimes.reduce((a, b) => a + b, 0);
      this.metrics.avgProcessingTime = sum / this.recentTaskTimes.length;
    }
  }

  /**
   * Update worker status
   * @param {number|string} workerId - Worker identifier
   * @param {Object} status - Worker status
   */
  updateWorkerStatus(workerId, status) {
    this.workerStatus.set(workerId, {
      ...status,
      lastUpdate: Date.now()
    });
    
    // Update worker counts
    const activeWorkers = [...this.workerStatus.values()].filter(s => s.status === 'processing').length;
    const idleWorkers = [...this.workerStatus.values()].filter(s => s.status === 'idle').length;
    
    this.metrics.activeWorkers = activeWorkers;
    this.metrics.idleWorkers = idleWorkers;
    
    this.emit('worker_status', { workerId, status, timestamp: Date.now() });
  }

  /**
   * Get all current metrics
   * @returns {Object} - All current metrics
   */
  getMetrics() {
    // Update elapsed time if still running
    if (this.isRunning && this.metrics.startTime) {
      this.metrics.elapsedTime = Date.now() - this.metrics.startTime;
    }
    
    return { ...this.metrics };
  }

  /**
   * Get a specific metric value
   * @param {string} name - Metric name
   * @returns {*} - Metric value
   */
  getMetric(name) {
    // Handle nested metrics with dot notation
    const parts = name.split('.');
    let value = this.metrics;
    
    for (const part of parts) {
      if (value && typeof value === 'object' && part in value) {
        value = value[part];
      } else {
        return undefined;
      }
    }
    
    return value;
  }

  /**
   * Get worker status
   * @param {number|string} workerId - Worker ID or 'all'
   * @returns {Object|Map} - Worker status(es)
   */
  getWorkerStatus(workerId) {
    if (workerId === 'all') {
      return new Map(this.workerStatus);
    }
    
    return this.workerStatus.get(workerId);
  }

  /**
   * Update calculated metrics
   * @private
   */
  _updateMetrics() {
    // Update elapsed time
    if (this.metrics.startTime) {
      this.metrics.elapsedTime = Date.now() - this.metrics.startTime;
    }
    
    // Calculate throughput (records/second)
    const now = Date.now();
    const timeDiff = now - this.prevTimestamp;
    if (timeDiff > 0) {
      const recordsProcessed = this.metrics.recordsProcessed - this.prevRecordsProcessed;
      this.metrics.throughput = Math.round((recordsProcessed / timeDiff) * 1000);
      this.prevRecordsProcessed = this.metrics.recordsProcessed;
      this.prevTimestamp = now;
    }
    
    // Update memory usage
    const memUsage = process.memoryUsage();
    this.metrics.currentMemoryUsage = Math.round(memUsage.heapUsed / (1024 * 1024)); // MB
    this.metrics.peakMemoryUsage = Math.max(this.metrics.peakMemoryUsage, this.metrics.currentMemoryUsage);
    
    // Get CPU usage (using OS module)
    try {
      const cpus = os.cpus();
      let totalIdle = 0;
      let totalTick = 0;
      
      for (const cpu of cpus) {
        for (const type in cpu.times) {
          totalTick += cpu.times[type];
        }
        totalIdle += cpu.times.idle;
      }
      
      const idle = totalIdle / cpus.length;
      const total = totalTick / cpus.length;
      const usage = Math.round(100 * (1 - idle / total));
      
      this.metrics.cpuUsage = usage;
    } catch (error) {
      // Fallback if CPU metrics unavailable
      this.metrics.cpuUsage = 0;
    }
    
    // Emit update event
    this.emit('metrics_updated', this.getMetrics());
  }

  /**
   * Final metrics update and cleanup
   */
  finalize() {
    this.stop();
    
    if (!this.metrics.endTime) {
      this.metrics.endTime = Date.now();
    }
    
    this.metrics.elapsedTime = this.metrics.endTime - (this.metrics.startTime || this.metrics.endTime);
    
    // Calculate final statistics
    const totalRecords = this.metrics.recordsProcessed;
    const totalTimeMs = this.metrics.elapsedTime;
    const avgThroughput = totalTimeMs > 0 ? Math.round((totalRecords / totalTimeMs) * 1000) : 0;
    
    this.metrics.finalStats = {
      totalRecords,
      totalTimeMs,
      avgThroughput,
      recordsPerWorker: Math.round(totalRecords / Math.max(1, this.metrics.totalThreads))
    };
    
    // Emit final event
    this.emit('pipeline_complete', {
      status: this.metrics.status,
      stats: this.metrics.finalStats,
      timestamp: Date.now()
    });
    
    return this.metrics.finalStats;
  }
}

module.exports = { PipelineMonitor };
