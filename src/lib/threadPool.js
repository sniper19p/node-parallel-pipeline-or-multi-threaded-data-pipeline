/**
 * Thread Pool Manager
 * Manages a pool of worker threads for parallel data processing
 */

const { Worker } = require('worker_threads');
const path = require('path');
const os = require('os');
const EventEmitter = require('events');
const { v4: uuidv4 } = require('uuid');
const logger = require('./logger');

class ThreadPoolManager extends EventEmitter {
  constructor(options) {
    super();
    this.options = {
      threadCount: Math.max(1, os.cpus().length - 1),
      maxQueueSize: 10000,
      workerScript: path.resolve(__dirname, '../workers/worker.js'),
      monitor: null,
      config: {},
      ...options
    };
    
    this.workers = [];
    this.availableWorkers = [];
    this.activeWorkers = new Map();
    this.taskQueue = [];
    this.isShuttingDown = false;
    this.monitor = this.options.monitor;
    this.taskTimeouts = new Map();
  }

  /**
   * Initialize the thread pool
   */
  async initialize() {
    logger.info(`Initializing thread pool with ${this.options.threadCount} workers`);
    
    if (this.monitor) {
      this.monitor.updateMetric('totalThreads', this.options.threadCount);
    }
    
    // Create worker threads
    for (let i = 0; i < this.options.threadCount; i++) {
      await this._createWorker(i);
    }
    
    logger.info(`Thread pool initialized with ${this.workers.length} workers`);
    return this.workers.length;
  }

  /**
   * Create a new worker
   * @param {number} workerId - Worker identifier
   * @returns {Promise<Worker>} - The created worker
   */
  async _createWorker(workerId) {
    return new Promise((resolve, reject) => {
      try {
        logger.debug(`Creating worker thread #${workerId}`);
        
        // Create worker with worker_threads
        const worker = new Worker(this.options.workerScript, {
          workerData: {
            workerId,
            config: this.options.config
          }
        });
        
        // Setup event listeners
        worker.on('message', (message) => this._handleWorkerMessage(worker, message));
        worker.on('error', (error) => this._handleWorkerError(worker, error));
        worker.on('exit', (code) => this._handleWorkerExit(worker, code, workerId));
        
        worker.id = workerId;
        worker.taskCount = 0;
        worker.isAvailable = true;
        
        // Add to workers array and available workers
        this.workers.push(worker);
        this.availableWorkers.push(worker);
        
        if (this.monitor) {
          this.monitor.updateMetric('activeWorkers', this.workers.length - this.availableWorkers.length);
          this.monitor.updateMetric('idleWorkers', this.availableWorkers.length);
        }
        
        resolve(worker);
      } catch (error) {
        logger.error(`Failed to create worker #${workerId}`, { error: error.message });
        reject(error);
      }
    });
  }

  /**
   * Handle worker message event
   * @param {Worker} worker - The worker that sent the message
   * @param {Object} message - The message received
   */
  _handleWorkerMessage(worker, message) {
    if (!message || !message.type) {
      logger.warn('Received invalid message from worker', { workerId: worker.id, message });
      return;
    }
    
    switch (message.type) {
      case 'task_complete':
        this._handleTaskComplete(worker, message.taskId, message.result);
        break;
      case 'task_error':
        this._handleTaskError(worker, message.taskId, message.error);
        break;
      case 'status_update':
        if (this.monitor) {
          this.monitor.updateWorkerStatus(worker.id, message.status);
        }
        break;
      case 'log':
        logger[message.level || 'info'](message.message, message.metadata || {});
        break;
      default:
        logger.warn('Unknown message type from worker', { 
          workerId: worker.id, 
          messageType: message.type 
        });
    }
  }

  /**
   * Handle successful task completion
   * @param {Worker} worker - The worker that completed the task
   * @param {string} taskId - The ID of the completed task
   * @param {*} result - The task result
   */
  _handleTaskComplete(worker, taskId, result) {
    // Clear any timeout for this task
    if (this.taskTimeouts.has(taskId)) {
      clearTimeout(this.taskTimeouts.get(taskId));
      this.taskTimeouts.delete(taskId);
    }
    
    const taskResolver = this.activeWorkers.get(taskId);
    if (taskResolver) {
      const { resolve, metadata } = taskResolver;
      
      // Record task completion time
      const endTime = Date.now();
      const processingTime = endTime - metadata.startTime;
      
      if (this.monitor) {
        this.monitor.recordTaskCompletion(processingTime);
        this.monitor.updateMetric('lastTaskDuration', processingTime);
      }
      
      // Remove from active workers
      this.activeWorkers.delete(taskId);
      
      // Make worker available again
      worker.isAvailable = true;
      this.availableWorkers.push(worker);
      
      // Update worker stats
      worker.taskCount++;
      
      if (this.monitor) {
        this.monitor.updateMetric('activeWorkers', this.workers.length - this.availableWorkers.length);
        this.monitor.updateMetric('idleWorkers', this.availableWorkers.length);
      }
      
      // Resolve the promise with the result
      resolve(result);
      
      // Process next task in queue if any
      this._processNextTask();
    } else {
      logger.warn(`Received completion for unknown task ${taskId} from worker ${worker.id}`);
    }
  }

  /**
   * Handle task error
   * @param {Worker} worker - The worker where the error occurred
   * @param {string} taskId - The ID of the failed task
   * @param {Error|string} error - The error that occurred
   */
  _handleTaskError(worker, taskId, error) {
    // Clear any timeout for this task
    if (this.taskTimeouts.has(taskId)) {
      clearTimeout(this.taskTimeouts.get(taskId));
      this.taskTimeouts.delete(taskId);
    }
    
    logger.error(`Task ${taskId} failed in worker ${worker.id}`, { error });
    
    const taskResolver = this.activeWorkers.get(taskId);
    if (taskResolver) {
      const { reject } = taskResolver;
      
      // Remove from active workers
      this.activeWorkers.delete(taskId);
      
      // Make worker available again
      worker.isAvailable = true;
      this.availableWorkers.push(worker);
      
      if (this.monitor) {
        this.monitor.updateMetric('activeWorkers', this.workers.length - this.availableWorkers.length);
        this.monitor.updateMetric('idleWorkers', this.availableWorkers.length);
        this.monitor.updateMetric('taskErrors', 1, true); // Increment error count
      }
      
      // Reject the promise with the error
      reject(typeof error === 'string' ? new Error(error) : error);
      
      // Process next task in queue if any
      this._processNextTask();
    } else {
      logger.warn(`Received error for unknown task ${taskId} from worker ${worker.id}`);
    }
  }

  /**
   * Handle worker error
   * @param {Worker} worker - The worker where the error occurred
   * @param {Error} error - The error that occurred
   */
  _handleWorkerError(worker, error) {
    logger.error(`Worker ${worker.id} encountered an error`, { error: error.message });
    
    if (this.monitor) {
      this.monitor.updateMetric('workerErrors', 1, true); // Increment error count
    }
    
    // Worker will terminate after error, it will be restarted by exit handler
  }

  /**
   * Handle worker exit
   * @param {Worker} worker - The worker that exited
   * @param {number} code - Exit code
   * @param {number} workerId - Worker ID to recreate
   */
  _handleWorkerExit(worker, code, workerId) {
    logger.info(`Worker ${worker.id} exited with code ${code}`);
    
    // Remove from workers and available workers arrays
    this.workers = this.workers.filter(w => w !== worker);
    this.availableWorkers = this.availableWorkers.filter(w => w !== worker);
    
    if (this.monitor) {
      this.monitor.updateMetric('activeWorkers', this.workers.length - this.availableWorkers.length);
      this.monitor.updateMetric('idleWorkers', this.availableWorkers.length);
    }
    
    // Check if any active tasks were assigned to this worker and fail them
    this.activeWorkers.forEach((taskResolver, taskId) => {
      if (taskResolver.worker === worker) {
        const { reject } = taskResolver;
        reject(new Error(`Worker ${worker.id} terminated unexpectedly`));
        this.activeWorkers.delete(taskId);
        
        if (this.monitor) {
          this.monitor.updateMetric('taskErrors', 1, true);
        }
      }
    });
    
    // Restart worker if not shutting down
    if (!this.isShuttingDown && code !== 0) {
      logger.info(`Restarting worker ${workerId} after abnormal termination`);
      this._createWorker(workerId).catch(error => {
        logger.error(`Failed to restart worker ${workerId}`, { error: error.message });
      });
    }
  }

  /**
   * Process the next task in the queue if an available worker exists
   * @private
   */
  _processNextTask() {
    // If no tasks in queue or no available workers, return
    if (this.taskQueue.length === 0 || this.availableWorkers.length === 0) {
      return;
    }
    
    // Get next task and available worker
    const task = this.taskQueue.shift();
    const worker = this.availableWorkers.pop();
    
    if (!worker) {
      // Put task back in queue
      this.taskQueue.unshift(task);
      return;
    }
    
    worker.isAvailable = false;
    
    // Set start time
    task.metadata.startTime = Date.now();
    
    // Add to active workers
    this.activeWorkers.set(task.id, {
      ...task,
      worker
    });
    
    if (this.monitor) {
      this.monitor.updateMetric('queueSize', this.taskQueue.length);
      this.monitor.updateMetric('activeWorkers', this.workers.length - this.availableWorkers.length);
      this.monitor.updateMetric('idleWorkers', this.availableWorkers.length);
    }
    
    // Set timeout for task if specified
    if (task.timeout > 0) {
      const timeoutId = setTimeout(() => {
        logger.warn(`Task ${task.id} timed out after ${task.timeout}ms`);
        task.reject(new Error(`Task timeout after ${task.timeout}ms`));
        this.activeWorkers.delete(task.id);
        
        if (this.monitor) {
          this.monitor.updateMetric('taskTimeouts', 1, true);
        }
        
        // Terminate worker and create a new one
        logger.info(`Terminating unresponsive worker ${worker.id}`);
        try {
          worker.terminate();
        } catch (error) {
          logger.error(`Error terminating worker ${worker.id}`, { error: error.message });
        }
      }, task.timeout);
      
      this.taskTimeouts.set(task.id, timeoutId);
    }
    
    // Send task to worker
    worker.postMessage({
      type: 'process_data',
      taskId: task.id,
      data: task.data,
      options: task.options
    });
  }

  /**
   * Process a batch of data items
   * @param {Array} batch - Batch of data items to process
   * @param {Object} options - Processing options
   * @returns {Promise<Array>} - Processed results
   */
  async processBatch(batch, options = {}) {
    if (this.isShuttingDown) {
      throw new Error('Thread pool is shutting down');
    }
    
    if (!Array.isArray(batch)) {
      throw new Error('Batch must be an array');
    }
    
    if (batch.length === 0) {
      return [];
    }
    
    // Create task
    const taskId = uuidv4();
    const taskOptions = { ...options };
    const timeout = taskOptions.timeout || 30000; // Default timeout: 30 seconds
    
    // Return promise that will be resolved/rejected when task completes/fails
    return new Promise((resolve, reject) => {
      const task = {
        id: taskId,
        data: batch,
        options: taskOptions,
        resolve,
        reject,
        timeout,
        metadata: {
          createdAt: Date.now()
        }
      };
      
      // Add to queue
      this.taskQueue.push(task);
      
      if (this.monitor) {
        this.monitor.updateMetric('queueSize', this.taskQueue.length);
        this.monitor.updateMetric('tasksQueued', 1, true);
      }
      
      // Try to process immediately if workers are available
      this._processNextTask();
      
      // Log warning if queue is getting large
      if (this.taskQueue.length > this.options.maxQueueSize * 0.8) {
        logger.warn(`Task queue is at ${this.taskQueue.length} items (${this.options.maxQueueSize * 0.8}% of max)`);
      }
    });
  }

  /**
   * Get thread pool status
   * @returns {Object} - Status object
   */
  getStatus() {
    return {
      totalWorkers: this.workers.length,
      activeWorkers: this.workers.length - this.availableWorkers.length,
      idleWorkers: this.availableWorkers.length,
      queueSize: this.taskQueue.length,
      isShuttingDown: this.isShuttingDown,
      activeTasks: Array.from(this.activeWorkers.keys())
    };
  }

  /**
   * Shut down the thread pool
   */
  async shutdown() {
    logger.info('Shutting down thread pool...');
    this.isShuttingDown = true;
    
    // Clear all timeouts
    for (const timeoutId of this.taskTimeouts.values()) {
      clearTimeout(timeoutId);
    }
    this.taskTimeouts.clear();
    
    // Reject any pending tasks
    this.taskQueue.forEach(task => {
      task.reject(new Error('Thread pool shutting down'));
    });
    this.taskQueue = [];
    
    // Reject any active tasks
    for (const [taskId, taskResolver] of this.activeWorkers.entries()) {
      taskResolver.reject(new Error('Thread pool shutting down'));
      this.activeWorkers.delete(taskId);
    }
    
    // Terminate all workers
    const terminationPromises = this.workers.map(worker => {
      return new Promise(resolve => {
        worker.once('exit', () => resolve());
        worker.terminate();
      });
    });
    
    // Wait for all workers to terminate
    await Promise.all(terminationPromises);
    
    logger.info('Thread pool shut down successfully');
  }
}

module.exports = { ThreadPoolManager };
