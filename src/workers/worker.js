/**
 * Worker Thread Implementation
 * Handles data processing tasks in separate threads
 */

const { parentPort, workerData } = require('worker_threads');
const { v4: uuidv4 } = require('uuid');

// Worker initialization data
const workerId = workerData.workerId;
const config = workerData.config || {};

// Send log message back to main thread
function log(level, message, metadata = {}) {
  parentPort.postMessage({
    type: 'log',
    level,
    message,
    metadata: { ...metadata, workerId }
  });
}

// Initialize worker
log('info', `Worker ${workerId} initialized`);

// Send status update to main thread
function updateStatus(status) {
  parentPort.postMessage({
    type: 'status_update',
    status: {
      ...status,
      workerId,
      timestamp: Date.now()
    }
  });
}

// Update initial status
updateStatus({ state: 'idle', memoryUsage: process.memoryUsage() });

/**
 * Filter data based on provided criteria
 * @param {Array} data - Data to filter
 * @param {Object} options - Filter options
 * @returns {Array} - Filtered data
 */
function filterData(data, options) {
  const { filterField, filterValue, filterOperator = '==' } = options;
  
  if (!filterField) {
    return data;
  }
  
  return data.filter(item => {
    const fieldValue = item[filterField];
    
    switch (filterOperator) {
      case '==':
        return fieldValue == filterValue;
      case '===':
        return fieldValue === filterValue;
      case '!=':
        return fieldValue != filterValue;
      case '!==':
        return fieldValue !== filterValue;
      case '>':
        return fieldValue > filterValue;
      case '>=':
        return fieldValue >= filterValue;
      case '<':
        return fieldValue < filterValue;
      case '<=':
        return fieldValue <= filterValue;
      case 'includes':
        return String(fieldValue).includes(filterValue);
      case 'startsWith':
        return String(fieldValue).startsWith(filterValue);
      case 'endsWith':
        return String(fieldValue).endsWith(filterValue);
      default:
        return true;
    }
  });
}

/**
 * Transform data using provided transformation function
 * @param {Array} data - Data to transform
 * @param {Object} options - Transform options
 * @returns {Array} - Transformed data
 */
function transformData(data, options) {
  const { transformations = [] } = options;
  
  if (!transformations.length) {
    return data;
  }
  
  return data.map(item => {
    let result = { ...item };
    
    transformations.forEach(transformation => {
      const { targetField, sourceField, operation, value } = transformation;
      
      switch (operation) {
        case 'copy':
          result[targetField] = result[sourceField];
          break;
        case 'set':
          result[targetField] = value;
          break;
        case 'remove':
          delete result[targetField];
          break;
        case 'rename':
          result[targetField] = result[sourceField];
          delete result[sourceField];
          break;
        case 'uppercase':
          if (result[sourceField]) {
            result[targetField] = String(result[sourceField]).toUpperCase();
          }
          break;
        case 'lowercase':
          if (result[sourceField]) {
            result[targetField] = String(result[sourceField]).toLowerCase();
          }
          break;
        case 'add':
          result[targetField] = Number(result[sourceField] || 0) + Number(value || 0);
          break;
        case 'multiply':
          result[targetField] = Number(result[sourceField] || 0) * Number(value || 1);
          break;
        case 'concat':
          result[targetField] = `${result[sourceField] || ''}${value || ''}`;
          break;
        default:
          // No transformation
      }
    });
    
    return result;
  });
}

/**
 * Aggregate data based on provided criteria
 * @param {Array} data - Data to aggregate
 * @param {Object} options - Aggregation options
 * @returns {Array} - Aggregated data
 */
function aggregateData(data, options) {
  const { groupByField, aggregations = [] } = options;
  
  if (!groupByField || !aggregations.length) {
    return data;
  }
  
  // Group data by the specified field
  const groups = {};
  
  data.forEach(item => {
    const groupValue = item[groupByField];
    if (!groups[groupValue]) {
      groups[groupValue] = [];
    }
    groups[groupValue].push(item);
  });
  
  // Apply aggregations to each group
  return Object.entries(groups).map(([groupValue, items]) => {
    const result = { [groupByField]: groupValue };
    
    aggregations.forEach(agg => {
      const { field, operation, outputField } = agg;
      
      switch (operation) {
        case 'count':
          result[outputField] = items.length;
          break;
        case 'sum':
          result[outputField] = items.reduce((sum, item) => sum + Number(item[field] || 0), 0);
          break;
        case 'avg':
          result[outputField] = items.reduce((sum, item) => sum + Number(item[field] || 0), 0) / items.length;
          break;
        case 'min':
          result[outputField] = Math.min(...items.map(item => Number(item[field] || 0)));
          break;
        case 'max':
          result[outputField] = Math.max(...items.map(item => Number(item[field] || 0)));
          break;
        case 'first':
          result[outputField] = items[0][field];
          break;
        case 'last':
          result[outputField] = items[items.length - 1][field];
          break;
        case 'concat':
          result[outputField] = items.map(item => item[field]).join(',');
          break;
        default:
          // No aggregation
      }
    });
    
    return result;
  });
}

/**
 * Process data through pipeline stages
 * @param {Array} data - Data to process
 * @param {Object} options - Processing options
 * @returns {Array} - Processed data
 */
function processData(data, options) {
  try {
    // Update status to processing
    updateStatus({ state: 'processing', itemCount: data.length });
    
    // Apply pipeline stages
    let result = data;
    
    // Filter stage
    if (options.filter) {
      result = filterData(result, options.filter);
      log('debug', `Filtered data: ${result.length} items remaining`);
    }
    
    // Transform stage
    if (options.transform) {
      result = transformData(result, options.transform);
      log('debug', `Transformed data: ${result.length} items`);
    }
    
    // Aggregate stage
    if (options.aggregate) {
      result = aggregateData(result, options.aggregate);
      log('debug', `Aggregated data: ${result.length} groups`);
    }
    
    // Update status to idle
    updateStatus({ state: 'idle', memoryUsage: process.memoryUsage() });
    
    return result;
  } catch (error) {
    log('error', `Error processing data: ${error.message}`, { stack: error.stack });
    throw error;
  }
}

// Listen for messages from the main thread
parentPort.on('message', async (message) => {
  try {
    if (!message || !message.type) {
      throw new Error('Invalid message received');
    }
    
    switch (message.type) {
      case 'process_data':
        const { taskId, data, options } = message;
        log('debug', `Processing task ${taskId} with ${data.length} items`);
        
        try {
          // Process the data
          const result = processData(data, options);
          
          // Send result back to main thread
          parentPort.postMessage({
            type: 'task_complete',
            taskId,
            result
          });
        } catch (error) {
          // Send error back to main thread
          parentPort.postMessage({
            type: 'task_error',
            taskId,
            error: error.message
          });
        }
        break;
        
      case 'ping':
        // Respond to ping
        parentPort.postMessage({
          type: 'pong',
          workerId,
          timestamp: Date.now()
        });
        break;
        
      default:
        log('warn', `Unknown message type: ${message.type}`);
    }
  } catch (error) {
    log('error', `Error handling message: ${error.message}`, { stack: error.stack });
  }
});

// Handle errors
process.on('uncaughtException', (error) => {
  log('error', `Uncaught exception: ${error.message}`, { stack: error.stack });
  // Exit with error code
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  log('error', `Unhandled rejection: ${reason}`, { stack: reason.stack });
  // Exit with error code
  process.exit(1);
});

// Log worker ready
log('info', `Worker ${workerId} ready to process data`);
