/**
 * Data Source Manager
 * Manages data input for the pipeline
 */

const fs = require('fs');
const path = require('path');
const { createReadStream } = require('fs');
const { Readable } = require('stream');
const csv = require('csv-parser');
const logger = require('./logger');

class DataSourceManager {
  constructor(options = {}) {
    this.options = {
      inputFile: null,
      batchSize: 1000,
      dataType: null, // 'json', 'csv', 'stream'
      monitor: null,
      ...options
    };
    
    this.isOpen = false;
    this.isEOF = false;
    this.monitor = this.options.monitor;
    this.buffer = [];
    this.currentPosition = 0;
    this.totalRecords = 0;
    this.sourceStream = null;
    
    // Auto-detect data type from file extension if not provided
    if (!this.options.dataType && this.options.inputFile) {
      const ext = path.extname(this.options.inputFile).toLowerCase();
      if (ext === '.json') {
        this.options.dataType = 'json';
      } else if (ext === '.csv') {
        this.options.dataType = 'csv';
      } else {
        this.options.dataType = 'raw';
      }
    }
  }

  /**
   * Open the data source
   */
  async open() {
    if (this.isOpen) {
      throw new Error('Data source is already open');
    }
    
    // Check if input file is provided and exists
    if (this.options.inputFile) {
      try {
        await fs.promises.access(this.options.inputFile, fs.constants.R_OK);
        logger.info(`Opening data source from file: ${this.options.inputFile}`);
        await this._openFile();
      } catch (error) {
        logger.error(`Failed to open input file: ${this.options.inputFile}`, { error: error.message });
        throw new Error(`Failed to open input file: ${error.message}`);
      }
    } else {
      // No input file, use generate mode
      logger.info('No input file provided, using generated data');
      await this._setupGeneratedData();
    }
    
    this.isOpen = true;
    
    if (this.monitor) {
      this.monitor.updateMetric('dataSourceType', this.options.dataType);
    }
    
    return true;
  }

  /**
   * Open and prepare file for reading
   * @private
   */
  async _openFile() {
    // Different handling based on data type
    switch (this.options.dataType) {
      case 'json':
        await this._openJsonFile();
        break;
      case 'csv':
        this._openCsvFile();
        break;
      case 'raw':
      default:
        this._openRawFile();
        break;
    }
  }

  /**
   * Open and parse JSON file
   * @private
   */
  async _openJsonFile() {
    try {
      // For JSON files, read the entire file into memory
      // This is appropriate for moderately sized files
      const content = await fs.promises.readFile(this.options.inputFile, 'utf8');
      const data = JSON.parse(content);
      
      if (Array.isArray(data)) {
        this.buffer = data;
      } else {
        // If the JSON is an object with a data property that is an array, use that
        this.buffer = Array.isArray(data.data) ? data.data : [data];
      }
      
      this.totalRecords = this.buffer.length;
      this.currentPosition = 0;
      
      logger.info(`Loaded ${this.totalRecords} records from JSON file`);
      
      if (this.monitor) {
        this.monitor.updateMetric('totalRecords', this.totalRecords);
      }
    } catch (error) {
      logger.error('Error parsing JSON file', { error: error.message });
      throw error;
    }
  }

  /**
   * Open and set up CSV file streaming
   * @private
   */
  _openCsvFile() {
    // For CSV files, create a read stream for processing
    this.sourceStream = createReadStream(this.options.inputFile)
      .pipe(csv());
      
    // Set up data buffering from stream
    this._setupStreamBuffering();
  }

  /**
   * Open raw file for reading
   * @private
   */
  _openRawFile() {
    // Create a read stream for raw files
    this.sourceStream = createReadStream(this.options.inputFile);
    
    // Set up data buffering from stream
    this._setupStreamBuffering();
  }

  /**
   * Set up generated data for testing
   * @private
   */
  async _setupGeneratedData() {
    // Generate sample data
    const dataSize = 10000; // Default size
    this.buffer = new Array(dataSize).fill(null).map((_, index) => ({
      id: `gen-${index}`,
      value: Math.random() * 100,
      name: `Item ${index}`,
      category: ['A', 'B', 'C', 'D'][Math.floor(Math.random() * 4)],
      timestamp: new Date().toISOString(),
      isActive: Math.random() > 0.3
    }));
    
    this.totalRecords = dataSize;
    this.currentPosition = 0;
    this.options.dataType = 'generated';
    
    logger.info(`Generated ${dataSize} sample records for processing`);
    
    if (this.monitor) {
      this.monitor.updateMetric('totalRecords', dataSize);
    }
  }

  /**
   * Setup buffering from stream sources
   * @private
   */
  _setupStreamBuffering() {
    this.buffer = [];
    
    // Setup stream data handling
    this.sourceStream.on('data', (chunk) => {
      this.buffer.push(chunk);
      this.totalRecords++;
      
      if (this.monitor && this.totalRecords % 1000 === 0) {
        this.monitor.updateMetric('totalRecords', this.totalRecords);
      }
    });
    
    this.sourceStream.on('end', () => {
      logger.info(`Stream ended, total records: ${this.totalRecords}`);
      this.isEOF = true;
      
      if (this.monitor) {
        this.monitor.updateMetric('totalRecords', this.totalRecords);
      }
    });
    
    this.sourceStream.on('error', (error) => {
      logger.error('Stream error', { error: error.message });
      this.isEOF = true;
    });
  }

  /**
   * Get the next batch of data
   * @param {number} count - Batch size (optional, default is from constructor)
   * @returns {Promise<Array>} - Next batch of data
   */
  async getNextBatch(count = null) {
    if (!this.isOpen) {
      throw new Error('Data source is not open');
    }
    
    if (this.isEOF && this.currentPosition >= this.totalRecords && this.buffer.length === 0) {
      return []; // No more data
    }
    
    const batchSize = count || this.options.batchSize;
    
    // Handle in-memory data
    if (this.options.dataType === 'json' || this.options.dataType === 'generated') {
      const start = this.currentPosition;
      const end = Math.min(start + batchSize, this.buffer.length);
      
      if (start >= this.buffer.length) {
        return []; // No more data
      }
      
      const batch = this.buffer.slice(start, end);
      this.currentPosition = end;
      
      // Update monitoring
      if (this.monitor) {
        this.monitor.updateMetric('recordsRead', batch.length, true);
        this.monitor.updateMetric('currentPosition', this.currentPosition);
      }
      
      return batch;
    }
    
    // Handle streaming data sources
    if (this.sourceStream) {
      // If we have enough data in buffer, return it
      if (this.buffer.length >= batchSize) {
        const batch = this.buffer.splice(0, batchSize);
        
        if (this.monitor) {
          this.monitor.updateMetric('recordsRead', batch.length, true);
        }
        
        return batch;
      }
      
      // If stream is ended and we have some data, return what we have
      if (this.isEOF && this.buffer.length > 0) {
        const batch = [...this.buffer];
        this.buffer = [];
        
        if (this.monitor) {
          this.monitor.updateMetric('recordsRead', batch.length, true);
        }
        
        return batch;
      }
      
      // Wait for more data from stream
      if (!this.isEOF) {
        await new Promise(resolve => setTimeout(resolve, 100));
        return this.getNextBatch(count); // Retry
      }
      
      return []; // No more data
    }
    
    return []; // No data source configured
  }

  /**
   * Close the data source
   */
  async close() {
    if (!this.isOpen) {
      return;
    }
    
    if (this.sourceStream) {
      // Close the stream if it supports it
      if (typeof this.sourceStream.close === 'function') {
        this.sourceStream.close();
      } else if (typeof this.sourceStream.destroy === 'function') {
        this.sourceStream.destroy();
      }
      this.sourceStream = null;
    }
    
    this.isOpen = false;
    logger.info('Data source closed');
  }

  /**
   * Get information about the data source
   * @returns {Object} - Data source information
   */
  getInfo() {
    return {
      type: this.options.dataType,
      source: this.options.inputFile || 'generated',
      totalRecords: this.totalRecords,
      currentPosition: this.currentPosition,
      isEOF: this.isEOF,
      isOpen: this.isOpen
    };
  }
}

module.exports = { DataSourceManager };
