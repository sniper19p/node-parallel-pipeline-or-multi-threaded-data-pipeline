/**
 * Data Sink Manager
 * Handles processed output data from the pipeline
 */

const fs = require('fs-extra');
const path = require('path');
const { createWriteStream } = require('fs');
const { Writable } = require('stream');
const logger = require('./logger');

class DataSinkManager {
  constructor(options = {}) {
    this.options = {
      outputFile: './output/processed_data.json',
      outputFormat: null, // 'json', 'csv', 'ndjson'
      monitor: null,
      bufferSize: 5000, // Maximum records to hold in memory before writing
      prettyPrint: false, // Whether to pretty print JSON output
      ...options
    };
    
    this.isOpen = false;
    this.buffer = [];
    this.totalWritten = 0;
    this.outputStream = null;
    this.monitor = this.options.monitor;
    
    // Auto-detect output format from file extension if not provided
    if (!this.options.outputFormat && this.options.outputFile) {
      const ext = path.extname(this.options.outputFile).toLowerCase();
      if (ext === '.json') {
        this.options.outputFormat = 'json';
      } else if (ext === '.csv') {
        this.options.outputFormat = 'csv';
      } else if (ext === '.ndjson' || ext === '.jsonl') {
        this.options.outputFormat = 'ndjson';
      } else {
        this.options.outputFormat = 'json'; // Default
      }
    }
  }

  /**
   * Open the data sink
   */
  async open() {
    if (this.isOpen) {
      throw new Error('Data sink is already open');
    }
    
    // Create output directory if it doesn't exist
    if (this.options.outputFile) {
      const outputDir = path.dirname(this.options.outputFile);
      await fs.ensureDir(outputDir);
      
      logger.info(`Opening data sink: ${this.options.outputFile}`);
      
      // Open output stream based on format
      await this._openOutputStream();
    } else {
      logger.info('No output file specified, data will be processed but not saved');
    }
    
    this.isOpen = true;
    
    if (this.monitor) {
      this.monitor.updateMetric('dataSinkFormat', this.options.outputFormat);
    }
    
    return true;
  }

  /**
   * Open the appropriate output stream based on format
   * @private
   */
  async _openOutputStream() {
    if (!this.options.outputFile) return;
    
    try {
      switch (this.options.outputFormat) {
        case 'json':
          // For JSON we'll buffer all data and write at once or when buffer is full
          this.buffer = [];
          break;
          
        case 'csv':
          this.outputStream = createWriteStream(this.options.outputFile);
          // Write CSV header if needed
          // We'll assume the first data batch will define the columns
          break;
          
        case 'ndjson':
        default:
          this.outputStream = createWriteStream(this.options.outputFile);
          break;
      }
      
      logger.info(`Output stream opened: ${this.options.outputFormat}`);
    } catch (error) {
      logger.error(`Failed to open output stream: ${error.message}`);
      throw error;
    }
  }

  /**
   * Write data to the sink
   * @param {Array|Object} data - Data to write
   */
  async write(data) {
    if (!this.isOpen) {
      throw new Error('Data sink is not open');
    }
    
    // Convert single object to array
    const dataArray = Array.isArray(data) ? data : [data];
    
    if (dataArray.length === 0) {
      return 0; // No data to write
    }
    
    // Add to processing count
    this.totalWritten += dataArray.length;
    
    // Update monitoring
    if (this.monitor) {
      this.monitor.updateMetric('recordsWritten', dataArray.length, true);
      this.monitor.updateMetric('totalWritten', this.totalWritten);
    }
    
    // If no output file, just count the data
    if (!this.options.outputFile) {
      return dataArray.length;
    }
    
    try {
      switch (this.options.outputFormat) {
        case 'json':
          // Add to buffer
          this.buffer.push(...dataArray);
          
          // If buffer exceeds limit, write to file
          if (this.buffer.length >= this.options.bufferSize) {
            await this._writeJsonBuffer();
          }
          break;
          
        case 'csv':
          await this._writeCsvData(dataArray);
          break;
          
        case 'ndjson':
        default:
          await this._writeNdJsonData(dataArray);
          break;
      }
      
      return dataArray.length;
    } catch (error) {
      logger.error(`Error writing data: ${error.message}`);
      throw error;
    }
  }

  /**
   * Write the JSON buffer to file
   * @private
   */
  async _writeJsonBuffer() {
    if (this.buffer.length === 0) return;
    
    logger.debug(`Writing JSON buffer with ${this.buffer.length} records`);
    
    // Create a properly formatted JSON document
    const jsonStr = this.options.prettyPrint 
      ? JSON.stringify(this.buffer, null, 2)
      : JSON.stringify(this.buffer);
      
    // Write to file, overwriting any previous content
    await fs.writeFile(this.options.outputFile, jsonStr, 'utf8');
    
    // Clear buffer
    const written = this.buffer.length;
    this.buffer = [];
    
    logger.debug(`Wrote ${written} records to JSON file`);
    return written;
  }

  /**
   * Write data in CSV format
   * @param {Array} dataArray - Array of data objects
   * @private
   */
  async _writeCsvData(dataArray) {
    if (dataArray.length === 0) return 0;
    
    // Check if header needs to be written
    if (this.totalWritten === dataArray.length && this.outputStream) {
      // Get headers from first object keys
      const headers = Object.keys(dataArray[0]).join(',') + '\n';
      this.outputStream.write(headers);
    }
    
    // Convert each object to CSV line
    let written = 0;
    for (const item of dataArray) {
      const line = Object.values(item)
        .map(val => {
          if (val === null || val === undefined) return '';
          if (typeof val === 'string') return `"${val.replace(/"/g, '""')}"`;
          if (typeof val === 'object') return `"${JSON.stringify(val).replace(/"/g, '""')}"`;
          return val;
        })
        .join(',') + '\n';
      
      if (this.outputStream) {
        const canContinue = this.outputStream.write(line);
        written++;
        
        // Handle backpressure
        if (!canContinue) {
          await new Promise(resolve => this.outputStream.once('drain', resolve));
        }
      }
    }
    
    return written;
  }

  /**
   * Write data in NDJSON format (one JSON object per line)
   * @param {Array} dataArray - Array of data objects
   * @private
   */
  async _writeNdJsonData(dataArray) {
    if (dataArray.length === 0) return 0;
    
    let written = 0;
    for (const item of dataArray) {
      const line = JSON.stringify(item) + '\n';
      
      if (this.outputStream) {
        const canContinue = this.outputStream.write(line);
        written++;
        
        // Handle backpressure
        if (!canContinue) {
          await new Promise(resolve => this.outputStream.once('drain', resolve));
        }
      }
    }
    
    return written;
  }

  /**
   * Flush any buffered data to storage
   */
  async flush() {
    if (!this.isOpen) {
      return;
    }
    
    try {
      switch (this.options.outputFormat) {
        case 'json':
          await this._writeJsonBuffer();
          break;
          
        case 'csv':
        case 'ndjson':
        default:
          if (this.outputStream) {
            // For streaming formats, just wait for any pending writes
            await new Promise(resolve => {
              this.outputStream.cork();
              process.nextTick(() => {
                this.outputStream.uncork();
                resolve();
              });
            });
          }
          break;
      }
      
      logger.debug('Data sink flushed');
    } catch (error) {
      logger.error(`Error flushing data sink: ${error.message}`);
      throw error;
    }
  }

  /**
   * Close the data sink
   */
  async close() {
    if (!this.isOpen) {
      return;
    }
    
    try {
      // Flush any remaining data
      await this.flush();
      
      // Close output stream if exists
      if (this.outputStream) {
        await new Promise((resolve, reject) => {
          this.outputStream.end(err => {
            if (err) reject(err);
            else resolve();
          });
        });
        this.outputStream = null;
      }
      
      this.isOpen = false;
      logger.info(`Data sink closed, total records written: ${this.totalWritten}`);
    } catch (error) {
      logger.error(`Error closing data sink: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get information about the data sink
   * @returns {Object} - Data sink information
   */
  getInfo() {
    return {
      format: this.options.outputFormat,
      destination: this.options.outputFile || 'none',
      totalWritten: this.totalWritten,
      bufferSize: this.buffer.length,
      isOpen: this.isOpen
    };
  }
}

module.exports = { DataSinkManager };
