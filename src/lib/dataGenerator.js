/**
 * Data Generator
 * Utility for generating sample data for testing the pipeline
 */

const fs = require('fs-extra');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

class DataGenerator {
  constructor(options = {}) {
    this.options = {
      count: 100000,
      batchSize: 10000,
      outputPath: null,
      dataType: 'json', // json, csv, or raw
      seed: Date.now(),
      ...options
    };
    
    this.generatedCount = 0;
    this.dataTypes = ['user', 'transaction', 'event', 'log', 'sensor'];
    this.statusTypes = ['active', 'pending', 'completed', 'failed', 'cancelled'];
    
    // Create predictable random number generator with seed
    this.random = this._createSeededRandom(this.options.seed);
  }

  /**
   * Generate data in batches
   * @param {Function} onBatchGenerated - Callback for each batch of data
   * @returns {Promise<number>} - Total generated records count
   */
  async generateData(onBatchGenerated = null) {
    this.generatedCount = 0;
    const totalCount = this.options.count;
    const batchSize = this.options.batchSize;
    
    // If output path is specified, ensure directory exists
    if (this.options.outputPath) {
      await fs.ensureDir(path.dirname(this.options.outputPath));
      
      // Create write stream for the output file
      const writeStream = fs.createWriteStream(this.options.outputPath, { encoding: 'utf8' });
      
      // Write file header based on data type
      if (this.options.dataType === 'json') {
        writeStream.write('[\n');
      } else if (this.options.dataType === 'csv') {
        const headers = ['id', 'type', 'name', 'value', 'status', 'timestamp', 'metadata'];
        writeStream.write(headers.join(',') + '\n');
      }
      
      try {
        let isFirstBatch = true;
        
        // Generate data in batches
        for (let i = 0; i < totalCount; i += batchSize) {
          const currentBatchSize = Math.min(batchSize, totalCount - i);
          const batch = this._generateBatch(currentBatchSize);
          this.generatedCount += batch.length;
          
          // Write batch to file
          if (this.options.dataType === 'json') {
            const batchText = batch.map(item => JSON.stringify(item)).join(',\n');
            if (!isFirstBatch) {
              writeStream.write(',\n');
            }
            writeStream.write(batchText);
          } else if (this.options.dataType === 'csv') {
            const batchText = batch.map(item => 
              `${item.id},${item.type},${item.name},${item.value},${item.status},${item.timestamp},${JSON.stringify(item.metadata).replace(/,/g, ';')}`
            ).join('\n');
            writeStream.write(batchText + '\n');
          } else {
            // Raw format - one JSON object per line (NDJSON)
            const batchText = batch.map(item => JSON.stringify(item)).join('\n');
            writeStream.write(batchText + '\n');
          }
          
          if (onBatchGenerated) {
            await onBatchGenerated(batch, this.generatedCount);
          }
          
          isFirstBatch = false;
        }
        
        // Close the JSON array if needed
        if (this.options.dataType === 'json') {
          writeStream.write('\n]');
        }
        
        // Close the stream
        await new Promise((resolve, reject) => {
          writeStream.end(err => {
            if (err) reject(err);
            else resolve();
          });
        });
        
      } catch (error) {
        console.error('Error generating data:', error);
        throw error;
      }
      
    } else {
      // No output path, just generate and return the data
      const allData = [];
      
      // Generate data in batches
      for (let i = 0; i < totalCount; i += batchSize) {
        const currentBatchSize = Math.min(batchSize, totalCount - i);
        const batch = this._generateBatch(currentBatchSize);
        this.generatedCount += batch.length;
        
        allData.push(...batch);
        
        if (onBatchGenerated) {
          await onBatchGenerated(batch, this.generatedCount);
        }
      }
      
      return allData;
    }
    
    return this.generatedCount;
  }
  
  /**
   * Generate a single data record
   * @returns {Object} - Generated record
   */
  generateRecord() {
    const type = this._getRandomElement(this.dataTypes);
    const status = this._getRandomElement(this.statusTypes);
    const value = parseFloat((this.random() * 1000).toFixed(2));
    
    return {
      id: uuidv4(),
      type,
      name: `${type}_${Math.floor(this.random() * 1000)}`,
      value,
      status,
      timestamp: new Date().toISOString(),
      metadata: {
        source: 'generator',
        priority: Math.floor(this.random() * 5) + 1,
        tags: this._generateTags(),
        version: "1.0",
        nested: {
          detail1: this._generateWord(),
          detail2: this._generateWord(),
          metrics: {
            accuracy: parseFloat((this.random() * 100).toFixed(2)),
            confidence: parseFloat((this.random()).toFixed(4))
          }
        }
      }
    };
  }
  
  /**
   * Generate a batch of records
   * @param {number} batchSize - Number of records to generate
   * @returns {Array<Object>} - Array of generated records
   * @private
   */
  _generateBatch(batchSize) {
    const batch = [];
    
    for (let i = 0; i < batchSize; i++) {
      batch.push(this.generateRecord());
    }
    
    return batch;
  }
  
  /**
   * Create a seeded random number generator
   * @param {number} seed - Random seed
   * @returns {Function} - Random number generator function
   * @private
   */
  _createSeededRandom(seed) {
    return function() {
      seed = (seed * 9301 + 49297) % 233280;
      return seed / 233280;
    };
  }
  
  /**
   * Get random element from array
   * @param {Array} array - Source array
   * @returns {*} - Random element
   * @private
   */
  _getRandomElement(array) {
    return array[Math.floor(this.random() * array.length)];
  }
  
  /**
   * Generate random tags
   * @returns {Array<string>} - Array of tags
   * @private
   */
  _generateTags() {
    const tagCount = Math.floor(this.random() * 4) + 1;
    const possibleTags = ['important', 'test', 'dev', 'prod', 'critical', 'low', 'high', 'medium', 'batch', 'retry'];
    
    const tags = [];
    for (let i = 0; i < tagCount; i++) {
      tags.push(this._getRandomElement(possibleTags));
    }
    
    // Remove duplicates
    return [...new Set(tags)];
  }
  
  /**
   * Generate random word
   * @returns {string} - Random word
   * @private
   */
  _generateWord() {
    const words = [
      'data', 'process', 'record', 'entry', 'object', 
      'info', 'result', 'value', 'element', 'item',
      'status', 'metric', 'report', 'event', 'action'
    ];
    
    return this._getRandomElement(words);
  }
  
  /**
   * Static method to generate a single file
   * @param {Object} options - Generator options
   * @returns {Promise<string>} - Path to the generated file
   */
  static async generateFile(options) {
    const outputPath = options.outputPath || path.join(
      process.cwd(), 
      'data',
      `generated_${Date.now()}.${options.dataType === 'csv' ? 'csv' : 'json'}`
    );
    
    const generator = new DataGenerator({
      ...options,
      outputPath
    });
    
    await generator.generateData();
    return outputPath;
  }
}

module.exports = { DataGenerator };
