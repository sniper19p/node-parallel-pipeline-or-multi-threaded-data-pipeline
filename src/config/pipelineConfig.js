/**
 * Pipeline Configuration
 * Manages configuration settings for the data processing pipeline
 */

const os = require('os');
const path = require('path');
const fs = require('fs-extra');
const dotenv = require('dotenv');

// Load environment variables from .env file if present
dotenv.config();

class PipelineConfig {
  constructor(options = {}) {
    // Default configuration
    this.defaults = {
      // Thread settings
      threads: Math.max(1, os.cpus().length - 1),
      
      // Data processing settings
      batchSize: 1000,
      inputFile: null,
      outputFile: './output/processed_data.json',
      
      // Test data generation
      generateData: !options.inputFile,
      dataSize: 100000,
      
      // Monitoring settings
      port: 3000,
      monitorInterval: 1000,
      
      // Processing stages
      stages: [
        { name: 'filter', timeout: 5000 },
        { name: 'transform', timeout: 10000 },
        { name: 'aggregate', timeout: 15000 }
      ],
      
      // Stage options
      stageOptions: {
        filter: {
          // Default filter options
        },
        transform: {
          addTimestamp: true,
          addWorkerId: true
        },
        aggregate: {
          // Default aggregation options
        }
      },
      
      // Performance settings
      simulateLoad: false,
      workerAffinityEnabled: false,
      
      // File paths
      baseDir: process.cwd(),
      dataDir: './data',
      outputDir: './output',
      configFile: './config/pipeline.json'
    };
    
    // Override defaults with provided options
    this.config = {
      ...this.defaults,
      ...options
    };
    
    // Ensure directories exist
    this._ensureDirectories();
    
    // Load from config file if specified and exists
    if (options.configFile) {
      this.loadFromFile(options.configFile);
    } else if (fs.existsSync(path.join(process.cwd(), this.defaults.configFile))) {
      this.loadFromFile(path.join(process.cwd(), this.defaults.configFile));
    }
    
    // Override with environment variables
    this._loadFromEnv();
  }

  /**
   * Get a configuration value
   * @param {string} key - Configuration key
   * @param {*} defaultValue - Default value if key not found
   * @returns {*} - Configuration value
   */
  get(key, defaultValue) {
    return key in this.config ? this.config[key] : defaultValue;
  }

  /**
   * Set a configuration value
   * @param {string} key - Configuration key
   * @param {*} value - Configuration value
   */
  set(key, value) {
    this.config[key] = value;
  }

  /**
   * Check if configuration has a key
   * @param {string} key - Configuration key
   * @returns {boolean} - True if key exists
   */
  has(key) {
    return key in this.config;
  }

  /**
   * Load configuration from a file
   * @param {string} filePath - Path to configuration file
   * @returns {Object} - Loaded configuration
   */
  loadFromFile(filePath) {
    try {
      if (!fs.existsSync(filePath)) {
        return this.config;
      }
      
      const fileConfig = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      
      // Merge with existing config
      this.config = {
        ...this.config,
        ...fileConfig
      };
      
      return this.config;
    } catch (error) {
      console.error(`Error loading config from ${filePath}:`, error.message);
      return this.config;
    }
  }

  /**
   * Save configuration to a file
   * @param {string} filePath - Path to save configuration
   * @returns {boolean} - True if saved successfully
   */
  saveToFile(filePath = null) {
    const savePath = filePath || path.join(process.cwd(), this.defaults.configFile);
    
    try {
      // Ensure directory exists
      fs.ensureDirSync(path.dirname(savePath));
      
      // Write config to file
      fs.writeFileSync(savePath, JSON.stringify(this.config, null, 2), 'utf8');
      return true;
    } catch (error) {
      console.error(`Error saving config to ${savePath}:`, error.message);
      return false;
    }
  }

  /**
   * Load configuration from environment variables
   * @private
   */
  _loadFromEnv() {
    // Map of environment variable names to config keys
    const envMap = {
      PIPELINE_THREADS: { key: 'threads', transform: Number },
      PIPELINE_BATCH_SIZE: { key: 'batchSize', transform: Number },
      PIPELINE_INPUT_FILE: { key: 'inputFile' },
      PIPELINE_OUTPUT_FILE: { key: 'outputFile' },
      PIPELINE_GENERATE_DATA: { key: 'generateData', transform: val => val === 'true' },
      PIPELINE_DATA_SIZE: { key: 'dataSize', transform: Number },
      PIPELINE_PORT: { key: 'port', transform: Number },
      PIPELINE_MONITOR_INTERVAL: { key: 'monitorInterval', transform: Number },
      PIPELINE_SIMULATE_LOAD: { key: 'simulateLoad', transform: val => val === 'true' }
    };
    
    // Apply environment variables
    for (const [envVar, config] of Object.entries(envMap)) {
      if (process.env[envVar] !== undefined) {
        const value = config.transform 
          ? config.transform(process.env[envVar])
          : process.env[envVar];
        
        this.config[config.key] = value;
      }
    }
  }

  /**
   * Ensure required directories exist
   * @private
   */
  _ensureDirectories() {
    try {
      // Ensure data directory exists
      fs.ensureDirSync(path.join(this.config.baseDir, this.config.dataDir));
      
      // Ensure output directory exists
      fs.ensureDirSync(path.join(this.config.baseDir, this.config.outputDir));
      
      // Ensure config directory exists
      fs.ensureDirSync(path.join(this.config.baseDir, path.dirname(this.config.configFile)));
    } catch (error) {
      console.error('Error creating directories:', error.message);
    }
  }

  /**
   * Get full configuration object
   * @returns {Object} - Full configuration
   */
  getAll() {
    return { ...this.config };
  }

  /**
   * Get configuration for a specific processing stage
   * @param {string} stageName - Stage name
   * @returns {Object} - Stage configuration
   */
  getStageConfig(stageName) {
    const stage = this.config.stages.find(s => s.name === stageName);
    const options = this.config.stageOptions?.[stageName] || {};
    
    return {
      ...(stage || {}),
      options
    };
  }
}

// Make properties accessible directly
// For example: config.threads instead of config.get('threads')
Object.defineProperties(PipelineConfig.prototype, {
  threads: {
    get() { return this.config.threads; },
    set(value) { this.config.threads = value; }
  },
  batchSize: {
    get() { return this.config.batchSize; },
    set(value) { this.config.batchSize = value; }
  },
  inputFile: {
    get() { return this.config.inputFile; },
    set(value) { this.config.inputFile = value; }
  },
  outputFile: {
    get() { return this.config.outputFile; },
    set(value) { this.config.outputFile = value; }
  },
  generateData: {
    get() { return this.config.generateData; },
    set(value) { this.config.generateData = value; }
  },
  dataSize: {
    get() { return this.config.dataSize; },
    set(value) { this.config.dataSize = value; }
  },
  port: {
    get() { return this.config.port; },
    set(value) { this.config.port = value; }
  },
  monitorInterval: {
    get() { return this.config.monitorInterval; },
    set(value) { this.config.monitorInterval = value; }
  },
  stages: {
    get() { return [...this.config.stages]; },
    set(value) { this.config.stages = [...value]; }
  },
  stageOptions: {
    get() { return { ...this.config.stageOptions }; },
    set(value) { this.config.stageOptions = { ...value }; }
  }
});

module.exports = { PipelineConfig };
