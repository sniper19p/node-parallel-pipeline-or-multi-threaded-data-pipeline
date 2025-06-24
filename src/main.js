/**
 * Multi-Threaded Data Processing Pipeline
 * Main Coordinator Module
 */

const path = require('path');
const os = require('os');
const { program } = require('commander');
const { ThreadPoolManager } = require('./lib/threadPool');
const { DataSourceManager } = require('./lib/dataSource');
const { DataSinkManager } = require('./lib/dataSink');
const { PipelineMonitor } = require('./lib/monitor');
const { DashboardServer } = require('./server/server');
const logger = require('./lib/logger');
const { DataGenerator } = require('./lib/dataGenerator');
const { PipelineConfig } = require('./config/pipelineConfig');

// Parse command line arguments
program
  .option('-t, --threads <number>', 'Number of worker threads', Math.max(1, os.cpus().length - 1))
  .option('-b, --batchSize <number>', 'Batch size for processing', 1000)
  .option('-i, --inputFile <path>', 'Input file path')
  .option('-o, --outputFile <path>', 'Output file path', './output/processed_data.json')
  .option('-g, --generateData', 'Generate sample data', false)
  .option('-s, --dataSize <number>', 'Size of generated data (records)', 1000000)
  .option('-p, --port <number>', 'Dashboard server port', 3000)
  .option('-m, --monitorInterval <number>', 'Monitoring update interval (ms)', 1000)
  .parse(process.argv);

const options = program.opts();

// Configure the pipeline
const config = new PipelineConfig({
  threads: parseInt(options.threads, 10),
  batchSize: parseInt(options.batchSize, 10),
  inputFile: options.inputFile,
  outputFile: options.outputFile,
  generateData: options.generateData || !options.inputFile,
  dataSize: parseInt(options.dataSize, 10),
  port: parseInt(options.port, 10),
  monitorInterval: parseInt(options.monitorInterval, 10),
  stages: [
    { name: 'filter', timeout: 100 },
    { name: 'transform', timeout: 150 },
    { name: 'aggregate', timeout: 200 }
  ]
});

// Initialize components
async function initialize() {
  logger.info(`Initializing pipeline with ${config.threads} threads`);
  
  // Generate sample data if requested or if no input file provided
  if (config.generateData) {
    logger.info(`Generating ${config.dataSize} sample records...`);
    const outputPath = path.join(process.cwd(), 'data', 'sample_data.json');
    await DataGenerator.generateFile({
      count: config.dataSize,
      outputPath,
      dataType: 'json'
    });
    config.inputFile = outputPath;
    logger.info(`Sample data generated at ${outputPath}`);
  }

  // Create monitoring system
  const monitor = new PipelineMonitor(config.monitorInterval);
  
  // Initialize thread pool
  const threadPool = new ThreadPoolManager({
    threadCount: config.threads,
    monitor,
    config
  });
  
  // Initialize data source
  const dataSource = new DataSourceManager({
    inputFile: config.inputFile,
    batchSize: config.batchSize,
    monitor
  });
  
  // Initialize data sink
  const dataSink = new DataSinkManager({
    outputFile: config.outputFile,
    monitor
  });
  
  // Setup server for dashboard
  const server = new DashboardServer(monitor, config.port);
  await server.start();
  
  return {
    threadPool,
    dataSource,
    dataSink,
    monitor,
    server
  };
}

// Execute the pipeline
async function runPipeline() {
  try {
    logger.info('Starting data processing pipeline...');
    
    // Initialize pipeline components
    const { threadPool, dataSource, dataSink, monitor, server } = await initialize();
    
    // Setup event handlers
    setupEventHandlers(threadPool, dataSource, dataSink, monitor, server);
    
    // Start the pipeline
    await startPipeline(threadPool, dataSource, dataSink, monitor);
    
    logger.info('Pipeline completed successfully');
    
    // Keep the server running for monitoring
    logger.info(`Dashboard available at http://localhost:${config.port}`);
  } catch (error) {
    logger.error('Pipeline failed', { error: error.message, stack: error.stack });
    process.exit(1);
  }
}

// Set up event handlers for graceful shutdown
function setupEventHandlers(threadPool, dataSource, dataSink, monitor, server) {
  const shutdown = async () => {
    logger.info('Shutting down pipeline...');
    await threadPool.shutdown();
    await dataSource.close();
    await dataSink.close();
    monitor.stop();
    if (server) {
      await server.stop();
    }
    process.exit(0);
  };
  
  // Handle process termination
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception', { error: error.message, stack: error.stack });
    shutdown();
  });
}

// Start the pipeline processing
async function startPipeline(threadPool, dataSource, dataSink, monitor) {
  try {
    // Start the monitoring system
    monitor.start();
    
    // Initialize thread pool
    await threadPool.initialize();
    
    // Open data source
    await dataSource.open();
    
    // Open data sink
    await dataSink.open();
    
    // Process data in batches
    let batchCount = 0;
    let recordCount = 0;
    
    monitor.setStatus('running');
    
    // Process data until source is exhausted
    while (true) {
      // Get next batch from source
      const batch = await dataSource.getNextBatch();
      
      // Exit if no more data
      if (!batch || batch.length === 0) {
        break;
      }
      
      batchCount++;
      recordCount += batch.length;
      
      // Update monitoring
      monitor.updateMetric('batchesProcessed', batchCount);
      monitor.updateMetric('recordsProcessed', recordCount);
      
      // Process the batch with thread pool
      const results = await threadPool.processBatch(batch);
      
      // Write processed results
      await dataSink.write(results);
    }
    
    monitor.setStatus('completed');
    
    logger.info(`Pipeline processing complete. Processed ${batchCount} batches, ${recordCount} records.`);
    
    // Ensure all data is flushed
    await dataSink.flush();
    
    // Properly close data source and sink
    await dataSource.close();
    await dataSink.close();
  } catch (error) {
    monitor.setStatus('failed');
    throw error;
  } finally {
    // Final metrics update
    monitor.updateMetric('totalBatches', batchCount);
    monitor.finalize();
  }
}

// Run the pipeline
if (require.main === module) {
  runPipeline();
}

module.exports = { runPipeline };
