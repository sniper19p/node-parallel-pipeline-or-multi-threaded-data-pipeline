# Node Parallel Pipeline

A high-performance, scalable data processing system built with Node.js Worker Threads for parallel data processing at scale.

> **Note:** This project was developed as part of a college course. The assignment required students to implement a multi-threaded data processing pipeline capable of handling large datasets with fault tolerance and real-time monitoring capabilities.
>
> **Assignment Requirements:**
> - Implement parallel processing using Node.js Worker Threads
> - Design a modular pipeline with configurable stages
> - Create a real-time monitoring dashboard
> - Handle at least 1 million records without performance degradation
> - Implement proper error handling and fault tolerance
> - Demonstrate thread pool management and backpressure handling
> - Document architecture and performance characteristics

## Features

- **Multi-threaded Architecture**: Leverages Node.js Worker Threads for true parallel processing
- **Dynamic Thread Pool**: Automatically adjusts the number of threads based on system resources
- **Configurable Pipeline Stages**: Filter, transform, and aggregate data with customizable operations
- **Backpressure Handling**: Prevents memory overloading with intelligent queue management
- **Real-time Monitoring**: Built-in monitoring of throughput, latency, and resource usage
- **Fault Tolerance**: Graceful error handling and automatic worker recovery
- **WebSocket Dashboard**: Live visualization of pipeline performance metrics
- **Multiple Data Formats**: Support for JSON, CSV, and custom data formats
- **Extensible Design**: Easily add custom data sources, sinks, and processing operations

## System Requirements

- Node.js 14.x or higher
- 4+ CPU cores recommended for optimal performance
- 8GB+ RAM for processing large datasets

## Installation

```bash
# Clone the repository
git clone https://github.com/sniper19p/node-parallel-pipeline-or-multi-threaded-data-pipeline
cd node-parallel-pipeline-or-multi-threaded-data-pipeline

# Install dependencies
npm install
```

## Quick Start

```bash
# Generate sample data and run the pipeline with default settings
npm start

# Or with custom configuration
node src/main.js --threads=8 --batchSize=1000 --inputFile=./data/large-dataset.json
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `--threads` | Number of worker threads | CPU cores - 1 |
| `--batchSize` | Number of records per batch | 1000 |
| `--inputFile` | Path to input data file | Auto-generated |
| `--outputFile` | Path to output data file | ./data/output.json |
| `--port` | Dashboard server port | 3000 |
| `--logLevel` | Logging level (debug, info, warn, error) | info |
| `--generateData` | Generate sample data | true |
| `--dataSize` | Number of records to generate | 1000000 |

## System Architecture

The pipeline consists of several modular components:

### Core Components

1. **ThreadPoolManager (`src/lib/threadPool.js`)**
   - Manages a pool of worker threads
   - Handles task distribution and load balancing
   - Provides fault tolerance with automatic worker restart
   - Implements backpressure handling

2. **Worker Implementation (`src/workers/worker.js`)**
   - Executes data processing tasks in separate threads
   - Implements filter, transform, and aggregate operations
   - Communicates with main thread via message passing
   - Handles errors and provides status updates

3. **DataSourceManager (`src/lib/dataSource.js`)**
   - Reads data from various sources (files, APIs, etc.)
   - Batches data for efficient processing
   - Implements streaming for large datasets

4. **DataSinkManager (`src/lib/dataSink.js`)**
   - Writes processed data to output destinations
   - Handles different output formats
   - Implements buffering for performance

5. **PipelineMonitor (`src/lib/monitor.js`)**
   - Tracks performance metrics in real-time
   - Emits events for status changes
   - Provides aggregated statistics

6. **DashboardServer (`src/server/server.js`)**
   - Serves real-time monitoring dashboard
   - Implements WebSocket communication
   - Visualizes pipeline performance

7. **Logger (`src/lib/logger.js`)**
   - Centralized logging system
   - Console and file transports
   - Log rotation and error tracking

### Data Flow

```
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌────────────┐
│ Data Source │ -> │ Thread Pool  │ -> │ Worker Threads  │ -> │ Data Sink  │
└─────────────┘    └──────────────┘    └─────────────────┘    └────────────┘
       │                  │                    │                    │
       │                  │                    │                    │
       └──────────────────┴────────────────────┴────────────────────┘
                                    │
                          ┌─────────────────────┐
                          │ Pipeline Monitor    │
                          └─────────────────────┘
                                    │
                          ┌─────────────────────┐
                          │ Dashboard Server    │
                          └─────────────────────┘
```

## Pipeline Stages

The worker threads process data through three configurable stages:

1. **Filter Stage**: Selectively include/exclude records based on criteria
   ```javascript
   // Example filter configuration
   {
     filterField: "status",
     filterValue: "active",
     filterOperator: "=="
   }
   ```

2. **Transform Stage**: Modify records with various operations
   ```javascript
   // Example transform configuration
   {
     transformations: [
       { targetField: "fullName", sourceField: "firstName", operation: "concat", value: " " },
       { targetField: "fullName", sourceField: "lastName", operation: "concat" },
       { targetField: "email", sourceField: "email", operation: "lowercase" }
     ]
   }
   ```

3. **Aggregate Stage**: Group and summarize data
   ```javascript
   // Example aggregate configuration
   {
     groupByField: "department",
     aggregations: [
       { field: "salary", operation: "avg", outputField: "averageSalary" },
       { field: "id", operation: "count", outputField: "employeeCount" }
     ]
   }
   ```

## Dashboard

Access the real-time monitoring dashboard at: http://localhost:3000 (or your configured port)

The dashboard provides:
- Current pipeline status
- Processing throughput (records/second)
- Worker thread utilization
- Error rates and types
- Memory usage
- Batch processing times
- Queue sizes

## Troubleshooting

### Common Issues

1. **Worker Threads Crashing**
   - Check that the worker.js file exists in src/workers/
   - Verify that all required dependencies are installed
   - Increase memory limit if processing large objects

2. **Pipeline Fails to Complete**
   - Ensure data source and sink are properly closed after processing
   - Check for uncaught exceptions in worker threads
   - Verify input data format matches expected schema

3. **Dashboard Not Showing Data**
   - Confirm the server is running on the expected port
   - Check browser console for WebSocket connection errors
   - Verify that the monitor is emitting events correctly

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details
