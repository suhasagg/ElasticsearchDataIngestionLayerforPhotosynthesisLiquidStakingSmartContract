// ingestion.js
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

// Configuration
const FILE_PATH =
  '/home/keanu-xbox/photov10/Photosynthesis-Dorahacks-web3-competition-winner/' +
  'photosynthesisv13/photosynthesis-main/sap-with-full-liquid-stake-redemption-workflow/' +
  'dockernet/logs/redemptionrate'; // Update with actual file path
const ELASTICSEARCH_HOST = 'http://localhost:9200';
const INDEX_NAME = 'smart-contract-liquid-staking-logs';

// Initialize Elasticsearch client
const esClient = new Client({ node: ELASTICSEARCH_HOST });

// Variables to hold the latest values
let redemptionRate = null;
let stakedBal = null;

// Variable to keep track of the last read position
let lastFileSize = 0;

// Function to ingest data into Elasticsearch
async function ingestData(index, data) {
  try {
    const response = await esClient.index({
      index: index,
      body: data,
    });
    console.log(`Indexed data into ${index}:`, response.body);
  } catch (error) {
    console.error('Elasticsearch indexing error:', error);
  }
}

// Function to parse a line and extract desired key-value pairs
function parseLine(line) {
  // Regular expression to match YAML key-value pairs
  const regex = /^\s*([\w_-]+):\s*(.*)$/;
  const match = line.match(regex);

  if (match) {
    let key = match[1].trim().toLowerCase();
    let value = match[2].trim();

    // Remove surrounding quotes if present
    if (value.startsWith('"') && value.endsWith('"')) {
      value = value.slice(1, -1);
    }

    // Convert value to float if possible
    const numericValue = parseFloat(value);

    // Only keep the desired keys
    if (key === 'redemption_rate') {
      if (!isNaN(numericValue)) {
        redemptionRate = numericValue;
        console.log('Found redemption_rate:', redemptionRate);
      }
    } else if (key === 'staked_bal') {
      if (!isNaN(numericValue)) {
        stakedBal = numericValue;
        console.log('Found staked_bal:', stakedBal);
      }
    }
  }
}

// Function to process new data appended to the file
function processNewData(newData) {
  const lines = newData.split('\n');

  for (let line of lines) {
    if (!line.trim()) continue;

    try {
      // Parse the line and extract desired data
      parseLine(line);

      // If both values are found, ingest data and reset variables
      if (redemptionRate !== null && stakedBal !== null) {
        const data = {
          redemption_rate: redemptionRate,
          staked_bal: stakedBal,
          timestamp: Math.floor(Date.now() / 1000),
        };

        console.log('Parsed data:', data);

        // Ingest data into Elasticsearch
        ingestData(INDEX_NAME, data);

        // Reset values to avoid duplicate ingestion
        redemptionRate = null;
        stakedBal = null;
      }
    } catch (error) {
      console.error('Error processing line:', line);
      console.error(error);
    }
  }
}

// Function to monitor the file for new data
function monitorFile(filePath) {
  // Initialize lastFileSize
  fs.stat(filePath, (err, stats) => {
    if (err) {
      console.error('Error getting file stats:', err);
      return;
    }
    lastFileSize = stats.size;
    console.log(`Initial file size: ${lastFileSize} bytes`);
  });

  // Watch the file for changes
  fs.watchFile(filePath, { interval: 1000 }, (curr, prev) => {
    if (curr.size > prev.size) {
      console.log(`File changed: ${filePath}`);

      const stream = fs.createReadStream(filePath, {
        start: lastFileSize,
        end: curr.size,
      });

      let newData = '';

      stream.on('data', (chunk) => {
        newData += chunk;
      });

      stream.on('end', () => {
        // Process the new data
        processNewData(newData);

        // Update lastFileSize
        lastFileSize = curr.size;
      });

      stream.on('error', (error) => {
        console.error('Error reading file:', error);
      });
    } else if (curr.size < prev.size) {
      // File was truncated or rotated
      console.log('File was truncated. Resetting lastFileSize to 0.');
      lastFileSize = 0;
    }
  });
}

// Start monitoring the file
monitorFile(FILE_PATH);

console.log('Monitoring started. Press Ctrl+C to exit.');

