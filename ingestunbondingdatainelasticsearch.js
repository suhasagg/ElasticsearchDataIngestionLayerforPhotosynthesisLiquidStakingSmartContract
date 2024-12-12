// ingestion.js
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

// Configuration
const FILE_PATH = '/home/keanu-xbox/photov10/Photosynthesis-Dorahacks-web3-competition-winner/photosynthesisv13/photosynthesis-main/sap-with-full-liquid-stake-redemption-workflow/unbondingdata'; 
const ELASTICSEARCH_HOST = 'http://localhost:9200';
const INDEX_NAME = 'smart-contract-liquid-staking-logs';

// Initialize Elasticsearch client
const esClient = new Client({ node: ELASTICSEARCH_HOST });

// Keys we care about
const KEYS = ['status', 'native_token_amount', 'st_token_amount', 'epoch_number', 'host_zone_id'];

// State variables
let lastFileSize = 0;
let currentEpochNumber = null;
let currentRecord = {}; // Holds keys for the current host_zone_unbondings block

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

// Modified regex to allow optional leading dash for keys
const keyValueRegex = /^\s*-?\s*([\w_-]+):\s*(.*)$/;

// Reset currentRecord keys except we keep them separate if needed
function resetHostZoneBlock() {
  currentRecord = {};
}

// Check if we have all required keys in currentRecord
function haveAllKeys() {
  for (const k of KEYS) {
    if (!currentRecord[k]) return false;
  }
  return true;
}

function processKeyValue(key, value) {
  // Store the key-value pair if it's one we care about
  if (KEYS.includes(key)) {
    currentRecord[key] = value;
  }

  // If we got 'epoch_number', store as currentEpochNumber (applies to future blocks)
  if (key === 'epoch_number') {
    currentEpochNumber = value; 
    // Also store epoch_number in currentRecord so it's used for the next block
    currentRecord['epoch_number'] = value;
  }

  // If we encounter 'status', it likely means we reached the end of a host_zone_unbondings block
  // Check if we have all keys and ingest
  if (key === 'status') {
    // Ensure epoch_number is in currentRecord
    if (!currentRecord['epoch_number'] && currentEpochNumber) {
      currentRecord['epoch_number'] = currentEpochNumber;
    }

    if (haveAllKeys()) {
      // Ingest doc
      const doc = { ...currentRecord, timestamp: Math.floor(Date.now() / 1000) };
      console.log('Ingesting doc:', doc);
      ingestData(INDEX_NAME, doc);
      // Reset for next block
      resetHostZoneBlock();
      // Keep epoch_number from last block if needed
      if (currentEpochNumber) {
        currentRecord['epoch_number'] = currentEpochNumber;
      }
    }
  }
}

function processLine(line) {
  // match lines like "key: value"
  const match = line.match(keyValueRegex);
  if (match) {
    let key = match[1].trim().toLowerCase();
    let value = match[2].trim();
    if (value.startsWith('"') && value.endsWith('"')) {
      value = value.slice(1, -1);
    }

    // Print key-value
    console.log(`Key: ${key}, Value: ${value}`);

    // If line starts a new host_zone_unbondings block
    // Usually indicated by lines like "- denom: uarch"
    // We'll treat the appearance of 'denom:' as start of a new block
    // You can also detect start of a block by other means
    // For safety, if we see 'denom', start a new block (this might not always be needed)
    if (key === 'denom') {
      resetHostZoneBlock();
      // Make sure epoch_number is known for this block
      if (currentEpochNumber) {
        currentRecord['epoch_number'] = currentEpochNumber;
      }
    }

    // Store/process the key-value
    processKeyValue(key, value);
  }
}

function readNewData(filePath, newSize) {
  const stream = fs.createReadStream(filePath, {
    start: lastFileSize,
    end: newSize,
  });

  let newData = '';
  stream.on('data', (chunk) => {
    newData += chunk;
  });

  stream.on('end', () => {
    const lines = newData.split('\n');
    for (const line of lines) {
      if (line.trim()) {
        processLine(line);
      }
    }
    lastFileSize = newSize;
  });

  stream.on('error', (error) => {
    console.error('Error reading file:', error);
  });
}

function monitorFile(filePath) {
  fs.stat(filePath, (err, stats) => {
    if (err) {
      console.error('Error getting file stats:', err);
      return;
    }
    // Start from beginning of file to process all existing data once
    lastFileSize = 0;
    console.log(`Initial file size: ${stats.size} bytes`);
    if (stats.size > 0) {
      // Process existing file content
      readNewData(filePath, stats.size);
    }
  });

  fs.watchFile(filePath, { interval: 1000 }, (curr, prev) => {
    if (curr.size > prev.size) {
      console.log(`File changed: ${filePath}`);
      readNewData(filePath, curr.size);
    } else if (curr.size < prev.size) {
      console.log('File was truncated. Resetting lastFileSize to 0.');
      lastFileSize = 0;
    }
  });
}

// Start monitoring the file
monitorFile(FILE_PATH);
console.log('Monitoring started. Press Ctrl+C to exit.');

