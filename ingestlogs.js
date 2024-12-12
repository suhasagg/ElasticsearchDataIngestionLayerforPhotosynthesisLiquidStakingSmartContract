// ingestion.js
const fs = require('fs');
const { Tail } = require('tail');
const { Client } = require('@elastic/elasticsearch');

const esClient = new Client({ node: 'http://localhost:9200' });
const indexName = 'smart-contract-liquid-staking-logs';

async function ingestLine(line) {
  if (!line.trim()) return;

  try {
    const logEntry = JSON.parse(line);

    if (
      logEntry.message &&
      logEntry.message.startsWith('Parsed WebSocket Message:')
    ) {
      const result = logEntry.result;
      if (
        result &&
        result.data &&
        result.data.value &&
        result.data.value.TxResult
      ) {
        const txResult = result.data.value.TxResult;
        const events = txResult.result.events || [];
        const txHash = txResult.hash || '';
        const txHeight = txResult.height || '';
        const txTimestamp = extractTimestampFromResult(txResult);

        for (const event of events) {
          const eventType = event.type;
          const attributesList = event.attributes || [];

          await processEvent(
            eventType,
            attributesList,
            txHash,
            txHeight,
            txTimestamp
          );
        }
      }
    }
  } catch (error) {
    console.error('Error processing line:', line);
    console.error(error);
  }
}

async function processEvent(
  eventType,
  attributesList,
  txHash,
  txHeight,
  txTimestamp
) {
  // Map attributes to index fields
  const eventDoc = {
    event_type: eventType,
    tx_hash: txHash,
    block_height: parseInt(txHeight),
    timestamp: txTimestamp,
  };

  if (eventType === 'wasm-handle_liquid_staking_dapp_rewards') {
    // Group attributes into records
    const records = [];
    let currentRecord = {};

    for (const attr of attributesList) {
      const key = Buffer.from(attr.key, 'base64').toString('utf8');
      const value = Buffer.from(attr.value, 'base64').toString('utf8');

      if (key === '_contract_address' && Object.keys(currentRecord).length > 0) {
        // Start of a new record
        records.push(currentRecord);
        currentRecord = {};
      }
      currentRecord[key] = value;
    }
    // Add the last record
    if (Object.keys(currentRecord).length > 0) {
      records.push(currentRecord);
    }

    // Create deposit record documents
    for (const record of records) {
      const depositRecordDoc = {
        deposit_record_id: record['deposit_record_id']
          ? parseInt(record['deposit_record_id'])
          : null,
        deposit_record_status: record['deposit_record_status'] || null,
        pending_deposit_record_amount: record['pending_deposit_record_amount'] ? parseFloat(record['pending_deposit_record_amount']) : null,
        completed_deposit_record_amount: record['completed_deposit_record_amount'] ? parseFloat(record['completed_deposit_record_amount']) : null,
        reward_address: record['reward_address'] || null,
        contract_address: record['contract_address'] || null,
        timestamp: record['timestamp'] ? parseInt(record['timestamp']) : null,
        block_height: record['block_height']
          ? parseInt(record['block_height'])
          : null,
        tx_hash: txHash,
      };

      // Print the deposit record document
      console.log('Indexing deposit record document:', depositRecordDoc);

      // Index deposit record document
      await esClient.index({
        index: indexName,
        body: depositRecordDoc,
      });
    }

    // Create event document without deposit record fields
    const eventAttributes = {};
    for (const record of records) {
      for (const [key, value] of Object.entries(record)) {
        if (
          ![
            'deposit_record_id',
            'deposit_record_status',
            'pending_deposit_record_amount',
            'completed_deposit_record_amount',
            'reward_address',
            'contract_address',
            'timestamp',
            'block_height',
            '_contract_address',
          ].includes(key)
        ) {
          eventAttributes[key] = value;
        }
      }
    }
    Object.assign(eventDoc, eventAttributes);

    // Print the event document
    console.log('Indexing event document:', eventDoc);

    // Index event document
    await esClient.index({
      index: indexName,
      body: eventDoc,
    });
  } else {
    // For other events, create a single document
    const decodedAttributes = {};
    for (const attr of attributesList) {
      const key = Buffer.from(attr.key, 'base64').toString('utf8');
      const value = Buffer.from(attr.value, 'base64').toString('utf8');
      decodedAttributes[key] = value;
    }

    Object.assign(eventDoc, decodedAttributes);

    // Convert fields to correct types according to mapping
    if (eventDoc['timestamp']) {
      eventDoc['timestamp'] = parseInt(eventDoc['timestamp']);
    }
    if (eventDoc['block_height']) {
      eventDoc['block_height'] = parseInt(eventDoc['block_height']);
    }
    if (eventDoc['deposit_record_amount']) {
      eventDoc['deposit_record_amount'] = parseFloat(eventDoc['deposit_record_amount']);
    }

    // Print the event document
    console.log('Indexing event document:', eventDoc);

    // Index event document
    await esClient.index({
      index: indexName,
      body: eventDoc,
    });
  }
}

function extractTimestampFromResult(txResult) {
  // Extract timestamp from txResult if available
  // This depends on where the timestamp is in the data
  return null;
}

// Replace with the actual path to log file
const logFilePath = '/home/keanu-xbox/photov10/Photosynthesis-Dorahacks-web3-competition-winner/photosynthesisv13/photosynthesis-main/sap-with-full-liquid-stake-redemption-workflow/event-listener-combined.log';

function startLogFileMonitoring(filePath) {
  const tail = new Tail(filePath, {
    fromBeginning: false, // Set to true if you want to read from the beginning of the file
    useWatchFile: true,    // Fallback to fs.watchFile if needed
  });

  tail.on('line', (line) => {
    ingestLine(line);
  });

  tail.on('error', (error) => {
    console.error('Error tailing file:', error);
  });

  console.log(`Started monitoring log file: ${filePath}`);
}

startLogFileMonitoring(logFilePath);

