const WebSocket = require('ws');
const { Client } = require('@elastic/elasticsearch');
const winston = require('winston');

// Elasticsearch client setup
const esClient = new Client({ node: 'http://localhost:9200' });

// Logger setup
const logger = winston.createLogger({
  level: 'info', // Ensure this is set to 'info'
  format: winston.format.json(),
  defaultMeta: { service: 'event-listener' },
  transports: [
    new winston.transports.File({ filename: 'event-listener-error.log', level: 'error' }),
    new winston.transports.File({ filename: 'event-listener-combined.log' }),
    new winston.transports.Console(), // Optional: Log to console
  ],
});

// Configuration
const WEBSOCKET_URL = 'ws://localhost:26457/websocket';
const CONTRACT_ADDRESS = 'archway14hj2tavq8fpesdwxxcu44rty3hh90vhujrvcmstl4zr3txmfvw9sy85n2u';

function connectWebSocket() {
  const ws = new WebSocket(WEBSOCKET_URL);

  ws.on('open', () => {
    logger.info('WebSocket connection opened.');

    const subscribeMessage = JSON.stringify({
      jsonrpc: '2.0',
      method: 'subscribe',
      id: '1',
      params: {
        query: `tm.event='Tx' AND wasm._contract_address='${CONTRACT_ADDRESS}'`,
      },
    });

    ws.send(subscribeMessage);
  });

  ws.on('message', async (data) => {
    try {
      // Convert data to a string
      const dataString = data.toString();

      // Log the raw data received from the WebSocket
      logger.info('Received data:', dataString);

      // Parse the JSON string
      const parsedData = JSON.parse(dataString);

      // Log the parsed data
      logger.info('Parsed WebSocket Message:', parsedData);

      if (parsedData.result && parsedData.result.events) {
        const events = parsedData.result.events;

       // const eventDataArray = parseEvents(events, parsedData.result);

       // for (const eventData of eventDataArray) {
       //   await indexEvent(eventData);
       // }
      } else {
        // Log the parsed data if it doesn't contain events
        logger.warn('Received message without events:', parsedData);
      }
    } catch (error) {
      logger.error('Error processing message:', error);
      // Log the raw message data
      logger.error('Raw message data:', data.toString());
    }
  });

  ws.on('error', (error) => {
    logger.error('WebSocket error:', error);
  });

  ws.on('close', () => {
    logger.warn('WebSocket connection closed. Reconnecting in 5 seconds...');
    setTimeout(connectWebSocket, 5000);
  });
}

connectWebSocket();

function parseEvents(events, result) {
  const eventDataArray = [];

  // Extract common data
  const blockHeight = parseInt(result.data.value.TxResult.height);
  const txHash = Buffer.from(result.data.value.TxResult.tx).toString('hex');
  const timestamp = Date.now() / 1000; // Use current time as timestamp

  // Process 'wasm' events
  if (events.wasm) {
    const attributes = events.wasm;

    for (let i = 0; i < attributes['action'].length; i++) {
      const eventData = {
        block_height: blockHeight,
        tx_hash: txHash,
        timestamp: timestamp,
      };

      for (const key in attributes) {
        const value = attributes[key][i];
        eventData[key] = value;
      }

      eventDataArray.push(eventData);
    }
  }

  return eventDataArray;
}

async function indexEvent(eventData) {
  try {
    await esClient.index({
      index: 'smart-contract-events',
      body: eventData,
    });

    logger.info('Indexed event:', eventData);
  } catch (error) {
    logger.error('Error indexing event:', error);
  }
}

