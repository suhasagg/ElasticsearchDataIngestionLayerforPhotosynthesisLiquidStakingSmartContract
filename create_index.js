const { Client } = require('@elastic/elasticsearch');
const esClient = new Client({ node: 'http://localhost:9200' });

async function createIndex() {
  const indexName = 'smart-contract-liquid-staking-logs';

  // Check if index exists
  const exists = await esClient.indices.exists({ index: indexName });

  if (exists.body) {
    console.log(`Index "${indexName}" already exists.`);
    return;
  }

  // Create index with mapping
  await esClient.indices.create({
    index: indexName,
    body: {
      mappings: {
        properties: {         
          action: { type: 'keyword' },
          sender: { type: 'keyword' },
          owner: { type: 'keyword' },
          amount: { type: 'keyword' },
          liquid_staking_interval: { type: 'long' },
          arch_liquid_stake_interval: { type: 'long' },
          redemption_rate_query_interval: { type: 'long' },
          rewards_withdrawal_interval: { type: 'long' },
          redemption_interval_threshold: { type: 'long' },
          block_height: { type: 'long' },
          timestamp: { type: 'date', format: 'epoch_second' },
          rewards_address: { type: 'keyword' },
          reward_amount: { type: 'double' },
          pending_deposit_record_amount: { type: 'double' },
          completed_deposit_record_amount: { type: 'double' },
          stake_amount: { type: 'double' },
          redeem_tokens_amount: { type: 'double' },
          liquidity_amount: { type: 'double' },
          liquidity_tokens_amount: { type: 'double' },
          updates: { type: 'text' },
          contract_address: { type: 'keyword' },
          liquidity_provider_address: { type: 'keyword' },
          minimum_reward_amount: { type: 'double' },
          maximum_reward_amount: { type: 'double' },
          reward_address: { type: 'keyword' },
          processed_tasks: { type: 'text' },
          deposit_record_id: { type: 'long' },
          deposit_record_status: { type: 'keyword' },
          total_liquid_stake: { type: 'double' },
          stuarch_obtained: { type: 'double' },  
          redemption_rate: { type: 'double' },
          time_since_latest_redemption: { type: 'long' },
          action_taken: { type: 'keyword' },
          total_rewards: { type: 'double' },
          num_records: { type: 'long' },
          stake_proportion: { type: 'double' },
          liquidity_tokens_amount: { type: 'double' },
          tokens_amount: { type: 'double' },
          remaining_records: { type: 'long' },
          tx_hash: { type: 'keyword' },
          liquidity_address: { type: 'keyword' }, 
          status: { type: 'keyword' },            
          method: { type: 'keyword' },           
          address: { type: 'keyword' },
          redemption_ratio: { type: 'double' },
          total_redeem_tokens: { type: 'long' },
          redeem_amount: { type: 'long' },
          epoch_number: { type: 'long' },
          host_zone_id:{ type:'keyword' },
          native_token_amount:  { type: 'double' },
          st_token_amount:  { type: 'double' },
          status: { type:'keyword' },
          unbonding_time:  { type: 'long' },
          user_redemption_records: { type:'keyword' },        
          bech32prefix: { type:'keyword' },
          chain_id: { type:'keyword' },
          connection_id: { type:'keyword' },
          halted: { type: 'boolean' },
          host_denom: { type:'keyword' },
          ibc_denom: { type:'keyword' },
          last_redemption_rate: { type:'double' },
          max_redemption_rate: { type:'double' },
          min_redemption_rate: { type:'double' },
          redemption_account: {
           properties: {
           address: { type:'keyword' },
           target: { type:'keyword' }
          }
        },
         redemption_rate: { type:'double' },
         staked_bal: { type: 'long' },
         transfer_channel_id: { type:'keyword' },
         unbonding_frequency: { type: 'integer' },
         withdrawal_account: {
          properties: {
          address: { type:'keyword' },
          target: { type:'keyword' }
        }
      },
        },
      },
    },
  });

  console.log(`Index "${indexName}" created successfully.`);
}

createIndex().catch((error) => {
  console.error('Error creating index:', error);
});

