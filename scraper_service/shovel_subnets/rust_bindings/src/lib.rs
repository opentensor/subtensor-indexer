use deadpool::unmanaged;
use futures::future::join_all;
use parity_scale_codec::Decode;
use pyo3::prelude::*;
use std::{collections::HashMap, sync::Arc};
use subxt::{
    backend::{legacy::LegacyBackend, rpc::RpcClient},
    utils::H256,
    OnlineClient, PolkadotConfig,
};
use tokio;

#[subxt::subxt(runtime_metadata_path = "./metadata.scale")]
pub mod subtensor {}

#[pyclass]
#[derive(Debug, Default)]
#[allow(unused)]
struct NeuronInfo {
    subnet_id: u16,
    neuron_id: u16,

    block_hash: String,
    hotkey: String,
    pub active: bool,

    pub rank: u16,
    pub emission: u64,
    pub incentive: u16,
    pub consensus: u16,
    pub trust: u16,
    pub validator_trust: u16,
    pub dividends: u16,
    pub weights: Vec<(u16, u16)>,

    pub last_update: u64,
    pub pruning_scores: u16,
}

macro_rules! fetch_and_process_kvs {
    ($api_pool:expr, $block_hash:expr, $query:expr, $label:expr) => {{
        async {
            let api = $api_pool.get().await.unwrap();
            let start = std::time::Instant::now();
            let mut iter = api.storage().at($block_hash).iter($query).await.unwrap();
            let mut kvs = Vec::new();

            while let Some(Ok(kv)) = iter.next().await {
                kvs.push((kv.key_bytes, kv.value));
            }
            let end = std::time::Instant::now();
            println!("{} Time elapsed: {:?}", $label, end - start);

            kvs
        }
    }};
}

async fn get_api() -> OnlineClient<PolkadotConfig> {
    let url = std::env::var("SUBSTRATE_ARCHIVE_NODE_URL").unwrap();
    let client = RpcClient::from_insecure_url(url.clone())
        .await
        .expect("Failed to connect to node");
    let backend = LegacyBackend::<PolkadotConfig>::builder()
        .storage_page_size(1000)
        .build(client);
    OnlineClient::from_backend(Arc::new(backend)).await.unwrap()
}

/// Formats the sum of two numbers as string.
async fn query_neuron_info_inner(block_hash: String) -> PyResult<(Vec<NeuronInfo>, Vec<String>)> {
    let start = std::time::Instant::now();
    let block_hash = hex::decode(block_hash.trim_start_matches("0x")).expect("Decoding failed");
    let block_hash = H256::from_slice(&block_hash);

    let keys_query = subtensor::storage().subtensor_module().keys_iter();
    let active_query = subtensor::storage().subtensor_module().active_iter();
    let rank_query = subtensor::storage().subtensor_module().rank_iter();
    let trust_query = subtensor::storage().subtensor_module().trust_iter();
    let emission_query = subtensor::storage().subtensor_module().emission_iter();
    let consensus_query = subtensor::storage().subtensor_module().consensus_iter();
    let incentive_query = subtensor::storage().subtensor_module().incentive_iter();
    let dividends_query = subtensor::storage().subtensor_module().dividends_iter();
    let last_update_query = subtensor::storage().subtensor_module().last_update_iter();
    let pruning_scores_query = subtensor::storage()
        .subtensor_module()
        .pruning_scores_iter();
    let validator_trust_query = subtensor::storage()
        .subtensor_module()
        .validator_trust_iter();
    let weights_query = subtensor::storage().subtensor_module().weights_iter();

    // not important
    // let bonds_query = subtensor::storage().subtensor_module().bonds_iter();
    // let stake_query = subtensor::storage().subtensor_module().stake_iter();
    // let prometheus_query = subtensor::storage().subtensor_module().prometheus_iter();

    // axons are scraped seperately (too slow for every block)
    // let axons_query = subtensor::storage().subtensor_module().axons_iter();

    let concurrency = 32;
    let api_futures = (0..concurrency).map(|_| get_api()).collect::<Vec<_>>();
    let api_pool = join_all(api_futures).await.into_iter().collect::<Vec<_>>();
    let api_pool = unmanaged::Pool::from(api_pool);

    let (
        keys_kvs,
        active_kvs,
        rank_kvs,
        trust_kvs,
        emission_kvs,
        consensus_kvs,
        incentive_kvs,
        dividends_kvs,
        last_update_kvs,
        pruning_scores_kvs,
        validator_trust_kvs,
        weights_kvs,
    ) = tokio::join!(
        fetch_and_process_kvs!(&api_pool, block_hash, keys_query, "keys"),
        fetch_and_process_kvs!(&api_pool, block_hash, active_query, "active"),
        fetch_and_process_kvs!(&api_pool, block_hash, rank_query, "rank"),
        fetch_and_process_kvs!(&api_pool, block_hash, trust_query, "trust"),
        fetch_and_process_kvs!(&api_pool, block_hash, emission_query, "emission"),
        fetch_and_process_kvs!(&api_pool, block_hash, consensus_query, "consensus"),
        fetch_and_process_kvs!(&api_pool, block_hash, incentive_query, "incentive"),
        fetch_and_process_kvs!(&api_pool, block_hash, dividends_query, "dividends"),
        fetch_and_process_kvs!(&api_pool, block_hash, last_update_query, "last_update"),
        fetch_and_process_kvs!(
            &api_pool,
            block_hash,
            pruning_scores_query,
            "pruning_scores"
        ),
        fetch_and_process_kvs!(
            &api_pool,
            block_hash,
            validator_trust_query,
            "validator_trust"
        ),
        fetch_and_process_kvs!(&api_pool, block_hash, weights_query, "weights"),
    );
    let end = std::time::Instant::now();
    println!("Get main stuff elapsed: {:?}", end - start);

    let mut neuron_map: HashMap<(u16, u16), NeuronInfo> = HashMap::new();
    let mut hotkeys = Vec::new();

    // Init neuron map with hotkeys
    for (key, value) in keys_kvs {
        let mut second_last_two_bytes = &key[key.len() - 4..key.len() - 2];
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut second_last_two_bytes).unwrap();
        let neuron_id = u16::decode(&mut last_two_bytes).unwrap();
        let hotkey = value.to_string();
        hotkeys.push(hotkey.clone());
        let block_hash = block_hash.clone().to_string();
        let neuron_info = NeuronInfo {
            subnet_id,
            neuron_id,
            hotkey,
            block_hash,
            ..Default::default()
        };
        neuron_map.insert((subnet_id, neuron_id), neuron_info);
    }

    // Set active
    for (key, value) in active_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, active) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .active = *active;
        }
    }

    // Set rank
    for (key, value) in rank_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, rank) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .rank = *rank;
        }
    }

    // Set trust
    for (key, value) in trust_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, trust) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .trust = *trust;
        }
    }

    // Set emission
    for (key, value) in emission_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, emission) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .emission = *emission;
        }
    }

    // Set consensus
    for (key, value) in consensus_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, consensus) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .consensus = *consensus;
        }
    }

    // Set incentive
    for (key, value) in incentive_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, incentive) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .incentive = *incentive;
        }
    }

    // Set dividends
    for (key, value) in dividends_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, dividends) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .dividends = *dividends;
        }
    }

    // Set last_update
    for (key, value) in last_update_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, last_update) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .last_update = *last_update;
        }
    }

    // Set pruning_scores
    for (key, value) in pruning_scores_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, pruning_scores) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .pruning_scores = *pruning_scores;
        }
    }

    // Set validator_trust
    for (key, value) in validator_trust_kvs {
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        for (neuron_id, validator_trust) in value.iter().enumerate() {
            neuron_map
                .get_mut(&(subnet_id, neuron_id as u16))
                .expect("Subnet neuron not initialized!")
                .validator_trust = *validator_trust;
        }
    }

    // Set Weights
    for (key, weights) in weights_kvs {
        let mut second_last_two_bytes = &key[key.len() - 4..key.len() - 2];
        let mut last_two_bytes = &key[key.len() - 2..];
        let subnet_id = u16::decode(&mut second_last_two_bytes).unwrap();
        let neuron_id = u16::decode(&mut last_two_bytes).unwrap();
        neuron_map
            .get_mut(&(subnet_id, neuron_id as u16))
            .expect("Subnet neuron not initialized!")
            .weights = weights.to_owned();
    }

    Ok((neuron_map.into_values().collect(), hotkeys))
}

#[pyfunction]
fn query_neuron_info(block_hash: String) -> PyResult<(Vec<NeuronInfo>, Vec<String>)> {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(query_neuron_info_inner(block_hash))
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(query_neuron_info, m)?)?;
    Ok(())
}