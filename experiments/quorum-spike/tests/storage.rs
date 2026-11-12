use openraft::{
    StorageError,
    testing::{StoreBuilder, Suite},
};
use quorum_spike::{
    TypeConfig,
    storage::{LogStore, StateMachine},
};

struct Builder;
impl StoreBuilder<TypeConfig, LogStore, StateMachine> for Builder {
    async fn build(&self) -> Result<((), LogStore, StateMachine), StorageError<u64>> {
        Ok(((), LogStore::default(), StateMachine::default()))
    }
}

#[test]
fn upstream_storage_contract_suite() {
    Suite::test_all(Builder).unwrap();
}
