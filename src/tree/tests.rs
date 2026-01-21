use std::{collections::BTreeMap, fmt::Debug};

use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};

use crate::{
    block::{
        BLOCK_SIZE,
        allocator::{Allocator, set::SetAllocator},
        storage::map::MapStorage,
    },
    fs::node::NodeId,
    key, keys,
};

use super::*;

fn arb_node_id(max_id: u64) -> impl Strategy<Value = NodeId> {
    (0..max_id).prop_map(NodeId::new)
}

fn arb_data_type() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::Node),
        Just(DataType::Extent),
        Just(DataType::DirEntry),
    ]
}

prop_compose! {
    fn arb_key(max_id: u64, max_offset: u64)(
        id in arb_node_id(max_id),
        data_type in arb_data_type(),
        offset in 0..max_offset,
        ) -> Key {
        Key::new(id, data_type, offset)
    }
}

fn arb_data() -> impl Strategy<Value = Box<[u8]>> {
    prop::collection::vec(any::<u8>(), 0..=DATA_MAX_LEN).prop_map(|data| data.into_boxed_slice())
}

struct TreeStateReference;

#[derive(Clone)]
enum Transition {
    Insert(Key, Box<[u8]>),
    Remove(Key),
}

impl Debug for Transition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert(key, data) => f
                .debug_tuple("Insert")
                .field(key)
                .field(&format!("[u8; {}]", data.len()))
                .finish(),
            Self::Remove(key) => f.debug_tuple("Remove").field(key).finish(),
        }
    }
}

impl ReferenceStateMachine for TreeStateReference {
    type State = BTreeMap<Key, Box<[u8]>>;

    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(Self::State::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let mut strats = Vec::new();

        let insert_strat = (arb_key(100, 100), arb_data())
            .prop_map(|(key, data)| Transition::Insert(key, data))
            .boxed();
        strats.push(insert_strat);

        if !state.is_empty() {
            let keys: Vec<_> = state.keys().copied().collect();
            let remove_strat = proptest::sample::select(keys)
                .prop_map(Transition::Remove)
                .boxed();
            strats.push(remove_strat)
        }

        proptest::strategy::Union::new(strats).boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::Insert(key, data) => {
                state.insert(*key, data.clone());
            }
            Transition::Remove(key) => {
                state.remove(key);
            }
        }
        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::Insert(key, _) => !state.contains_key(key),
            Transition::Remove(key) => state.contains_key(key),
        }
    }
}

struct TreeState {
    storage: MapStorage,
    allocator: SetAllocator,
    root_addr: BlockAddr,
}

impl TreeState {
    fn get(&self, key: Key) -> Result<Option<Box<[u8]>>> {
        Tree::get(&self.storage, self.root_addr, key)
    }

    fn insert(&mut self, key: Key, data: &[u8]) -> Result<()> {
        Tree::try_insert(
            &mut self.storage,
            &mut self.allocator,
            &mut self.root_addr,
            key,
            &data,
        )
    }

    fn remove(&mut self, key: Key) -> Result<Option<Box<[u8]>>> {
        Tree::remove(
            &mut self.storage,
            &mut self.allocator,
            &mut self.root_addr,
            key,
        )
    }
}

impl Default for TreeState {
    fn default() -> Self {
        let mut storage = MapStorage::default();
        let mut allocator = SetAllocator::default();
        let root_addr = allocator
            .allocate(1)
            .expect("must be able to allocate root");
        let mut block = Block::default();
        Leaf::format(&mut block, 0);
        storage
            .write_at(&block, root_addr)
            .expect("must be able to write root");
        Self {
            storage,
            allocator,
            root_addr,
        }
    }
}

impl Debug for TreeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_tree(
            tree: &TreeState,
            f: &mut std::fmt::Formatter<'_>,
            addr: BlockAddr,
        ) -> std::fmt::Result {
            let mut block = Block::default();
            tree.storage.read_at(&mut block, addr).unwrap();
            let node = NodeVariant::try_new(&block).unwrap();
            match node {
                NodeVariant::Branch(node) => {
                    writeln!(f, "{:?}", node)?;
                    for child_idx in 0..node.item_count() {
                        let child_addr = node.child_at(child_idx.into()).unwrap();
                        fmt_tree(tree, f, child_addr)?;
                    }
                    Ok(())
                }
                NodeVariant::Leaf(node) => writeln!(f, "{:?}", node),
            }
        }

        fmt_tree(self, f, self.root_addr)
    }
}

impl StateMachineTest for TreeState {
    type SystemUnderTest = TreeState;

    type Reference = TreeStateReference;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        Self::SystemUnderTest::default()
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::Insert(key, data) => {
                state.insert(key, &data).expect("insertion failed");

                let ref_data = ref_state.get(&key).cloned();
                let data = state.get(key).expect("retrieval failed");
                assert_eq!(data, ref_data);
            }
            Transition::Remove(key) => {
                state.remove(key).expect("removal failed");

                let ref_data = ref_state.get(&key).cloned();
                let data = state.get(key).expect("retrieval failed");
                assert_eq!(data, ref_data);
            }
        }
        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        for (key, ref_data) in ref_state {
            let data = state.get(*key).unwrap();
            assert_eq!(data.as_ref(), Some(ref_data));
        }
    }
}

prop_state_machine! {
    #[test]
    fn prop_state_machine(sequential 1..100 => TreeState);
}

#[test]
fn get_nonexistent() {
    let state = TreeState::default();
    let got_data = state.get(key!()).unwrap();
    assert_eq!(got_data.as_deref(), None);
}

#[test]
fn insert() {
    let mut state = TreeState::default();
    let key = key!();
    let data = b"foo";

    state.insert(key, data).unwrap();
    let got_data = state.get(key).unwrap();
    assert_eq!(got_data.as_deref(), Some(data.as_ref()));
}

#[test]
fn insert_existent() {
    let mut state = TreeState::default();
    let key = key!();
    let data = b"foo";
    state.insert(key, data).unwrap();

    let result = state.insert(key, data);
    assert!(matches!(result, Err(Error::Occupied)));
}

#[test]
fn remove() {
    let mut state = TreeState::default();
    let key = key!();
    let data = b"foo";
    state.insert(key, data).unwrap();

    let got_data = state.remove(key).unwrap();
    assert_eq!(got_data.as_deref(), Some(data.as_ref()));

    let got_data = state.get(key).unwrap();
    assert_eq!(got_data, None);
}

#[test]
fn remove_nonexistent() {
    let mut state = TreeState::default();
    let got_data = state.remove(key!()).unwrap();
    assert_eq!(got_data.as_deref(), None);
}

const MANY_COUNT: usize = BLOCK_SIZE as usize * 4 / DATA_MAX_LEN;

#[test]
fn insert_many_sorted() {
    let mut state = TreeState::default();
    let keys = keys![0..MANY_COUNT as u64];
    let data = [0xAB; DATA_MAX_LEN];

    for &key in &keys {
        state.insert(key, &data).unwrap();
    }

    for &key in &keys {
        let got_data = state.get(key).unwrap();
        assert_eq!(got_data.as_deref(), Some(data.as_ref()), "{:?}", &state);
    }
}

#[test]
fn insert_many_reverse() {
    let mut state = TreeState::default();
    let mut keys = keys![0..MANY_COUNT as u64];
    keys.reverse();
    let data = [0xAB; DATA_MAX_LEN];

    for &key in &keys {
        state.insert(key, &data).unwrap();
    }

    for &key in &keys {
        let got_data = state.get(key).unwrap();
        assert_eq!(got_data.as_deref(), Some(data.as_ref()), "{:?}", &state);
    }
}

#[test]
fn remove_many_sorted() {
    let mut state = TreeState::default();
    let keys = keys![0..MANY_COUNT as u64];
    let data = [0xAB; DATA_MAX_LEN];

    for &key in &keys {
        state.insert(key, &data).unwrap();
    }

    for &key in &keys {
        state.remove(key).unwrap();
    }

    for &key in &keys {
        let got_data = state.get(key).unwrap();
        assert_eq!(got_data, None);
    }
}

#[test]
fn remove_many_reverse() {
    let mut state = TreeState::default();
    let mut keys = keys![0..MANY_COUNT as u64];
    let data = [0xAB; DATA_MAX_LEN];

    for &key in &keys {
        state.insert(key, &data).unwrap();
    }

    keys.reverse();
    for &key in &keys {
        state.remove(key).unwrap();
    }

    for &key in &keys {
        let got_data = state.get(key).unwrap();
        assert_eq!(got_data, None);
    }
}
