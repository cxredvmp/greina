use super::*;

#[macro_export]
macro_rules! key {
    () => {
        Key::new(crate::fs::node::NodeId::NULL, DataType::Node, 0)
    };
    ($obj_id:expr) => {
        Key::new(crate::fs::node::NodeId::new($obj_id), DataType::Node, 0)
    };
}

#[macro_export]
macro_rules! keys {
    ($range:expr) => {
        $range.map(|i| key!(i)).collect::<Vec<_>>()
    };
    ($($obj_id:expr),+ $(,)?) => {
        [
            $(
                key!($obj_id)
            ),+
        ]
    };
}

macro_rules! node {
    ($node_type:ident, $height:expr) => {{
        let block = Block::default();
        $node_type::format(block, $height)
    }};
}

mod branch {
    use super::*;

    macro_rules! branch {
            () => {
                node!(Branch, 1)
            };
            ($($key:expr => $child:expr),* $(,)?) => {
                {
                    let mut branch = node!(Branch, 1);
                    $(
                        branch.insert(key!($key), $child).unwrap();
                    )*
                    branch
                }
            };
        }

    macro_rules! assert_routes {
            ($branch:expr, $($key:expr => $child:expr),+ $(,)?) => {
                $(
                    let key = key!($key);
                    let got = $branch.child_for(key);
                    assert_eq!(
                        $child,
                        got,
                        "route mismatch for {:?}: expected {:?}, got {:?}",
                        key,
                        $child,
                        got
                    );
                )*
            };
        }

    #[test]
    fn insert() {
        let mut branch = branch!();

        branch.insert(key!(20), 20).unwrap();
        assert_routes!(
            branch,
            15 => 20,
            20 => 20,
            25 => 20,
        );

        branch.insert(key!(10), 10).unwrap();
        assert_routes!(
            branch,
            10 => 10,
            15 => 10,
            20 => 20,
            25 => 20,
        );
    }

    #[test]
    fn trigger_overflow() {
        let mut branch = branch!();
        const COUNT: usize = NODE_CAPACITY / size_of::<BranchItem>();
        for i in 0..COUNT {
            branch.insert(key!(i as u64), 0).unwrap()
        }

        assert!(matches!(
            branch.insert(key!(), 0),
            Err(InsertError::Overflow)
        ));
    }

    #[test]
    fn remove() {
        let mut branch = branch!(
            10 => 10,
            20 => 20,
            30 => 30,
        );

        branch.remove_at(1);

        assert_routes!(
            branch,
            10 => 10,
            20 => 10,
            30 => 30,
        );
    }

    #[test]
    fn rotate_left() {
        let mut right = branch!();
        const COUNT: usize = NODE_CAPACITY / size_of::<BranchItem>();
        for i in 0..COUNT {
            right.insert(key!(1 + i as u64), 0).unwrap();
        }
        let mut branch = branch!(0 => 0);

        branch.rotate_left(&mut right).unwrap();

        assert!(!right.is_deficient());
        assert!(!branch.is_deficient());
    }

    #[test]
    fn rotate_right() {
        const COUNT: usize = NODE_CAPACITY / size_of::<BranchItem>();
        let mut left = branch!();
        for i in 0..COUNT {
            left.insert(key!(i as u64), 0).unwrap()
        }
        let mut branch = branch!(COUNT as u64 => 0);

        branch.rotate_right(&mut left).unwrap();

        assert!(!left.is_deficient());
        assert!(!branch.is_deficient());
    }

    #[test]
    fn split() {
        let mut branch = branch!(
            10 => 10,
            20 => 20,
            30 => 30,
        );
        let mut right = branch!();

        branch.split(&mut right);

        assert_routes!(
            branch,
            10 => 10,
            20 => 20,
        );

        assert_routes!(
            right,
            20 => 30,
            30 => 30,
        );
    }

    #[test]
    fn merge() {
        let mut branch = branch!(
            10 => 10,
            20 => 20,
        );
        let right = branch!(
            30 => 30,
            40 => 40,
        );

        branch.merge(&right).unwrap();

        assert_routes!(
            branch,
            10 => 10,
            20 => 20,
            30 => 30,
            40 => 40,
        );
    }
}

mod leaf {
    use crate::tree::DATA_MAX_LEN;

    use super::*;

    macro_rules! leaf {
            () => {
                node!(Leaf, 0)
            };
            ($($key:expr => $data:expr),* $(,)?) => {
                {
                    let mut leaf = node!(Leaf, 0);
                    $(
                        leaf.insert(key!($key), $data).unwrap();
                    )*
                    leaf
                }
            };
        }

    macro_rules! assert_has {
            ($leaf:expr, $($key:expr => $expected:expr),+ $(,)?) => {
                $(
                    let key = key!($key);
                    let got = $leaf.get(key);
                    let expected: &[u8] = $expected;
                    assert_eq!(
                        Some(expected),
                        got,
                        "mismatch when getting {:?}: expected {:?}, got {:?}",
                        key,
                        Some(expected),
                        got
                    );
                )*
            };
        }

    macro_rules! assert_not_has {
            ($leaf:expr, $($key:expr),+ $(,)?) => {
                $(
                    let key = $key;
                    let got = $leaf.get(key);
                    assert!(
                        matches!(got, None),
                        "mismatch when getting {:?}: expected None, got {:?}",
                        key,
                        got
                    );
                )*
            };
        }

    #[test]
    fn get_nonexistent() {
        let leaf = leaf!();
        assert!(matches!(leaf.get(key!(0)), None));
    }

    #[test]
    fn insert_many_sorted() {
        let mut leaf = leaf!();

        leaf.insert(key!(0), b"foo").unwrap();
        leaf.insert(key!(1), b"bar").unwrap();
        leaf.insert(key!(2), b"baz").unwrap();

        assert_has!(
            leaf,
            0 => b"foo",
            1 => b"bar",
            2 => b"baz",
        );
    }

    #[test]
    fn insert_many_reverse() {
        let mut leaf = leaf!();

        leaf.insert(key!(2), b"baz").unwrap();
        leaf.insert(key!(0), b"foo").unwrap();
        leaf.insert(key!(1), b"bar").unwrap();

        assert_has!(
            leaf,
            0 => b"foo",
            1 => b"bar",
            2 => b"baz",
        );
    }

    #[test]
    fn insert_existent() {
        let mut leaf = leaf!(0 => b"foobar");
        assert!(matches!(
            leaf.insert(key!(0), b"foobar"),
            Err(InsertError::Occupied)
        ))
    }

    #[test]
    fn insert_empty_data() {
        let mut leaf = leaf!();

        leaf.insert(key!(0), b"").unwrap();
        assert_has!(leaf, 0 => b"");

        let expect_free = NODE_CAPACITY - size_of::<LeafItem>();
        assert_eq!(leaf.free_space(), expect_free);
    }

    #[test]
    fn trigger_overflow() {
        const COUNT: usize = NODE_CAPACITY / (DATA_MAX_LEN + size_of::<LeafItem>());
        let mut leaf = leaf!();
        let data = [0xABu8; DATA_MAX_LEN];
        for i in 0..COUNT as u64 {
            leaf.insert(key!(i), &data).unwrap();
        }

        assert!(matches!(
            leaf.insert(key!(1), &data),
            Err(InsertError::Overflow)
        ))
    }

    #[test]
    fn insert_space_change() {
        let mut leaf = leaf!();
        let space_before = leaf.free_space();

        leaf.insert(key!(), b"foobar").unwrap();
        let space_after = leaf.free_space();

        let expect_change = size_of::<LeafItem>() + b"foobar".len();
        let got_change = space_before - space_after;
        assert_eq!(expect_change, got_change)
    }

    #[test]
    fn remove() {
        let mut leaf = leaf!(
            0 => b"foo",
            1 => b"bar",
            2 => b"baz"
        );

        leaf.remove(key!(1)).unwrap();
        assert_not_has!(leaf, key!(1));
        assert_has!(
            leaf,
            0 => b"foo",
            2 => b"baz",
        );
    }

    #[test]
    fn remove_empty() {
        let mut leaf = leaf!(0 => b"");
        let free_before = leaf.free_space();

        leaf.remove(key!(0)).unwrap();
        let free_after = leaf.free_space();

        let expect_change = size_of::<LeafItem>();
        assert_eq!(free_after - free_before, expect_change);
    }

    #[test]
    fn remove_nonexistent() {
        let mut leaf = leaf!();
        assert!(matches!(leaf.remove(key!(0)), None))
    }

    #[test]
    fn remove_space_change() {
        let mut leaf = leaf!(0 => b"foobar");
        let space_before = leaf.free_space();

        leaf.remove(key!(0)).unwrap();
        let space_after = leaf.free_space();

        let expect_change = size_of::<LeafItem>() + b"foobar".len();
        let got_change = space_after - space_before;
        assert_eq!(expect_change, got_change)
    }

    #[test]
    fn rotate_left() {
        let mut leaf = leaf!();

        const COUNT: usize = NODE_CAPACITY / (DATA_MAX_LEN + size_of::<LeafItem>());
        let mut right = leaf!();
        let data = [0xABu8; DATA_MAX_LEN];
        for i in 0..COUNT as u64 {
            right.insert(key!(i), &data).unwrap();
        }

        leaf.rotate_left(&mut right).unwrap();

        assert!(!right.is_deficient());
        assert!(!leaf.is_deficient());
    }

    #[test]
    fn rotate_right() {
        let mut leaf = leaf!();
        let mut left = leaf!();

        let data = b"foo";
        const COUNT: usize = OCCUPANCY_THRESH / ("foo".len() + size_of::<LeafItem>()) - 1;
        for i in 0..COUNT as u64 {
            left.insert(key!(i), data).unwrap();
            leaf.insert(key!(i), data).unwrap();
        }

        leaf.rotate_right(&mut left).unwrap();

        assert!(!left.is_deficient());
        assert!(!leaf.is_deficient());
    }

    #[test]
    fn split() {
        let mut leaf = leaf!(
            0 => b"foo",
            1 => b"bar",
            2 => b"baz",
        );
        let mut right = leaf!();

        leaf.split(&mut right);

        assert_not_has!(leaf, key!(2));
        assert_has!(
            leaf,
            0 => b"foo",
            1 => b"bar",
        );

        assert_not_has!(right, key!(0), key!(1));
        assert_has!(
            right,
            2 => b"baz",
        );
    }

    #[test]
    fn merge() {
        let mut leaf = leaf!(
            0 => b"foo",
            1 => b"foo",
        );
        let right = leaf!(
            2 => b"bar",
            3 => b"bar",
        );

        leaf.merge(&right).unwrap();
        assert_has!(
            leaf,
            0 => b"foo",
            1 => b"foo",
            2 => b"bar",
            3 => b"bar",
        );
    }

    #[test]
    fn merge_full() {
        const SIZE: usize = NODE_CAPACITY - size_of::<LeafItem>();
        let data = [0xABu8; SIZE];
        let mut leaf = leaf!(0 => &data);
        let right = leaf!(1 => &data);
        assert!(matches!(leaf.merge(&right), Err(MergeError::Overflows)));
    }
}
