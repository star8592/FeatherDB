use std::collections::HashMap;

use crate::model::TabletId;

pub type TabletSlot = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReverseIndexError {
    TooManyTablets,
    DuplicateTabletId(TabletId),
    EmptySentinelCollision(TabletId),
}

pub trait TabletReverseIndex {
    fn get(&self, tablet_id: TabletId) -> Option<TabletSlot>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn allocated_bytes(&self) -> usize;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContiguousTabletIndex {
    first: TabletId,
    len: TabletSlot,
}

impl ContiguousTabletIndex {
    pub fn try_from_ids(ids: &[TabletId]) -> Result<Option<Self>, ReverseIndexError> {
        let len = TabletSlot::try_from(ids.len()).map_err(|_| ReverseIndexError::TooManyTablets)?;
        let Some(&first) = ids.first() else {
            return Ok(Some(Self { first: 0, len: 0 }));
        };

        for (slot, &tablet_id) in ids.iter().enumerate() {
            let expected = first
                .checked_add(slot as u64)
                .ok_or(ReverseIndexError::TooManyTablets)?;
            if tablet_id != expected {
                return Ok(None);
            }
        }

        Ok(Some(Self { first, len }))
    }
}

impl TabletReverseIndex for ContiguousTabletIndex {
    fn get(&self, tablet_id: TabletId) -> Option<TabletSlot> {
        let offset = tablet_id.checked_sub(self.first)?;
        let slot = TabletSlot::try_from(offset).ok()?;
        (slot < self.len).then_some(slot)
    }

    fn len(&self) -> usize {
        self.len as usize
    }

    fn allocated_bytes(&self) -> usize {
        0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortedTabletIndex {
    entries: Vec<(TabletId, TabletSlot)>,
}

impl SortedTabletIndex {
    pub fn build(ids: &[TabletId]) -> Result<Self, ReverseIndexError> {
        let mut entries = Vec::with_capacity(ids.len());
        for (slot, &tablet_id) in ids.iter().enumerate() {
            let slot = TabletSlot::try_from(slot).map_err(|_| ReverseIndexError::TooManyTablets)?;
            entries.push((tablet_id, slot));
        }
        entries.sort_unstable_by_key(|entry| entry.0);
        if let Some(pair) = entries.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(ReverseIndexError::DuplicateTabletId(pair[0].0));
        }
        Ok(Self { entries })
    }
}

impl TabletReverseIndex for SortedTabletIndex {
    fn get(&self, tablet_id: TabletId) -> Option<TabletSlot> {
        self.entries
            .binary_search_by_key(&tablet_id, |entry| entry.0)
            .ok()
            .map(|index| self.entries[index].1)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn allocated_bytes(&self) -> usize {
        self.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<(TabletId, TabletSlot)>())
    }
}

#[derive(Clone, Debug)]
pub struct StdHashTabletIndex {
    entries: HashMap<TabletId, TabletSlot>,
}

impl StdHashTabletIndex {
    pub fn build(ids: &[TabletId]) -> Result<Self, ReverseIndexError> {
        let mut entries = HashMap::with_capacity(ids.len());
        for (slot, &tablet_id) in ids.iter().enumerate() {
            let slot = TabletSlot::try_from(slot).map_err(|_| ReverseIndexError::TooManyTablets)?;
            if entries.insert(tablet_id, slot).is_some() {
                return Err(ReverseIndexError::DuplicateTabletId(tablet_id));
            }
        }
        Ok(Self { entries })
    }
}

impl TabletReverseIndex for StdHashTabletIndex {
    fn get(&self, tablet_id: TabletId) -> Option<TabletSlot> {
        self.entries.get(&tablet_id).copied()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn allocated_bytes(&self) -> usize {
        // std::collections::HashMap does not expose its exact backing allocation.
        // Report only logical key/value payload. RSS benchmark captures real process cost.
        self.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<(TabletId, TabletSlot)>())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAddressTabletIndex {
    keys: Vec<TabletId>,
    slots: Vec<TabletSlot>,
    len: usize,
}

impl OpenAddressTabletIndex {
    const EMPTY: TabletId = TabletId::MAX;
    const LOAD_NUMERATOR: usize = 7;
    const LOAD_DENOMINATOR: usize = 10;

    pub fn build(ids: &[TabletId]) -> Result<Self, ReverseIndexError> {
        if ids.contains(&Self::EMPTY) {
            return Err(ReverseIndexError::EmptySentinelCollision(Self::EMPTY));
        }
        if ids.len() > TabletSlot::MAX as usize {
            return Err(ReverseIndexError::TooManyTablets);
        }

        let min_capacity = ids
            .len()
            .saturating_mul(Self::LOAD_DENOMINATOR)
            .div_ceil(Self::LOAD_NUMERATOR)
            .max(1);
        let capacity = min_capacity
            .checked_next_power_of_two()
            .ok_or(ReverseIndexError::TooManyTablets)?;
        let mut index = Self {
            keys: vec![Self::EMPTY; capacity],
            slots: vec![0; capacity],
            len: 0,
        };

        for (slot, &tablet_id) in ids.iter().enumerate() {
            index.insert(
                tablet_id,
                TabletSlot::try_from(slot).map_err(|_| ReverseIndexError::TooManyTablets)?,
            )?;
        }
        Ok(index)
    }

    fn insert(&mut self, tablet_id: TabletId, slot: TabletSlot) -> Result<(), ReverseIndexError> {
        let mask = self.keys.len() - 1;
        let mut bucket = hash_tablet_id(tablet_id) as usize & mask;
        loop {
            match self.keys[bucket] {
                Self::EMPTY => {
                    self.keys[bucket] = tablet_id;
                    self.slots[bucket] = slot;
                    self.len += 1;
                    return Ok(());
                }
                existing if existing == tablet_id => {
                    return Err(ReverseIndexError::DuplicateTabletId(tablet_id));
                }
                _ => bucket = (bucket + 1) & mask,
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.keys.len()
    }
}

impl TabletReverseIndex for OpenAddressTabletIndex {
    fn get(&self, tablet_id: TabletId) -> Option<TabletSlot> {
        if tablet_id == Self::EMPTY || self.keys.is_empty() {
            return None;
        }
        let mask = self.keys.len() - 1;
        let mut bucket = hash_tablet_id(tablet_id) as usize & mask;
        loop {
            match self.keys[bucket] {
                Self::EMPTY => return None,
                existing if existing == tablet_id => return Some(self.slots[bucket]),
                _ => bucket = (bucket + 1) & mask,
            }
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn allocated_bytes(&self) -> usize {
        self.keys
            .capacity()
            .saturating_mul(std::mem::size_of::<TabletId>())
            .saturating_add(
                self.slots
                    .capacity()
                    .saturating_mul(std::mem::size_of::<TabletSlot>()),
            )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdaptiveTabletIndexKind {
    Contiguous,
    Sorted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdaptiveTabletIndex {
    Contiguous(ContiguousTabletIndex),
    Sorted(SortedTabletIndex),
}

impl AdaptiveTabletIndex {
    pub fn build(ids: &[TabletId]) -> Result<Self, ReverseIndexError> {
        if let Some(index) = ContiguousTabletIndex::try_from_ids(ids)? {
            return Ok(Self::Contiguous(index));
        }
        Ok(Self::Sorted(SortedTabletIndex::build(ids)?))
    }

    pub fn kind(&self) -> AdaptiveTabletIndexKind {
        match self {
            Self::Contiguous(_) => AdaptiveTabletIndexKind::Contiguous,
            Self::Sorted(_) => AdaptiveTabletIndexKind::Sorted,
        }
    }
}

impl TabletReverseIndex for AdaptiveTabletIndex {
    fn get(&self, tablet_id: TabletId) -> Option<TabletSlot> {
        match self {
            Self::Contiguous(index) => index.get(tablet_id),
            Self::Sorted(index) => index.get(tablet_id),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Contiguous(index) => index.len(),
            Self::Sorted(index) => index.len(),
        }
    }

    fn allocated_bytes(&self) -> usize {
        match self {
            Self::Contiguous(index) => index.allocated_bytes(),
            Self::Sorted(index) => index.allocated_bytes(),
        }
    }
}

fn hash_tablet_id(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_index_detects_fast_path() {
        let ids = [100, 101, 102, 103];
        let index = ContiguousTabletIndex::try_from_ids(&ids).unwrap().unwrap();
        assert_eq!(index.get(100), Some(0));
        assert_eq!(index.get(103), Some(3));
        assert_eq!(index.get(99), None);
        assert_eq!(index.get(104), None);
        assert_eq!(index.allocated_bytes(), 0);
    }

    #[test]
    fn contiguous_index_rejects_gap() {
        assert_eq!(
            ContiguousTabletIndex::try_from_ids(&[100, 101, 200]).unwrap(),
            None
        );
    }

    #[test]
    fn all_general_indexes_match_slots_for_sparse_ids() {
        let ids = [90, 5, 1_000_000, 7, 42, 777, 12];
        let sorted = SortedTabletIndex::build(&ids).unwrap();
        let hash = StdHashTabletIndex::build(&ids).unwrap();
        let open = OpenAddressTabletIndex::build(&ids).unwrap();

        for (slot, id) in ids.iter().copied().enumerate() {
            let expected = Some(slot as TabletSlot);
            assert_eq!(sorted.get(id), expected);
            assert_eq!(hash.get(id), expected);
            assert_eq!(open.get(id), expected);
        }
        for id in [0, 6, 99, 123_456_789] {
            assert_eq!(sorted.get(id), None);
            assert_eq!(hash.get(id), None);
            assert_eq!(open.get(id), None);
        }
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let ids = [1, 2, 2, 3];
        assert_eq!(
            SortedTabletIndex::build(&ids),
            Err(ReverseIndexError::DuplicateTabletId(2))
        );
        assert!(matches!(
            StdHashTabletIndex::build(&ids),
            Err(ReverseIndexError::DuplicateTabletId(2))
        ));
        assert_eq!(
            OpenAddressTabletIndex::build(&ids),
            Err(ReverseIndexError::DuplicateTabletId(2))
        );
    }

    #[test]
    fn adaptive_index_uses_zero_allocation_fast_path_when_possible() {
        let ids: Vec<_> = (1_000_000..1_010_000).collect();
        let index = AdaptiveTabletIndex::build(&ids).unwrap();
        assert_eq!(index.kind(), AdaptiveTabletIndexKind::Contiguous);
        assert_eq!(index.allocated_bytes(), 0);
        assert_eq!(index.get(1_005_432), Some(5_432));
    }

    #[test]
    fn adaptive_index_falls_back_to_sorted_for_sparse_ids() {
        let ids = [100, 500, 101, 9_000, 102];
        let index = AdaptiveTabletIndex::build(&ids).unwrap();
        assert_eq!(index.kind(), AdaptiveTabletIndexKind::Sorted);
        for (slot, tablet_id) in ids.iter().copied().enumerate() {
            assert_eq!(index.get(tablet_id), Some(slot as TabletSlot));
        }
    }

    #[test]
    fn open_address_memory_is_explicit_and_bounded() {
        let ids: Vec<_> = (10_000..11_000).collect();
        let index = OpenAddressTabletIndex::build(&ids).unwrap();
        assert!(index.capacity().is_power_of_two());
        assert!(index.capacity() >= ids.len());
        assert_eq!(
            index.allocated_bytes(),
            index.capacity()
                * (std::mem::size_of::<TabletId>() + std::mem::size_of::<TabletSlot>())
        );
    }
}
