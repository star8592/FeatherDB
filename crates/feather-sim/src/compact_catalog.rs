use crate::model::TabletId;
use crate::range_resize::{HASH_SPACE_END, RangeResizeError, RangeTabletMap};
use crate::reverse_index::{AdaptiveTabletIndex, ReverseIndexError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactCatalogError {
    Range(RangeResizeError),
    ReverseIndex(ReverseIndexError),
    StartOverflow,
    CountMismatch,
    ZeroTablets,
    SizeOverflow,
    IdOverflow,
}

impl From<RangeResizeError> for CompactCatalogError {
    fn from(value: RangeResizeError) -> Self {
        Self::Range(value)
    }
}

impl From<ReverseIndexError> for CompactCatalogError {
    fn from(value: ReverseIndexError) -> Self {
        Self::ReverseIndex(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactTabletCatalog {
    generation: u64,
    next_tablet_id: TabletId,
    ids: Vec<TabletId>,
    starts: Vec<u64>,
    bytes: Vec<u64>,
}

impl CompactTabletCatalog {
    pub fn from_range_map(map: &RangeTabletMap) -> Result<Self, CompactCatalogError> {
        map.validate()?;

        let mut ids = Vec::with_capacity(map.tablet_count());
        let mut starts = Vec::with_capacity(map.tablet_count());
        let mut bytes = Vec::with_capacity(map.tablet_count());

        for tablet in map.tablets() {
            let start =
                u64::try_from(tablet.start).map_err(|_| CompactCatalogError::StartOverflow)?;
            ids.push(tablet.id);
            starts.push(start);
            bytes.push(tablet.bytes);
        }

        Ok(Self {
            generation: map.generation(),
            next_tablet_id: map.next_tablet_id(),
            ids,
            starts,
            bytes,
        })
    }

    pub fn uniform(
        first_tablet_id: TabletId,
        tablet_count: usize,
        total_bytes: u64,
    ) -> Result<Self, CompactCatalogError> {
        if tablet_count == 0 {
            return Err(CompactCatalogError::ZeroTablets);
        }
        let count_u64 =
            u64::try_from(tablet_count).map_err(|_| CompactCatalogError::SizeOverflow)?;
        let next_tablet_id = first_tablet_id
            .checked_add(count_u64)
            .ok_or(CompactCatalogError::IdOverflow)?;

        let mut ids = Vec::with_capacity(tablet_count);
        let mut starts = Vec::with_capacity(tablet_count);
        let mut bytes = Vec::with_capacity(tablet_count);
        let base_bytes = total_bytes / count_u64;
        let extra = total_bytes % count_u64;

        for slot in 0..tablet_count {
            let slot_u64 = slot as u64;
            ids.push(
                first_tablet_id
                    .checked_add(slot_u64)
                    .ok_or(CompactCatalogError::IdOverflow)?,
            );
            let start = (slot as u128).saturating_mul(HASH_SPACE_END) / tablet_count as u128;
            starts.push(u64::try_from(start).map_err(|_| CompactCatalogError::StartOverflow)?);
            bytes.push(base_bytes + u64::from(slot_u64 < extra));
        }

        Ok(Self {
            generation: 0,
            next_tablet_id,
            ids,
            starts,
            bytes,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn next_tablet_id(&self) -> TabletId {
        self.next_tablet_id
    }

    pub fn tablet_count(&self) -> usize {
        self.ids.len()
    }

    pub fn tablet_ids(&self) -> &[TabletId] {
        &self.ids
    }

    pub fn build_reverse_index(&self) -> Result<AdaptiveTabletIndex, CompactCatalogError> {
        Ok(AdaptiveTabletIndex::build(&self.ids)?)
    }

    pub fn tablet_id(&self, slot: usize) -> Option<TabletId> {
        self.ids.get(slot).copied()
    }

    pub fn bytes(&self, slot: usize) -> Option<u64> {
        self.bytes.get(slot).copied()
    }

    pub fn start(&self, slot: usize) -> Option<u64> {
        self.starts.get(slot).copied()
    }

    pub fn end(&self, slot: usize) -> Option<u128> {
        if slot >= self.tablet_count() {
            return None;
        }

        if let Some(next) = self.starts.get(slot + 1) {
            Some(u128::from(*next))
        } else {
            Some(HASH_SPACE_END)
        }
    }

    pub fn route_slot(&self, token: u64) -> Option<usize> {
        if self.starts.is_empty() {
            return None;
        }

        let index = self.starts.partition_point(|start| *start <= token);
        index.checked_sub(1)
    }

    pub fn route_tablet_id(&self, token: u64) -> Option<TabletId> {
        self.route_slot(token).and_then(|slot| self.tablet_id(slot))
    }

    pub fn total_bytes(&self) -> u64 {
        self.bytes
            .iter()
            .fold(0_u64, |sum, bytes| sum.saturating_add(*bytes))
    }

    pub fn logical_bytes(&self) -> usize {
        self.ids
            .len()
            .saturating_mul(std::mem::size_of::<TabletId>())
            .saturating_add(self.starts.len().saturating_mul(std::mem::size_of::<u64>()))
            .saturating_add(self.bytes.len().saturating_mul(std::mem::size_of::<u64>()))
    }

    pub fn allocated_bytes(&self) -> usize {
        self.ids
            .capacity()
            .saturating_mul(std::mem::size_of::<TabletId>())
            .saturating_add(
                self.starts
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(
                self.bytes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
    }

    pub fn equivalent_to_range_map(&self, map: &RangeTabletMap) -> bool {
        if self.generation != map.generation()
            || self.next_tablet_id != map.next_tablet_id()
            || self.tablet_count() != map.tablet_count()
            || self.total_bytes() != map.total_bytes()
        {
            return false;
        }

        map.tablets().iter().enumerate().all(|(slot, tablet)| {
            self.tablet_id(slot) == Some(tablet.id)
                && self.start(slot).map(u128::from) == Some(tablet.start)
                && self.end(slot) == Some(tablet.end)
                && self.bytes(slot) == Some(tablet.bytes)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reverse_index::{AdaptiveTabletIndexKind, TabletReverseIndex};

    fn split_map(rounds: usize) -> RangeTabletMap {
        let mut map = RangeTabletMap::single(10, 10_000, vec![1, 2, 3]).unwrap();
        for _ in 0..rounds {
            let plan = map.plan_split_all(7).unwrap();
            map.commit(&plan, 7).unwrap();
        }
        map
    }

    #[test]
    fn catalog_matches_range_map_after_repeated_splits() {
        let map = split_map(6);
        let catalog = CompactTabletCatalog::from_range_map(&map).unwrap();

        assert_eq!(catalog.tablet_count(), 64);
        assert_eq!(catalog.logical_bytes(), 64 * 24);
        assert_eq!(catalog.total_bytes(), 10_000);
        assert!(catalog.equivalent_to_range_map(&map));
    }

    #[test]
    fn catalog_routes_exactly_like_range_map() {
        let map = split_map(8);
        let catalog = CompactTabletCatalog::from_range_map(&map).unwrap();

        for token in (0_u64..=u16::MAX as u64).step_by(251) {
            assert_eq!(
                catalog.route_tablet_id(token),
                map.route(token).map(|tablet| tablet.id)
            );
        }

        for token in [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
            assert_eq!(
                catalog.route_tablet_id(token),
                map.route(token).map(|tablet| tablet.id)
            );
        }
    }

    #[test]
    fn last_range_implicitly_ends_at_full_hash_space() {
        let map = split_map(3);
        let catalog = CompactTabletCatalog::from_range_map(&map).unwrap();
        let last = catalog.tablet_count() - 1;

        assert_eq!(catalog.end(last), Some(HASH_SPACE_END));
        assert_eq!(catalog.route_slot(u64::MAX), Some(last));
    }

    #[test]
    fn uniform_catalog_preserves_full_hash_space_and_bytes() {
        let catalog = CompactTabletCatalog::uniform(50_000, 1_000, 123_457).unwrap();

        assert_eq!(catalog.tablet_count(), 1_000);
        assert_eq!(catalog.tablet_id(0), Some(50_000));
        assert_eq!(catalog.tablet_id(999), Some(50_999));
        assert_eq!(catalog.next_tablet_id(), 51_000);
        assert_eq!(catalog.start(0), Some(0));
        assert_eq!(catalog.end(999), Some(HASH_SPACE_END));
        assert_eq!(catalog.total_bytes(), 123_457);
        assert_eq!(catalog.logical_bytes(), 24_000);
        assert!(catalog.route_slot(u64::MAX).is_some());
    }

    #[test]
    fn current_full_split_merge_generations_use_contiguous_reverse_fast_path() {
        let mut map = split_map(8);

        for _ in 0..4 {
            let catalog = CompactTabletCatalog::from_range_map(&map).unwrap();
            let index = catalog.build_reverse_index().unwrap();
            assert_eq!(index.kind(), AdaptiveTabletIndexKind::Contiguous);
            assert_eq!(index.allocated_bytes(), 0);

            for (slot, tablet_id) in catalog.tablet_ids().iter().copied().enumerate() {
                assert_eq!(index.get(tablet_id), Some(slot as u32));
            }

            let merge = map.plan_merge_pairs(7).unwrap();
            map.commit(&merge, 7).unwrap();
        }
    }

    #[test]
    fn catalog_rebuild_tracks_new_generation_after_merge() {
        let mut map = split_map(4);
        let before = CompactTabletCatalog::from_range_map(&map).unwrap();

        let merge = map.plan_merge_pairs(7).unwrap();
        map.commit(&merge, 7).unwrap();
        let after = CompactTabletCatalog::from_range_map(&map).unwrap();

        assert_eq!(after.generation(), before.generation() + 1);
        assert_eq!(after.tablet_count(), before.tablet_count() / 2);
        assert_eq!(after.total_bytes(), before.total_bytes());
        assert!(after.equivalent_to_range_map(&map));
    }
}
