// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(
    clippy::collapsible_if,
    clippy::explicit_iter_loop,
    reason = "generated by crepe"
)]

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;

use either::Either;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::{ActorMapping, VirtualNode, WorkerSlotId, WorkerSlotMapping};
use risingwave_common::{bail, hash};
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::meta::table_fragments::fragment::{
    FragmentDistributionType, PbFragmentDistributionType,
};
use risingwave_pb::stream_plan::DispatcherType::{self, *};

use crate::manager::{WorkerId, WorkerLocations};
use crate::model::ActorId;
use crate::stream::schedule_units_for_slots;
use crate::stream::stream_graph::fragment::CompleteStreamFragmentGraph;
use crate::stream::stream_graph::id::GlobalFragmentId as Id;
use crate::MetaResult;

type HashMappingId = usize;

/// The internal distribution structure for processing in the scheduler.
///
/// See [`Distribution`] for the public interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DistId {
    Singleton(WorkerSlotId),
    Hash(HashMappingId),
}

/// Facts as the input of the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Fact {
    /// An internal(building) fragment.
    Fragment(Id),
    /// An edge in the fragment graph.
    Edge {
        from: Id,
        to: Id,
        dt: DispatcherType,
    },
    /// A distribution requirement for an external(existing) fragment.
    ExternalReq { id: Id, dist: DistId },
    /// A singleton requirement for a building fragment.
    /// Note that the physical worker slot is not determined yet.
    SingletonReq(Id),
}

/// Results of all building fragments, as the output of the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Result {
    /// This fragment is required to be distributed by the given [`DistId`].
    Required(DistId),
    /// This fragment is singleton, and should be scheduled to the default worker slot.
    DefaultSingleton,
    /// This fragment is hash-distributed, and should be scheduled by the default hash mapping.
    DefaultHash,
}

crepe::crepe! {
    @input
    struct Input(Fact);

    struct Fragment(Id);
    struct Edge(Id, Id, DispatcherType);
    struct ExternalReq(Id, DistId);
    struct SingletonReq(Id);
    struct Requirement(Id, DistId);

    @output
    struct Success(Id, Result);
    @output
    #[derive(Debug)]
    struct Failed(Id);

    // Extract facts.
    Fragment(id) <- Input(f), let Fact::Fragment(id) = f;
    Edge(from, to, dt) <- Input(f), let Fact::Edge { from, to, dt } = f;
    ExternalReq(id, dist) <- Input(f), let Fact::ExternalReq { id, dist } = f;
    SingletonReq(id) <- Input(f), let Fact::SingletonReq(id) = f;

    // Requirements from the facts.
    Requirement(x, d) <- ExternalReq(x, d);
    // Requirements propagate through `NoShuffle` edges.
    Requirement(x, d) <- Edge(x, y, NoShuffle), Requirement(y, d);
    Requirement(y, d) <- Edge(x, y, NoShuffle), Requirement(x, d);

    // The downstream fragment of a `Simple` edge must be singleton.
    SingletonReq(y) <- Edge(_, y, Simple);
    // Singleton requirements propagate through `NoShuffle` edges.
    SingletonReq(x) <- Edge(x, y, NoShuffle), SingletonReq(y);
    SingletonReq(y) <- Edge(x, y, NoShuffle), SingletonReq(x);

    // Multiple requirements conflict.
    Failed(x) <- Requirement(x, d1), Requirement(x, d2), (d1 != d2);
    // Singleton requirement conflicts with hash requirement.
    Failed(x) <- SingletonReq(x), Requirement(x, d), let DistId::Hash(_) = d;

    // Take the required distribution as the result.
    Success(x, Result::Required(d)) <- Fragment(x), Requirement(x, d), !Failed(x);
    // Take the default singleton distribution as the result, if no other requirement.
    Success(x, Result::DefaultSingleton) <- Fragment(x), SingletonReq(x), !Requirement(x, _);
    // Take the default hash distribution as the result, if no other requirement.
    Success(x, Result::DefaultHash) <- Fragment(x), !SingletonReq(x), !Requirement(x, _);
}

/// The distribution of a fragment.
#[derive(Debug, Clone, EnumAsInner)]
pub(super) enum Distribution {
    /// The fragment is singleton and is scheduled to the given worker slot.
    Singleton(WorkerSlotId),

    /// The fragment is hash-distributed and is scheduled by the given hash mapping.
    Hash(WorkerSlotMapping),
}

impl Distribution {
    /// The parallelism required by the distribution.
    pub fn parallelism(&self) -> usize {
        self.worker_slots().count()
    }

    /// All worker slots required by the distribution.
    pub fn worker_slots(&self) -> impl Iterator<Item = WorkerSlotId> + '_ {
        match self {
            Distribution::Singleton(p) => Either::Left(std::iter::once(*p)),
            Distribution::Hash(mapping) => Either::Right(mapping.iter_unique()),
        }
    }

    /// Create a distribution from a persisted protobuf `Fragment`.
    pub fn from_fragment(
        fragment: &risingwave_pb::meta::table_fragments::Fragment,
        actor_location: &HashMap<ActorId, WorkerId>,
    ) -> Self {
        match fragment.get_distribution_type().unwrap() {
            FragmentDistributionType::Unspecified => unreachable!(),
            FragmentDistributionType::Single => {
                let actor_id = fragment.actors.iter().exactly_one().unwrap().actor_id;
                let location = actor_location.get(&actor_id).unwrap();
                let worker_slot_id = WorkerSlotId::new(*location, 0);
                Distribution::Singleton(worker_slot_id)
            }
            FragmentDistributionType::Hash => {
                let actor_bitmaps: HashMap<_, _> = fragment
                    .actors
                    .iter()
                    .map(|actor| {
                        (
                            actor.actor_id as hash::ActorId,
                            Bitmap::from(actor.vnode_bitmap.as_ref().unwrap()),
                        )
                    })
                    .collect();

                let actor_mapping = ActorMapping::from_bitmaps(&actor_bitmaps);
                let mapping = actor_mapping.to_worker_slot(actor_location);

                Distribution::Hash(mapping)
            }
        }
    }

    /// Convert the distribution to [`PbFragmentDistributionType`].
    pub fn to_distribution_type(&self) -> PbFragmentDistributionType {
        match self {
            Distribution::Singleton(_) => PbFragmentDistributionType::Single,
            Distribution::Hash(_) => PbFragmentDistributionType::Hash,
        }
    }
}

/// [`Scheduler`] schedules the distribution of fragments in a stream graph.
pub(super) struct Scheduler {
    /// The default hash mapping for hash-distributed fragments, if there's no requirement derived.
    default_hash_mapping: WorkerSlotMapping,

    /// The default worker slot for singleton fragments, if there's no requirement derived.
    default_singleton_worker_slot: WorkerSlotId,
}

impl Scheduler {
    /// Create a new [`Scheduler`] with the given worker slots and the default parallelism.
    ///
    /// Each hash-distributed fragment will be scheduled to at most `default_parallelism` parallel
    /// units, in a round-robin fashion on all compute nodes. If the `default_parallelism` is
    /// `None`, all worker slots will be used.
    ///
    /// For different streaming jobs, we even out possible scheduling skew by using the streaming job id as the salt for the scheduling algorithm.
    pub fn new(
        streaming_job_id: u32,
        workers: &HashMap<u32, WorkerNode>,
        default_parallelism: NonZeroUsize,
    ) -> MetaResult<Self> {
        // Group worker slots with worker node.

        let slots = workers
            .iter()
            .map(|(worker_id, worker)| (*worker_id, worker.parallelism as usize))
            .collect();

        let parallelism = default_parallelism.get();
        let scheduled = schedule_units_for_slots(&slots, parallelism, streaming_job_id)?;

        let scheduled_worker_slots = scheduled
            .into_iter()
            .flat_map(|(worker_id, size)| {
                (0..size).map(move |slot| WorkerSlotId::new(worker_id, slot))
            })
            .collect_vec();

        assert_eq!(scheduled_worker_slots.len(), parallelism);

        // Build the default hash mapping uniformly.
        // TODO(var-vnode): use vnode count from config
        let default_hash_mapping =
            WorkerSlotMapping::build_from_ids(&scheduled_worker_slots, VirtualNode::COUNT);

        let single_scheduled = schedule_units_for_slots(&slots, 1, streaming_job_id)?;
        let default_single_worker_id = single_scheduled.keys().exactly_one().cloned().unwrap();

        let default_singleton_worker_slot = WorkerSlotId::new(default_single_worker_id, 0);

        Ok(Self {
            default_hash_mapping,
            default_singleton_worker_slot,
        })
    }

    /// Schedule the given complete graph and returns the distribution of each **building
    /// fragment**.
    pub fn schedule(
        &self,
        graph: &CompleteStreamFragmentGraph,
    ) -> MetaResult<HashMap<Id, Distribution>> {
        let existing_distribution = graph.existing_distribution();

        // Build an index map for all hash mappings.
        let all_hash_mappings = existing_distribution
            .values()
            .flat_map(|dist| dist.as_hash())
            .cloned()
            .unique()
            .collect_vec();
        let hash_mapping_id: HashMap<_, _> = all_hash_mappings
            .iter()
            .enumerate()
            .map(|(i, m)| (m.clone(), i))
            .collect();

        let mut facts = Vec::new();

        // Building fragments and Singletons
        for (&id, fragment) in graph.building_fragments() {
            facts.push(Fact::Fragment(id));
            if fragment.requires_singleton {
                facts.push(Fact::SingletonReq(id));
            }
        }
        // External
        for (id, req) in existing_distribution {
            let dist = match req {
                Distribution::Singleton(worker_slot_id) => DistId::Singleton(worker_slot_id),
                Distribution::Hash(mapping) => DistId::Hash(hash_mapping_id[&mapping]),
            };
            facts.push(Fact::ExternalReq { id, dist });
        }
        // Edges
        for (from, to, edge) in graph.all_edges() {
            facts.push(Fact::Edge {
                from,
                to,
                dt: edge.dispatch_strategy.r#type(),
            });
        }

        // Run the algorithm.
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (success, failed) = crepe.run();
        if !failed.is_empty() {
            bail!("Failed to schedule: {:?}", failed);
        }
        // Should not contain any existing fragments.
        assert_eq!(success.len(), graph.building_fragments().len());

        // Extract the results.
        let distributions = success
            .into_iter()
            .map(|Success(id, result)| {
                let distribution = match result {
                    // Required
                    Result::Required(DistId::Singleton(worker_slot)) => {
                        Distribution::Singleton(worker_slot)
                    }
                    Result::Required(DistId::Hash(mapping)) => {
                        Distribution::Hash(all_hash_mappings[mapping].clone())
                    }

                    // Default
                    Result::DefaultSingleton => {
                        Distribution::Singleton(self.default_singleton_worker_slot)
                    }
                    Result::DefaultHash => Distribution::Hash(self.default_hash_mapping.clone()),
                };
                (id, distribution)
            })
            .collect();

        tracing::debug!(?distributions, "schedule fragments");

        Ok(distributions)
    }
}

/// [`Locations`] represents the worker slot and worker locations of the actors.
#[cfg_attr(test, derive(Default))]
pub struct Locations {
    /// actor location map.
    pub actor_locations: BTreeMap<ActorId, WorkerSlotId>,
    /// worker location map.
    pub worker_locations: WorkerLocations,
}

impl Locations {
    /// Returns all actors for every worker node.
    pub fn worker_actors(&self) -> HashMap<WorkerId, Vec<ActorId>> {
        self.actor_locations
            .iter()
            .map(|(actor_id, worker_slot_id)| (worker_slot_id.worker_id(), *actor_id))
            .into_group_map()
    }

    /// Returns an iterator of `ActorInfo`.
    pub fn actor_infos(&self) -> impl Iterator<Item = ActorInfo> + '_ {
        self.actor_locations
            .iter()
            .map(|(actor_id, worker_slot_id)| ActorInfo {
                actor_id: *actor_id,
                host: self.worker_locations[&worker_slot_id.worker_id()]
                    .host
                    .clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_success(facts: impl IntoIterator<Item = Fact>, expected: HashMap<Id, Result>) {
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (success, failed) = crepe.run();

        assert!(failed.is_empty());

        let success: HashMap<_, _> = success
            .into_iter()
            .map(|Success(id, result)| (id, result))
            .collect();

        assert_eq!(success, expected);
    }

    fn test_failed(facts: impl IntoIterator<Item = Fact>) {
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (_success, failed) = crepe.run();

        assert!(!failed.is_empty());
    }

    // 101
    #[test]
    fn test_single_fragment_hash() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 101
    #[test]
    fn test_single_fragment_singleton() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::SingletonReq(101.into()),
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::DefaultSingleton,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 -->
    //                103 --> 104
    // 2 -|-> 102 -->
    #[test]
    fn test_scheduling_mv_on_mv() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::Fragment(102.into()),
            Fact::Fragment(103.into()),
            Fact::Fragment(104.into()),
            Fact::ExternalReq { id: 1.into(), dist: DistId::Hash(1) },
            Fact::ExternalReq { id: 2.into(), dist: DistId::Singleton(WorkerSlotId::new(0, 2)) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 102.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 103.into(), to: 104.into(), dt: Simple },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(DistId::Hash(1)),
            102.into() => Result::Required(DistId::Singleton(WorkerSlotId::new(0, 2))),
            103.into() => Result::DefaultHash,
            104.into() => Result::DefaultSingleton,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 --> 103 -->
    //             X          105
    // 2 -|-> 102 --> 104 -->
    #[test]
    fn test_delta_join() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::Fragment(102.into()),
            Fact::Fragment(103.into()),
            Fact::Fragment(104.into()),
            Fact::Fragment(105.into()),
            Fact::ExternalReq { id: 1.into(), dist: DistId::Hash(1) },
            Fact::ExternalReq { id: 2.into(), dist: DistId::Hash(2) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 102.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 103.into(), dt: NoShuffle },
            Fact::Edge { from: 102.into(), to: 104.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 104.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 103.into(), to: 105.into(), dt: Hash },
            Fact::Edge { from: 104.into(), to: 105.into(), dt: Hash },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(DistId::Hash(1)),
            102.into() => Result::Required(DistId::Hash(2)),
            103.into() => Result::Required(DistId::Hash(1)),
            104.into() => Result::Required(DistId::Hash(2)),
            105.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 -->
    //                103
    //        102 -->
    #[test]
    fn test_singleton_leaf() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::Fragment(102.into()),
            Fact::Fragment(103.into()),
            Fact::ExternalReq { id: 1.into(), dist: DistId::Hash(1) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::SingletonReq(102.into()), // like `Now`
            Fact::Edge { from: 101.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Broadcast },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(DistId::Hash(1)),
            102.into() => Result::DefaultSingleton,
            103.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 1 -|->
    //        101
    // 2 -|->
    #[test]
    fn test_upstream_hash_shard_failed() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::ExternalReq { id: 1.into(), dist: DistId::Hash(1) },
            Fact::ExternalReq { id: 2.into(), dist: DistId::Hash(2) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 101.into(), dt: NoShuffle },
        ];

        test_failed(facts);
    }
}
