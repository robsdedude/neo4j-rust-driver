// Copyright Rouven Bauer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use super::ValueReceive;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Node {
    pub id: i64,
    pub labels: Vec<String>,
    pub properties: HashMap<String, ValueReceive>,
    pub element_id: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node(labels={:?}, element_id={}, properties={:?})",
            self.labels, self.element_id, self.properties
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Relationship {
    pub id: i64,
    pub start_node_id: i64,
    pub end_node_id: i64,
    pub type_: String,
    pub properties: HashMap<String, ValueReceive>,
    pub element_id: String,
    pub start_node_element_id: String,
    pub end_node_element_id: String,
}

impl Display for Relationship {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Relationship(type={}, element_id={}, start_node_element_id={}, end_node_element_id={}, properties={:?})",
            self.type_, self.element_id, self.start_node_element_id, self.end_node_element_id, self.properties
        )
    }
}

/// # Invariants
///  * `indices`
///    * is not empty
///    * has an even number of elements
///    * 1st, 3rd, ... entry is in the range
///      `-self.nodes.len()..0` or `1..=self.relationships.len()`
///    * 2nd, 4th, ... entry is in the range `0..self.nodes.len()`
///  * (this implies `nodes` and `relationships` are not empty)
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub nodes: Vec<Node>,
    pub relationships: Vec<UnboundRelationship>,
    /// complicated stuff, explain properly!
    /// https://neo4j.com/docs/bolt/current/bolt/structure-semantics/#structure-path
    pub indices: Vec<isize>,
}

impl Path {
    /// # Panics
    /// Panics if `self.nodes`, `self.relationships` or `self.indices` has been tempered with, in
    /// a way that violates the invariants of a path.
    /// Such an invalid path cannot be obtained from the database, as the database's return values
    /// are checked for validity before being converted to `Path`.
    pub fn traverse(&self) -> Vec<(&Node, &UnboundRelationship, &Node)> {
        let mut result = Vec::with_capacity(self.indices.len() / 2);
        let mut index_iter = self.indices.iter();
        let mut prev_node_idx = 0;
        let mut relationship_idx = *index_iter.next().expect("indices cannot be empty");
        let mut next_node_idx = index_iter
            .next()
            .expect("indices must contain at least 2 elements")
            .to_owned()
            .try_into()
            .expect("2nd, 4th, ... entry in indices must be in the >= 0");
        loop {
            let mut start_node = &self.nodes[prev_node_idx];
            let mut end_node = &self.nodes[next_node_idx];
            if relationship_idx < 0 {
                (start_node, end_node) = (end_node, start_node);
                relationship_idx = -relationship_idx;
            }
            relationship_idx -= 1;
            let relationship = {
                let relationship_idx: usize = relationship_idx
                    .try_into()
                    .expect("1st, 3rd, ... entry in indices cannot be 0");
                &self.relationships[relationship_idx]
            };
            result.push((start_node, relationship, end_node));

            let Some(next_relationship_idx) = index_iter.next() else {
                break;
            };
            relationship_idx = *next_relationship_idx;
            prev_node_idx = next_node_idx;
            next_node_idx = index_iter
                .next()
                .expect("indices must contain an even number of elements")
                .to_owned()
                .try_into()
                .expect("2nd, 4th, ... entry in indices must be >= 0");
        }
        result
    }
}

/// # Panics
/// Panics if `Path`'s invariants are violated.
impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut last_node = &self.nodes[0];
        write!(f, "({})", last_node.element_id)?;
        for (start_node, relationship, end_node) in self.traverse() {
            if last_node.element_id == start_node.element_id {
                write!(
                    f,
                    "-[{}]->({})",
                    relationship.element_id, end_node.element_id
                )?;
                last_node = end_node;
            } else {
                assert_eq!(last_node.element_id, end_node.element_id);
                write!(
                    f,
                    "<-[{}]-({})",
                    relationship.element_id, start_node.element_id
                )?;
                last_node = start_node;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnboundRelationship {
    pub id: i64,
    pub type_: String,
    pub properties: HashMap<String, ValueReceive>,
    pub element_id: String,
}
