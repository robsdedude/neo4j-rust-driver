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

use std::collections::VecDeque;
use usize_cast::IntoIsize;

use super::super::bolt5x0::Bolt5x0StructTranslator;
use super::super::{BoltStructTranslator, BoltStructTranslatorWithUtcPatch};
use crate::driver::io::bolt::PackStreamSerializer;
use crate::value::graph::{Node, Path, Relationship, UnboundRelationship};
use crate::value::value_receive::{BrokenValue, BrokenValueInner};
use crate::{ValueReceive, ValueSend};

const TAG_NODE: u8 = b'N';
const TAG_RELATIONSHIP: u8 = b'R';
const TAG_UNBOUND_RELATIONSHIP: u8 = b'r';
const TAG_PATH: u8 = b'P';

#[derive(Debug, Default)]
pub(crate) struct Bolt4x4StructTranslator {
    utc_patch: bool,
    bolt5x5_translator: Bolt5x0StructTranslator,
}

impl BoltStructTranslator for Bolt4x4StructTranslator {
    fn serialize<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        value: &ValueSend,
    ) -> Result<(), S::Error> {
        self.bolt5x5_translator.serialize(serializer, value)
    }

    fn deserialize_struct(&self, tag: u8, fields: Vec<ValueReceive>) -> ValueReceive {
        match tag {
            TAG_NODE => {
                let size = fields.len();
                let mut fields = VecDeque::from(fields);
                if size != 3 {
                    return invalid_struct(format!(
                        "expected 3 fields for node struct b'N', found {size}"
                    ));
                }
                let id = match fields.pop_front().unwrap() {
                    ValueReceive::Integer(i) => i,
                    v => {
                        return invalid_struct(format!(
                            "expected node id be an integer, found {v:?}"
                        ));
                    }
                };
                let labels = match fields.pop_front().unwrap() {
                    ValueReceive::List(v) => {
                        let mut labels = Vec::with_capacity(v.len());
                        for label in v {
                            labels.push(match label {
                                ValueReceive::String(s) => s,
                                v => {
                                    return invalid_struct(format!(
                                        "expected node label to be a string, found {v:?}"
                                    ));
                                }
                            });
                        }
                        labels
                    }
                    v => {
                        return invalid_struct(format!(
                            "expected node id be an integer, found {v:?}"
                        ));
                    }
                };
                let properties = match fields.pop_front().unwrap() {
                    ValueReceive::Map(m) => m,
                    v => {
                        return invalid_struct(format!(
                            "expected node properties to be a map, found {v:?}"
                        ));
                    }
                };
                ValueReceive::Node(Node {
                    id,
                    labels,
                    properties,
                    element_id: format!("{id}"),
                })
            }
            TAG_RELATIONSHIP => {
                let size = fields.len();
                let mut fields = VecDeque::from(fields);
                if size != 5 {
                    return invalid_struct(format!(
                        "expected 5 fields for relationship struct b'R', found {size}"
                    ));
                }
                let id = match fields.pop_front().unwrap() {
                    ValueReceive::Integer(i) => i,
                    v => {
                        return invalid_struct(format!(
                            "expected relationship id be an integer, found {v:?}"
                        ));
                    }
                };
                let start_node_id = match fields.pop_front().unwrap() {
                    ValueReceive::Integer(i) => i,
                    v => {
                        return invalid_struct(format!(
                            "expected relationship start_node_id be an integer, found {v:?}"
                        ));
                    }
                };
                let end_node_id = match fields.pop_front().unwrap() {
                    ValueReceive::Integer(i) => i,
                    v => {
                        return invalid_struct(format!(
                            "expected relationship end_node_id be an integer, found {v:?}"
                        ));
                    }
                };
                let type_ = match fields.pop_front().unwrap() {
                    ValueReceive::String(s) => s,
                    v => {
                        return invalid_struct(format!(
                            "expected relationship type to be a string, found {v:?}"
                        ));
                    }
                };
                let properties = match fields.pop_front().unwrap() {
                    ValueReceive::Map(m) => m,
                    v => {
                        return invalid_struct(format!(
                            "expected relationship properties to be a map, found {v:?}"
                        ));
                    }
                };
                ValueReceive::Relationship(Relationship {
                    id,
                    start_node_id,
                    end_node_id,
                    type_,
                    properties,
                    element_id: format!("{id}"),
                    start_node_element_id: format!("{start_node_id}"),
                    end_node_element_id: format!("{end_node_id}"),
                })
            }
            TAG_PATH => {
                let size = fields.len();
                let mut fields = VecDeque::from(fields);
                if size != 3 {
                    return invalid_struct(format!(
                        "expected 3 fields for path struct b'P', found {size}"
                    ));
                }
                let nodes = match fields.pop_front().unwrap() {
                    ValueReceive::List(v) => {
                        let mut nodes = Vec::with_capacity(v.len());
                        for node in v {
                            nodes.push(match node {
                                ValueReceive::Node(n) => n,
                                v => {
                                    return invalid_struct(format!(
                                        "expected path node to be a Node, found {v:?}"
                                    ));
                                }
                            });
                        }
                        nodes
                    }
                    v => {
                        return invalid_struct(format!(
                            "expected path nodes to be a list, found {v:?}"
                        ));
                    }
                };
                let relationships = match fields.pop_front().unwrap() {
                    ValueReceive::List(v) => {
                        let mut relationships = Vec::with_capacity(v.len());
                        for relationship in v {
                            relationships.push(match relationship {
                                ValueReceive::BrokenValue(BrokenValue {
                                    inner:
                                        BrokenValueInner::UnknownStruct {
                                            tag: rel_tag,
                                            fields: mut rel_fields,
                                        },
                                }) if rel_tag == TAG_UNBOUND_RELATIONSHIP => {
                                    let rel_size = rel_fields.len();
                                    if rel_size != 3 {
                                        return invalid_struct(
                                            format!(
                                                "expected 3 fields for unbound relationship struct b'r', found {rel_size}",
                                            ),
                                        );
                                    }
                                    let id = match rel_fields.pop_front().unwrap() {
                                        ValueReceive::Integer(i) => i,
                                        v => {
                                            return invalid_struct(
                                                format!(
                                                    "expected unbound relationship id be an integer, found {v:?}"
                                                ),
                                            );
                                        }
                                    };
                                    let type_ = match rel_fields.pop_front().unwrap() {
                                        ValueReceive::String(s) => s,
                                        v => {
                                            return invalid_struct(
                                                format!(
                                                    "expected unbound relationship type to be a string, found {v:?}"
                                                ),
                                            );
                                        }
                                    };
                                    let properties = match rel_fields.pop_front().unwrap() {
                                        ValueReceive::Map(m) => m,
                                        v => {
                                            return invalid_struct(
                                                format!(
                                                    "expected unbound relationship properties to be a map, found {v:?}"
                                                ),
                                            );
                                        }
                                    };
                                    UnboundRelationship {
                                        id,
                                        type_,
                                        properties,
                                        element_id: format!("{id}"),
                                    }
                                }
                                v => {
                                    return invalid_struct(
                                        format!("expected path relationship to be an unbound relationship, found {v:?}"),
                                    )
                                }
                            });
                        }
                        relationships
                    }
                    v => {
                        return invalid_struct(format!(
                            "expected path nodes to be a list, found {v:?}"
                        ))
                    }
                };
                let indices = match fields.pop_front().unwrap() {
                    ValueReceive::List(v) => {
                        let mut indices = Vec::with_capacity(v.len());
                        for index in v {
                            indices.push(match index {
                                ValueReceive::Integer(i) => i.into_isize(),
                                v => {
                                    return invalid_struct(format!(
                                        "expected path index to be an integer, found {v:?}"
                                    ))
                                }
                            });
                        }
                        indices
                    }
                    v => {
                        return invalid_struct(format!(
                            "expected path indices to be a list, found {v:?}"
                        ))
                    }
                };
                ValueReceive::Path(Path {
                    nodes,
                    relationships,
                    indices,
                })
            }
            _ => self.bolt5x5_translator.deserialize_struct(tag, fields),
        }
    }
}

fn invalid_struct(reason: String) -> ValueReceive {
    ValueReceive::BrokenValue(BrokenValueInner::InvalidStruct { reason }.into())
}

impl BoltStructTranslatorWithUtcPatch for Bolt4x4StructTranslator {
    fn enable_utc_patch(&mut self) {
        self.utc_patch = true;
    }
}
