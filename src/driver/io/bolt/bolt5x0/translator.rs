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
use usize_cast::{FromUsize, IntoIsize};

use super::super::BoltStructTranslator;
use crate::driver::io::bolt::PackStreamSerializer;
use crate::value::graph::{Node, Path, Relationship, UnboundRelationship};
use crate::value::spatial::{
    Cartesian2D, Cartesian3D, SRID_CARTESIAN_2D, SRID_CARTESIAN_3D, SRID_WGS84_2D, SRID_WGS84_3D,
    WGS84_2D, WGS84_3D,
};
use crate::value::value_receive::{BrokenValue, BrokenValueInner};
use crate::{ValueReceive, ValueSend};

const TAG_2D_POINT: u8 = b'X';
const TAG_3D_POINT: u8 = b'Y';
const TAG_NODE: u8 = b'N';
const TAG_RELATIONSHIP: u8 = b'R';
const TAG_UNBOUND_RELATIONSHIP: u8 = b'r';
const TAG_PATH: u8 = b'P';

#[derive(Debug, Default)]
pub(crate) struct Bolt5x0StructTranslator {}

impl Bolt5x0StructTranslator {
    fn write_2d_point<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        srid: i64,
        coordinates: &[f64; 2],
    ) -> Result<(), S::Error> {
        serializer.write_struct_header(TAG_2D_POINT, 3)?;
        serializer.write_int(srid)?;
        serializer.write_float(coordinates[0])?;
        serializer.write_float(coordinates[1])
    }

    fn write_3d_point<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        srid: i64,
        coordinates: &[f64; 3],
    ) -> Result<(), S::Error> {
        serializer.write_struct_header(TAG_3D_POINT, 4)?;
        serializer.write_int(srid)?;
        serializer.write_float(coordinates[0])?;
        serializer.write_float(coordinates[1])?;
        serializer.write_float(coordinates[2])
    }
}

impl BoltStructTranslator for Bolt5x0StructTranslator {
    fn serialize<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        value: &ValueSend,
    ) -> Result<(), S::Error> {
        match value {
            ValueSend::Null => serializer.write_null(),
            ValueSend::Boolean(b) => serializer.write_bool(*b),
            ValueSend::Integer(i) => serializer.write_int(*i),
            ValueSend::Float(f) => serializer.write_float(*f),
            ValueSend::Bytes(b) => serializer.write_bytes(b),
            ValueSend::String(s) => serializer.write_string(s),
            ValueSend::List(l) => {
                serializer.write_list_header(u64::from_usize(l.len()))?;
                for v in l {
                    self.serialize(serializer, v)?;
                }
                Ok(())
            }
            ValueSend::Map(d) => {
                serializer.write_dict_header(u64::from_usize(d.len()))?;
                for (k, v) in d {
                    serializer.write_string(k)?;
                    self.serialize(serializer, v)?;
                }
                Ok(())
            }
            ValueSend::Cartesian2D(Cartesian2D { srid, coordinates }) => {
                self.write_2d_point(serializer, *srid, coordinates)
            }
            ValueSend::Cartesian3D(Cartesian3D { srid, coordinates }) => {
                self.write_3d_point(serializer, *srid, coordinates)
            }
            ValueSend::WGS84_2D(WGS84_2D { srid, coordinates }) => {
                self.write_2d_point(serializer, *srid, coordinates)
            }
            ValueSend::WGS84_3D(WGS84_3D { srid, coordinates }) => {
                self.write_3d_point(serializer, *srid, coordinates)
            }
        }
    }

    fn deserialize_struct(&self, tag: u8, fields: Vec<ValueReceive>) -> ValueReceive {
        let size = fields.len();
        let mut fields = VecDeque::from(fields);
        match tag {
            TAG_2D_POINT => {
                if size != 3 {
                    return invalid_struct(format!(
                        "expected 3 fields for point 2D struct b'X', found {size}"
                    ));
                }
                let srid = match fields.pop_front().unwrap() {
                    ValueReceive::Integer(i) => i,
                    v => {
                        return invalid_struct(format!(
                            "expected 2D SRID to be an integer, found {v:?}"
                        ));
                    }
                };
                let mut coordinates = [0.0; 2];
                for (i, coordinate) in coordinates.iter_mut().enumerate() {
                    *coordinate = match fields.pop_front().unwrap() {
                        ValueReceive::Float(f) => f,
                        v => {
                            return invalid_struct(format!(
                                "expected {i}th 2D coordinate to be a float, found {v:?}"
                            ));
                        }
                    };
                }
                let x = coordinates[0];
                let y = coordinates[1];
                match srid {
                    SRID_CARTESIAN_2D => ValueReceive::Cartesian2D(Cartesian2D::new(x, y)),
                    SRID_WGS84_2D => ValueReceive::WGS84_2D(WGS84_2D::new(x, y)),
                    srid => invalid_struct(format!("unknown 2D SRID {srid}")),
                }
            }
            TAG_3D_POINT => {
                if size != 4 {
                    return invalid_struct(format!(
                        "expected 4 fields for point 3D struct b'X', found {size}"
                    ));
                }
                let srid = match fields.pop_front().unwrap() {
                    ValueReceive::Integer(i) => i,
                    v => {
                        return invalid_struct(format!(
                            "expected 3D SRID to be an integer, found {v:?}"
                        ));
                    }
                };
                let mut coordinates = [0.0; 3];
                for (i, coordinate) in coordinates.iter_mut().enumerate() {
                    *coordinate = match fields.pop_front().unwrap() {
                        ValueReceive::Float(f) => f,
                        v => {
                            return invalid_struct(format!(
                                "expected {i}th 3D coordinate to be a float, found {v:?}"
                            ));
                        }
                    };
                }
                let x = coordinates[0];
                let y = coordinates[1];
                let z = coordinates[2];
                match srid {
                    SRID_CARTESIAN_3D => ValueReceive::Cartesian3D(Cartesian3D::new(x, y, z)),
                    SRID_WGS84_3D => ValueReceive::WGS84_3D(WGS84_3D::new(x, y, z)),
                    srid => invalid_struct(format!("unknown 3D SRID {srid}")),
                }
            }
            TAG_NODE => {
                if size != 4 {
                    return invalid_struct(format!(
                        "expected 4 fields for node struct b'N', found {size}"
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
                let element_id = match fields.pop_front().unwrap() {
                    ValueReceive::String(s) => s,
                    v => {
                        return invalid_struct(format!(
                            "expected node element_id to be a string, found {v:?}"
                        ));
                    }
                };
                ValueReceive::Node(Node {
                    id,
                    labels,
                    properties,
                    element_id,
                })
            }
            TAG_RELATIONSHIP => {
                if size != 8 {
                    return invalid_struct(format!(
                        "expected 8 fields for relationship struct b'R', found {size}"
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
                let element_id = match fields.pop_front().unwrap() {
                    ValueReceive::String(s) => s,
                    v => {
                        return invalid_struct(format!(
                            "expected relationship element_id to be a string, found {v:?}"
                        ));
                    }
                };
                let start_node_element_id = match fields.pop_front().unwrap() {
                    ValueReceive::String(s) => s,
                    v => {
                        return invalid_struct(format!(
                            "expected relationship start_node_element_id to be a string, found {v:?}"
                        ));
                    }
                };
                let end_node_element_id = match fields.pop_front().unwrap() {
                    ValueReceive::String(s) => s,
                    v => {
                        return invalid_struct(format!(
                            "expected relationship end_node_element_id to be a string, found {v:?}"
                        ))
                    }
                };
                ValueReceive::Relationship(Relationship {
                    id,
                    start_node_id,
                    end_node_id,
                    type_,
                    properties,
                    element_id,
                    start_node_element_id,
                    end_node_element_id,
                })
            }
            TAG_PATH => {
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
                                    if rel_size != 4 {
                                        return invalid_struct(
                                            format!(
                                                "expected 4 fields for unbound relationship struct b'r', found {rel_size}",
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
                                    let element_id = match rel_fields.pop_front().unwrap() {
                                        ValueReceive::String(s) => s,
                                        v => {
                                            return invalid_struct(
                                                format!(
                                                    "expected unbound relationship element_id to be a string, found {v:?}"
                                                ),
                                            )
                                        }
                                    };
                                    UnboundRelationship {
                                        id,
                                        type_,
                                        properties,
                                        element_id,
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
            _ => ValueReceive::BrokenValue(BrokenValue {
                inner: BrokenValueInner::UnknownStruct { tag, fields },
            }),
        }
    }
}

fn invalid_struct(reason: String) -> ValueReceive {
    ValueReceive::BrokenValue(BrokenValueInner::InvalidStruct { reason }.into())
}
