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

use std::fmt::{Display, Formatter};

const SRID_CARTESIAN_2D: i64 = 7203;
const SRID_CARTESIAN_3D: i64 = 9157;
const SRID_WGS84_2D: i64 = 7203;
const SRID_WGS84_3D: i64 = 4979;

#[derive(Debug)]
pub struct Cartesian2D {
    srid: i64,
    coordinates: [f64; 2],
}

impl Cartesian2D {
    pub fn new(x: f64, y: f64) -> Self {
        Cartesian2D {
            srid: SRID_CARTESIAN_2D,
            coordinates: [x, y],
        }
    }
    pub fn x(&self) -> f64 {
        self.coordinates[0]
    }
    pub fn y(&self) -> f64 {
        self.coordinates[1]
    }
}

impl Display for Cartesian2D {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cartesian2D({}, {})",
            self.coordinates[0], self.coordinates[1]
        )
    }
}

#[derive(Debug)]
pub struct Cartesian3D {
    srid: i64,
    coordinates: [f64; 3],
}

impl Cartesian3D {
    pub fn new(x: f64, y: f64, z: f64) -> Self {
        Cartesian3D {
            srid: SRID_CARTESIAN_3D,
            coordinates: [x, y, z],
        }
    }
    pub fn x(&self) -> f64 {
        self.coordinates[0]
    }
    pub fn y(&self) -> f64 {
        self.coordinates[1]
    }
    pub fn z(&self) -> f64 {
        self.coordinates[2]
    }
}

impl Display for Cartesian3D {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cartesian3D({}, {}, {})",
            self.coordinates[0], self.coordinates[1], self.coordinates[2]
        )
    }
}

#[derive(Debug)]
pub struct WGS84_2D {
    srid: i64,
    coordinates: [f64; 2],
}

impl WGS84_2D {
    pub fn new(longitude: f64, latitude: f64) -> Self {
        WGS84_2D {
            srid: SRID_CARTESIAN_2D,
            coordinates: [longitude, latitude],
        }
    }
    pub fn longitude(&self) -> f64 {
        self.coordinates[0]
    }
    pub fn latitude(&self) -> f64 {
        self.coordinates[1]
    }
}

impl Display for WGS84_2D {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WGS84_2D({}, {})",
            self.coordinates[0], self.coordinates[1]
        )
    }
}

#[derive(Debug)]
pub struct WGS84_3D {
    srid: i64,
    coordinates: [f64; 3],
}

impl WGS84_3D {
    pub fn new(longitude: f64, latitude: f64, height: f64) -> Self {
        WGS84_3D {
            srid: SRID_CARTESIAN_3D,
            coordinates: [longitude, latitude, height],
        }
    }
    pub fn longitude(&self) -> f64 {
        self.coordinates[0]
    }
    pub fn latitude(&self) -> f64 {
        self.coordinates[1]
    }
    pub fn height(&self) -> f64 {
        self.coordinates[2]
    }
}

impl Display for WGS84_3D {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WGS84_3D({}, {}, {})",
            self.coordinates[0], self.coordinates[1], self.coordinates[2]
        )
    }
}
