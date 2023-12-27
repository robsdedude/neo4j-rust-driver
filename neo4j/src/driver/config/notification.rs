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

// imports for docs
#[allow(unused)]
use super::super::session::config::SessionConfig;
#[allow(unused)]
use super::super::ExecuteQueryBuilder;
#[allow(unused)]
use super::DriverConfig;
#[allow(unused)]
use crate::Neo4jError;

/// Defines which notifications the server should send to the client.
///
/// See [`DriverConfig::with_notification_filter()`], [`SessionConfig::with_notification_filter()`],
/// and [`ExecuteQueryBuilder::with_notification_filter()`].
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct NotificationFilter {
    /// Request the server to only send notifications of this severity or higher.
    ///
    /// Set to [`None`] to leave it up to the server to choose a default.
    pub minimum_severity: Option<MinimumSeverity>,
    /// Request the server to not send notifications of these categories.
    ///
    /// Set to [`None`] to leave it up to the server to choose a default.
    /// Set to an empty [`Vec`] to make sure no categories are disabled.
    pub disabled_categories: Option<Vec<DisabledCategory>>,
}

impl NotificationFilter {
    /// Create a new [`NotificationFilter`] which requests the server to not send any notifications.
    pub fn disable_all() -> Self {
        Self {
            minimum_severity: Some(MinimumSeverity::Disabled),
            disabled_categories: None,
        }
    }

    pub(crate) fn is_default(&self) -> bool {
        self.minimum_severity.is_none() && self.disabled_categories.is_none()
    }
}

/// See [`NotificationFilter::minimum_severity`].
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum MinimumSeverity {
    /// Disable all notifications
    Disabled,
    Warning,
    Information,
}

impl MinimumSeverity {
    pub(crate) fn as_protocol_str(&self) -> &'static str {
        match self {
            Self::Disabled => "OFF",
            Self::Warning => "WARNING",
            Self::Information => "INFORMATION",
        }
    }
}

/// See [`NotificationFilter::disabled_categories`].
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum DisabledCategory {
    Hint,
    Unrecognized,
    Unsupported,
    Performance,
    Deprecation,
    Generic,
    /// This requires Neo4j 5.13 or newer.
    /// For older versions, this will result in a [`Neo4jError::ServerError`].
    Security,
    /// This requires Neo4j 5.13 or newer.
    /// For older versions, this will result in a [`Neo4jError::ServerError`].
    Topology,
}

impl DisabledCategory {
    pub(crate) fn as_protocol_str(&self) -> &'static str {
        match self {
            Self::Hint => "HINT",
            Self::Unrecognized => "UNRECOGNIZED",
            Self::Unsupported => "UNSUPPORTED",
            Self::Performance => "PERFORMANCE",
            Self::Deprecation => "DEPRECATION",
            Self::Generic => "GENERIC",
            Self::Security => "SECURITY",
            Self::Topology => "TOPOLOGY",
        }
    }
}
