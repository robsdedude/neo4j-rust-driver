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

use super::BackendId;
use serde::Serialize;

#[derive(Serialize, Debug)]
#[serde(tag = "name", content = "data")]
pub(crate) enum Response {
    BackendError { msg: String },
    FeatureList { features: Vec<String> },
    RunTest,
    RunSubTests,
    SkipTest { reason: String },
    Driver { id: BackendId },
}

impl Response {
    pub(crate) fn feature_list() -> Self {
        Self::FeatureList {
            features: [
                // "Feature:API:BookmarkManager",
                // "Feature:API:ConnectionAcquisitionTimeout",
                // "Feature:API:Driver.ExecuteQuery",
                // "Feature:API:Driver:GetServerInfo",
                // "Feature:API:Driver.IsEncrypted",
                // "Feature:API:Driver:NotificationsConfig",
                // "Feature:API:Driver.VerifyConnectivity",
                // "Feature:API:Liveness.Check",
                // "Feature:API:Result.List",
                // "Feature:API:Result.Peek",
                // "Feature:API:Result.Single",
                // "Feature:API:Result.SingleOptional",
                // "Feature:API:Session:NotificationsConfig",
                // "Feature:API:SSLConfig",
                // "Feature:API:SSLSchemes",
                "Feature:API:Type.Spatial",
                // "Feature:API:Type.Temporal",
                // "Feature:Auth:Bearer",
                // "Feature:Auth:Custom",
                // "Feature:Auth:Kerberos",
                // "Feature:Bolt:3.0",
                // "Feature:Bolt:4.1",
                // "Feature:Bolt:4.2",
                // "Feature:Bolt:4.3",
                // "Feature:Bolt:4.4",
                "Feature:Bolt:5.0",
                // "Feature:Bolt:5.1",
                // "Feature:Bolt:5.2",
                // "Feature:Bolt:Patch:UTC",
                "Feature:Impersonation",
                // "Feature:TLS:1.1",
                // "Feature:TLS:1.2",
                // "Feature:TLS:1.3",

                // "AuthorizationExpiredTreatment",
                "Optimization:ConnectionReuse",
                // "Optimization:EagerTransactionBegin",
                "Optimization:ImplicitDefaultArguments",
                "Optimization:MinimalBookmarksSet",
                "Optimization:MinimalResets",
                // "Optimization:AuthPipelining",
                "Optimization:PullPipelining",
                // "Optimization:ResultListFetchAll",

                // "Detail:ClosedDriverIsEncrypted",
                // "Detail:DefaultSecurityConfigValueEquality",
                // "ConfHint:connection.recv_timeout_seconds",

                // "Backend:RTFetch",
                // "Backend:RTForceUpdate",
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        }
    }
}
