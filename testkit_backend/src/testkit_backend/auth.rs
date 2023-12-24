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

use std::sync::Arc;
use std::time::Duration;

use atomic_refcell::AtomicRefCell;

use neo4j::driver::auth::{
    auth_managers, AuthManager, AuthToken, ManagerGetAuthReturn, ManagerHandleErrReturn,
};
use neo4j::error::ServerError;
use neo4j::time::Instant;

use super::backend_id::{BackendId, Generator};
use super::errors::TestKitError;
use super::requests::{AuthTokenAndExpiration, Request, TestKitAuth};
use super::responses::Response;
use super::BackendIo;

pub(super) struct TestKitAuthManagers {}

impl TestKitAuthManagers {
    pub(super) fn new_basic(
        backend_io: Arc<AtomicRefCell<BackendIo>>,
        id_generator: Generator,
    ) -> (BackendId, Arc<dyn AuthManager>) {
        let manager_id = id_generator.next_id();

        let manager = Arc::new(auth_managers::new_basic(move || {
            let mut io = backend_io.borrow_mut();
            let id = id_generator.next_id();
            io.send(&Response::BasicAuthTokenProviderRequest {
                id,
                basic_auth_token_manager_id: manager_id,
            })?;
            let request = io.read_request()?;
            let request: Request = match serde_json::from_str(&request) {
                Ok(req) => req,
                Err(err) => return Err(Box::new(TestKitError::from(err))),
            };
            match request {
                Request::BasicAuthTokenProviderCompleted { request_id, auth } => {
                    if request_id != id {
                        return Err(Box::new(TestKitError::backend_err(format!(
                            "expected BasicAuthTokenProviderCompleted for id {}, received for {}",
                            id, request_id
                        ))));
                    }
                    Ok(auth.try_into()?)
                }
                _ => Err(Box::new(TestKitError::backend_err(format!(
                    "expected BasicAuthTokenProviderCompleted, received {:?}",
                    request
                )))),
            }
        }));

        (manager_id, manager)
    }

    pub(super) fn new_bearer(
        backend_io: Arc<AtomicRefCell<BackendIo>>,
        id_generator: Generator,
    ) -> (BackendId, Arc<dyn AuthManager>) {
        let manager_id = id_generator.next_id();

        let manager = Arc::new(auth_managers::new_bearer(move || {
            let mut io = backend_io.borrow_mut();
            let id = id_generator.next_id();
            io.send(&Response::BearerAuthTokenProviderRequest {
                id,
                bearer_auth_token_manager_id: manager_id,
            })?;
            let request = io.read_request()?;
            let request: Request = match serde_json::from_str(&request) {
                Ok(req) => req,
                Err(err) => return Err(Box::new(TestKitError::from(err))),
            };
            match request {
                Request::BearerAuthTokenProviderCompleted { request_id, auth } => {
                    if request_id != id {
                        return Err(Box::new(TestKitError::backend_err(format!(
                            "expected BearerAuthTokenProviderCompleted for id {}, received for {}",
                            id, request_id
                        ))));
                    }
                    let AuthTokenAndExpiration::AuthTokenAndExpiration {
                        auth,
                        expires_in_ms,
                    } = auth;
                    let expires = expires_in_ms
                        .map(|ms| {
                            let now = Instant::now().raw();
                            if ms >= 0 {
                                now.checked_add(Duration::from_millis(ms as u64))
                                    .ok_or_else(|| {
                                        TestKitError::backend_err("expiration time out of bounds")
                                    })
                            } else {
                                now.checked_sub(Duration::from_millis((-ms) as u64))
                                    .ok_or_else(|| {
                                        TestKitError::backend_err("expiration time out of bounds")
                                    })
                            }
                        })
                        .transpose()?;
                    Ok((auth.try_into()?, expires))
                }
                _ => Err(Box::new(TestKitError::backend_err(format!(
                    "expected BearerAuthTokenProviderCompleted, received {:?}",
                    request
                )))),
            }
        }));

        (manager_id, manager)
    }

    pub(super) fn new_custom(
        backend_io: Arc<AtomicRefCell<BackendIo>>,
        id_generator: Generator,
    ) -> (BackendId, Arc<dyn AuthManager>) {
        let manager_id = id_generator.next_id();

        let manager = Arc::new(TestKitCustomAuthManager {
            id: manager_id,
            backend_io,
            id_generator,
        });

        (manager_id, manager)
    }
}

#[derive(Debug)]
pub(super) struct TestKitCustomAuthManager {
    id: BackendId,
    backend_io: Arc<AtomicRefCell<BackendIo>>,
    id_generator: Generator,
}

impl AuthManager for TestKitCustomAuthManager {
    fn get_auth(&self) -> ManagerGetAuthReturn {
        let mut io = self.backend_io.borrow_mut();
        let id = self.id_generator.next_id();
        io.send(&Response::AuthTokenManagerGetAuthRequest {
            id,
            auth_token_manager_id: self.id,
        })?;
        let request = io.read_request()?;
        let request: Request = match serde_json::from_str(&request) {
            Ok(req) => req,
            Err(err) => return Err(Box::new(TestKitError::from(err))),
        };
        match request {
            Request::AuthTokenManagerGetAuthCompleted { request_id, auth } => {
                if request_id != id {
                    return Err(Box::new(TestKitError::backend_err(format!(
                        "expected AuthTokenManagerGetAuthCompleted for id {}, received for {}",
                        id, request_id
                    ))));
                }
                Ok(Arc::new(auth.try_into()?))
            }
            _ => Err(Box::new(TestKitError::backend_err(format!(
                "expected AuthTokenManagerGetAuthCompleted, received {:?}",
                request
            )))),
        }
    }

    fn handle_security_error(
        &self,
        auth: &Arc<AuthToken>,
        error: &ServerError,
    ) -> ManagerHandleErrReturn {
        let mut io = self.backend_io.borrow_mut();
        let id = self.id_generator.next_id();
        io.send(&Response::AuthTokenManagerHandleSecurityExceptionRequest {
            id,
            auth_token_manager_id: self.id,
            auth: TestKitAuth::try_clone_auth_token(auth)?,
            error_code: String::from(error.code()),
        })?;
        let request = io.read_request()?;
        let request: Request = match serde_json::from_str(&request) {
            Ok(req) => req,
            Err(err) => return Err(Box::new(TestKitError::from(err))),
        };
        match request {
            Request::AuthTokenManagerHandleSecurityExceptionCompleted {
                request_id,
                handled,
            } => {
                if request_id != id {
                    return Err(Box::new(TestKitError::backend_err(format!(
                        "expected AuthTokenManagerHandleSecurityExceptionCompleted for id {}, received for {}",
                        id, request_id
                    ))));
                }
                Ok(handled)
            }
            _ => Err(Box::new(TestKitError::backend_err(format!(
                "expected AuthTokenManagerGetAuthCompleted, received {:?}",
                request
            )))),
        }
    }
}
