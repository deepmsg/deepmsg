//! Who may join a cluster session, and what a joined session may do.
//!
//! Counterpart of `io.aeron.security.{Authenticator, SessionProxy,
//! AuthorisationService}` plus the cluster's two supplier lookups
//! (`ConsensusModule.java:1307-1370`). The interfaces live in `aeron-client` in
//! the reference, not in the cluster, because the archive uses the same two
//! questions.
//!
//! # Names, not reflection
//!
//! The reference loads a supplier by class name. This build has no class
//! loading, so the names are a **closed set**: an empty authenticator name is
//! the reference's default supplier, an empty authorisation name is the
//! reference's default service (the one that allows backup and standby queries
//! and nothing else), and any other name is refused at launch, naming the
//! property and the value. Quietly falling back to the default for a name
//! nobody recognised is the failure mode the project refuses (LC-6: no name is
//! silently ignored).

use deepmsg_codec_cluster::SBE_SCHEMA_ID;
use deepmsg_codec_cluster::backup_query_codec::SBE_TEMPLATE_ID as BACKUP_QUERY_TEMPLATE_ID;
use deepmsg_codec_cluster::heartbeat_request_codec::SBE_TEMPLATE_ID as HEARTBEAT_REQUEST_TEMPLATE_ID;
use deepmsg_codec_cluster::standby_snapshot_codec::SBE_TEMPLATE_ID as STANDBY_SNAPSHOT_TEMPLATE_ID;

use crate::config::{DEFAULT_AUTHENTICATOR_SUPPLIER, MAX_ENCODED_PRINCIPAL_LENGTH};

/// `AuthorisationService.ALLOW_ALL_NAME` (`AuthorisationService.java:44`).
pub const ALLOW_ALL_NAME: &str = "ALLOW_ALL";

/// `AuthorisationService.DENY_ALL_NAME` (`AuthorisationService.java:39`).
pub const DENY_ALL_NAME: &str = "DENY_ALL";

/// The identity a session shows to an [`Authenticator`] for the duration of one
/// callback (`SessionProxy.java`).
///
/// The reference binds a `ClusterSessionProxy` to a session and a publication
/// before each call and says the proxy is valid only for that call. This is the
/// same lifetime, expressed as a trait object instead of a field.
pub trait SessionProxy {
    /// The id the system assigned to the potential session.
    fn session_id(&self) -> i64;

    /// Ask for the challenge to be sent; `false` means "not now, try later".
    ///
    /// # Errors
    ///
    /// The send's own failure — the reference's `ClusterException` out of the
    /// egress publisher, which is control flow here.
    fn challenge(&mut self, encoded_challenge: &[u8]) -> Result<bool, crate::ClusterError>;

    /// The credentials passed; `false` means "not now, try later".
    ///
    /// # Errors
    ///
    /// [`crate::ClusterError::Cluster`] when the principal exceeds
    /// `MAX_ENCODED_PRINCIPAL_LENGTH` — the reference throws here too
    /// (`ClusterSession.java:537-545`), and the throw must reach `doWork`.
    fn authenticate(&mut self, encoded_principal: &[u8]) -> Result<bool, crate::ClusterError>;

    /// The session should be rejected.
    fn reject(&mut self);
}

/// The reference's `Authenticator` (`Authenticator.java:26-69`).
pub trait Authenticator {
    /// Called on a connect request, before the response channel is connected.
    fn on_connect_request(&mut self, session_id: i64, encoded_credentials: &[u8], now_ms: i64);

    /// Called on a challenge response from a challenged session.
    fn on_challenge_response(&mut self, session_id: i64, encoded_credentials: &[u8], now_ms: i64);

    /// The response channel is connected; may be called repeatedly.
    ///
    /// # Errors
    ///
    /// Whatever the proxy's answer failed with. The reference's proxy throws
    /// out of the authenticator call; here it is a `Result` so the duty cycle
    /// can propagate it with `?` (plan §4.4, rule 5).
    fn on_connected_session(
        &mut self,
        session: &mut dyn SessionProxy,
        now_ms: i64,
    ) -> Result<(), crate::ClusterError>;

    /// The session is challenged; may be called repeatedly.
    ///
    /// # Errors
    ///
    /// As [`Authenticator::on_connected_session`].
    fn on_challenged_session(
        &mut self,
        session: &mut dyn SessionProxy,
        now_ms: i64,
    ) -> Result<(), crate::ClusterError>;
}

/// The reference's `AuthorisationService` (`AuthorisationService.java:48-58`).
pub trait AuthorisationService {
    /// Whether a session with `encoded_principal` may perform the action.
    ///
    /// `action_type` carries the request type where the protocol has one — for
    /// an admin request it is the `AdminRequestType` value
    /// (`ConsensusModuleAgent.java:783-788`).
    fn is_authorised(
        &self,
        protocol_id: i32,
        action_id: i32,
        action_type: Option<i32>,
        encoded_principal: &[u8],
    ) -> bool;
}

/// `DefaultAuthenticatorSupplier.DefaultAuthenticator`
/// (`DefaultAuthenticatorSupplier.java:58-77`): everybody authenticates, with
/// an empty principal.
///
/// The challenge hook is a no-op, and both "connected" and "challenged" are
/// answered by authenticating — which is why a cluster with no authenticator
/// configured never sends a `Challenge`.
#[derive(Debug, Default)]
pub struct DefaultAuthenticator;

impl Authenticator for DefaultAuthenticator {
    fn on_connect_request(&mut self, _session_id: i64, _encoded_credentials: &[u8], _now_ms: i64) {}

    fn on_challenge_response(
        &mut self,
        _session_id: i64,
        _encoded_credentials: &[u8],
        _now_ms: i64,
    ) {
    }

    fn on_connected_session(
        &mut self,
        session: &mut dyn SessionProxy,
        _now_ms: i64,
    ) -> Result<(), crate::ClusterError> {
        session.authenticate(&[])?;
        Ok(())
    }

    fn on_challenged_session(
        &mut self,
        session: &mut dyn SessionProxy,
        _now_ms: i64,
    ) -> Result<(), crate::ClusterError> {
        session.authenticate(&[])?;
        Ok(())
    }
}

/// `AllowBackupAndStandbyAuthorisationService`
/// (`AllowBackupAndStandbyAuthorisationService.java:28-48`), the cluster's
/// default authorisation service: only the three backup/standby queries are
/// authorised, and everything else is denied.
///
/// It is expressed as a value rather than an enum with one variant because the
/// two reference tokens are also values ([`ALLOW_ALL_NAME`] / [`DENY_ALL_NAME`]).
#[derive(Debug, Default)]
pub struct AllowBackupAndStandbyAuthorisationService;

impl AuthorisationService for AllowBackupAndStandbyAuthorisationService {
    fn is_authorised(
        &self,
        protocol_id: i32,
        action_id: i32,
        _action_type: Option<i32>,
        _encoded_principal: &[u8],
    ) -> bool {
        protocol_id == i32::from(SBE_SCHEMA_ID)
            && (action_id == i32::from(BACKUP_QUERY_TEMPLATE_ID)
                || action_id == i32::from(HEARTBEAT_REQUEST_TEMPLATE_ID)
                || action_id == i32::from(STANDBY_SNAPSHOT_TEMPLATE_ID))
    }
}

/// `AuthorisationService.ALLOW_ALL` (`AuthorisationService.java:29`).
#[derive(Debug, Default)]
pub struct AllowAll;

impl AuthorisationService for AllowAll {
    fn is_authorised(&self, _: i32, _: i32, _: Option<i32>, _: &[u8]) -> bool {
        true
    }
}

/// `AuthorisationService.DENY_ALL` (`AuthorisationService.java:34`).
#[derive(Debug, Default)]
pub struct DenyAll;

impl AuthorisationService for DenyAll {
    fn is_authorised(&self, _: i32, _: i32, _: Option<i32>, _: &[u8]) -> bool {
        false
    }
}

/// A supplier this build cannot build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthConfigError {
    /// The authenticator supplier name is not one this build knows.
    UnknownAuthenticatorSupplier(String),
    /// The authorisation service supplier name is not one this build knows.
    UnknownAuthorisationServiceSupplier(String),
}

impl std::fmt::Display for AuthConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownAuthenticatorSupplier(name) => write!(
                f,
                "aeron.cluster.authenticator.supplier={name} is not a supplier this build can \
                 make; it knows {DEFAULT_AUTHENTICATOR_SUPPLIER}"
            ),
            Self::UnknownAuthorisationServiceSupplier(name) => write!(
                f,
                "aeron.cluster.authorisation.service.supplier={name} is not a supplier this \
                 build can make; it knows {ALLOW_ALL_NAME} and {DENY_ALL_NAME}, and an empty \
                 value means the backup/standby-only default"
            ),
        }
    }
}

impl std::error::Error for AuthConfigError {}

/// Build the authenticator a name asks for (`ConsensusModule.java:1312-1332`).
///
/// # Errors
///
/// [`AuthConfigError::UnknownAuthenticatorSupplier`] for any name but the
/// reference's default.
pub fn authenticator_for(name: &str) -> Result<Box<dyn Authenticator>, AuthConfigError> {
    if name == DEFAULT_AUTHENTICATOR_SUPPLIER {
        Ok(Box::new(DefaultAuthenticator))
    } else {
        Err(AuthConfigError::UnknownAuthenticatorSupplier(
            name.to_owned(),
        ))
    }
}

/// Build the authorisation service a name asks for
/// (`ConsensusModule.java:1341-1370`): the two tokens the reference
/// special-cases, and the default for an unset name.
///
/// # Errors
///
/// [`AuthConfigError::UnknownAuthorisationServiceSupplier`] for any class name.
pub fn authorisation_service_for(
    name: Option<&str>,
) -> Result<Box<dyn AuthorisationService>, AuthConfigError> {
    match name {
        None => Ok(Box::new(AllowBackupAndStandbyAuthorisationService)),
        Some(DENY_ALL_NAME) => Ok(Box::new(DenyAll)),
        Some(ALLOW_ALL_NAME) => Ok(Box::new(AllowAll)),
        Some(other) => Err(AuthConfigError::UnknownAuthorisationServiceSupplier(
            other.to_owned(),
        )),
    }
}

/// `ClusterSession.checkEncodedPrincipalLength` (`ClusterSession.java:537-545`).
///
/// # Errors
///
/// The reference's `ClusterException` words when the principal is too long.
pub fn check_encoded_principal_length(length: usize) -> Result<(), crate::ClusterError> {
    if length > MAX_ENCODED_PRINCIPAL_LENGTH {
        return Err(crate::ClusterError::Cluster(format!(
            "encoded principal max length {MAX_ENCODED_PRINCIPAL_LENGTH} exceeded: length={length}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_name_builds_the_default_authenticator() {
        assert!(authenticator_for(DEFAULT_AUTHENTICATOR_SUPPLIER).is_ok());
    }

    #[test]
    fn an_unknown_authenticator_name_is_refused_by_name() {
        let error = match authenticator_for("com.example.Mine") {
            Err(error) => error,
            Ok(_) => panic!("an unknown name built an authenticator"),
        };
        assert_eq!(
            AuthConfigError::UnknownAuthenticatorSupplier("com.example.Mine".to_owned()),
            error
        );
        assert!(error.to_string().contains("com.example.Mine"));
    }

    #[test]
    fn the_two_authorisation_tokens_are_values_and_a_class_name_is_refused() {
        assert!(authorisation_service_for(None).is_ok());
        assert!(authorisation_service_for(Some(DENY_ALL_NAME)).is_ok());
        assert!(authorisation_service_for(Some(ALLOW_ALL_NAME)).is_ok());
        let error = match authorisation_service_for(Some("x.Y")) {
            Err(error) => error,
            Ok(_) => panic!("an unknown name built an authorisation service"),
        };
        assert_eq!(
            AuthConfigError::UnknownAuthorisationServiceSupplier("x.Y".to_owned()),
            error
        );
    }

    #[test]
    fn the_default_authorisation_service_is_the_backup_and_standby_one() {
        let service = AllowBackupAndStandbyAuthorisationService;
        let schema = i32::from(SBE_SCHEMA_ID);
        assert!(service.is_authorised(schema, i32::from(BACKUP_QUERY_TEMPLATE_ID), None, &[]));
        assert!(service.is_authorised(schema, i32::from(HEARTBEAT_REQUEST_TEMPLATE_ID), None, &[]));
        assert!(service.is_authorised(schema, i32::from(STANDBY_SNAPSHOT_TEMPLATE_ID), None, &[]));
        // `AdminRequest` is 26 and is not in the set.
        assert!(!service.is_authorised(schema, 26, Some(0), &[]));
        assert!(!service.is_authorised(schema + 1, i32::from(BACKUP_QUERY_TEMPLATE_ID), None, &[]));
    }
}
