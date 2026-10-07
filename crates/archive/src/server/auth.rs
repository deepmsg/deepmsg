//! Who may connect, and what they may then do.
//!
//! Two questions, two hooks, and both are named by a deployment rather than
//! fixed: `aeron.archive.authenticator.supplier` names the class that decides
//! *whether a client gets a session at all*, and
//! `aeron.archive.authorisation.service.supplier` names the one that decides
//! *what a session that got one may do*. The reference loads both by
//! reflection (`Archive.java:956-1008`) — `Class.forName(...).getConstructor()
//! .newInstance()`, with any failure rethrown unchecked, which ends the archive
//! before it serves anything.
//!
//! **This is the same thing said without reflection**, and it is where the
//! first real decision of the slice is: the names are a closed set. There is no
//! class loading here, so a name this module does not know is a name the
//! archive **cannot** honour — and refusing to start is the only honest answer.
//! The alternative, quietly using the default authenticator for a name nobody
//! recognised, is the failure this build is built to avoid: an archive that
//! comes up and then fails every handshake for a reason no log explains.
//!
//! # The two names that matter, and why the sample one is here
//!
//! The reference's default is `io.aeron.security.DefaultAuthenticatorSupplier`
//! (`Archive.java:597`), which authenticates whoever asks. The archive test
//! harness does not use it: both harnesses pass
//! `-Daeron.archive.authenticator.supplier=io.aeron.samples.archive.SampleAuthenticatorSupplier`
//! (`aeron-archive/src/test/c/TestArchive.h:50`,
//! `aeron-archive/src/test/cpp_wrapper/TestArchive.h:76`), so **every** case in
//! the acceptance suite — the plain connects included — goes through
//! [`SampleAuthenticator`]'s credentials, challenge and rejection.
//!
//! # One authenticator, every session
//!
//! The reference's conductor builds one and hands it to every session it makes
//! (`ArchiveConductor.java:514`), which is why [`SampleAuthenticator`]
//! keeps its state keyed by session id and why
//! [`Authenticator`](crate::server::control_session::Authenticator) is lent to
//! a session for a call rather than owned by one.
//!
//! # The one thing this cannot express
//!
//! `SampleAuthenticator` drops a session's state **only if its answer went
//! out** — `if (sessionProxy.authenticate(...)) { sessionIdToStateMap.remove(
//! sessionId); }` (`SampleAuthenticator.java:125-129`, and the same at
//! `:158-163`). The reference's proxy reports the send's result back to the
//! caller; this build's [`Answer`] is a decision the session acts on, and there
//! is no road back. So the entry is left where it is.
//!
//! Nothing observable follows from that: the session stops asking once it has
//! authenticated, and every hook here is keyed by session id, so a stale entry
//! is never read as another session's. What is lost is the map's tidiness.

use std::collections::HashMap;

use crate::server::control_session::Answer;

/// `Archive.Configuration.AUTHENTICATOR_SUPPLIER_DEFAULT` (`Archive.java:597`).
pub const DEFAULT_AUTHENTICATOR_SUPPLIER: &str = "io.aeron.security.DefaultAuthenticatorSupplier";

/// The supplier both archive harnesses configure
/// (`TestArchive.h:50`, `cpp_wrapper/TestArchive.h:76`).
pub const SAMPLE_AUTHENTICATOR_SUPPLIER: &str =
    "io.aeron.samples.archive.SampleAuthenticatorSupplier";

/// `AuthorisationService.ALLOW_ALL_NAME` (`AuthorisationService.java:44`).
pub const ALLOW_ALL_NAME: &str = "ALLOW_ALL";

/// `AuthorisationService.DENY_ALL_NAME` (`AuthorisationService.java:39`).
pub const DENY_ALL_NAME: &str = "DENY_ALL";

/// A supplier name the archive cannot honour.
///
/// The reference's answer to an unknown name is `ClassNotFoundException` out of
/// `Class.forName`, rethrown unchecked by `LangUtil.rethrowUnchecked`
/// (`Archive.java:966-969`) — the archive never starts. This is the same
/// refusal as a value, so that the binary that meets it can say which name it
/// was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// An authenticator supplier that is not one of the two this build knows.
    UnknownAuthenticatorSupplier {
        /// The supplier name, as the deployment spelled it.
        name: String,
    },
    /// An authorisation service supplier this build does not know. The two
    /// tokens the reference special-cases are known; a class name is not.
    UnknownAuthorisationServiceSupplier {
        /// The supplier name, as the deployment spelled it.
        name: String,
    },
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownAuthenticatorSupplier { name } => write!(
                f,
                "aeron.archive.authenticator.supplier={name} is not a supplier this archive can \
                 build; it knows {DEFAULT_AUTHENTICATOR_SUPPLIER} and {SAMPLE_AUTHENTICATOR_SUPPLIER}"
            ),
            Self::UnknownAuthorisationServiceSupplier { name } => write!(
                f,
                "aeron.archive.authorisation.service.supplier={name} is not a supplier this \
                 archive can build; it knows {ALLOW_ALL_NAME} and {DENY_ALL_NAME}"
            ),
        }
    }
}

impl std::error::Error for AuthError {}

/// The archive's authenticator, as far as a session sees it
/// (`Authenticator.java:26-69`).
///
/// **One of these serves every session.** The reference's conductor keeps a
/// single instance and hands it to each `ControlSession` it makes
/// (`ArchiveConductor.java:514`), and its hooks are all keyed by session id
/// because of it — so this is lent to a session for a call rather than owned by
/// one, the same way the publications an egress writes through are.
pub trait Authenticator {
    /// A connect request arrived (`Authenticator.java:36`).
    fn on_connect_request(&mut self, session_id: i64, encoded_credentials: &[u8], now_ms: i64);

    /// The publication is connected: offer the session
    /// (`ControlSession.java:945`).
    fn on_connected_session(&mut self, session_id: i64, now_ms: i64) -> Answer;

    /// Still challenged; the reference calls this every turn
    /// (`ControlSession.java:954-958`).
    fn on_challenged_session(&mut self, session_id: i64, now_ms: i64) -> Answer;

    /// The client answered the challenge (`ControlSession.java:313`).
    fn on_challenge_response(
        &mut self,
        session_id: i64,
        encoded_credentials: &[u8],
        now_ms: i64,
    ) -> Answer;
}

/// The reference's `AuthorisationService` (`AuthorisationService.java:24-59`):
/// asked whether an authenticated session may perform an action.
///
/// The reference's signature carries a fourth parameter — an optional action
/// *type*, which the cluster's admin requests use to say which kind of request
/// they are (`AdminRequestType`). The archive passes `null` everywhere it calls
/// this, and this slice has no cluster in it, so the parameter is left out
/// rather than carried as an argument that is always `None`.
pub trait AuthorisationService {
    /// `isAuthorised(protocolId, actionId, type, encodedPrincipal)`, with the
    /// protocol id a schema id and the action id a template id
    /// (`ControlSessionAdapter.java:1207`).
    fn is_authorised(
        &self,
        protocol_id: i32,
        action_id: i32,
        encoded_principal: Option<&[u8]>,
    ) -> bool;
}

/// `AuthorisationService.ALLOW_ALL` (`AuthorisationService.java:29`), which is
/// what the archive's default supplier builds
/// (`Archive.java:610-611`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AllowAll;

impl AuthorisationService for AllowAll {
    fn is_authorised(&self, _: i32, _: i32, _: Option<&[u8]>) -> bool {
        true
    }
}

/// `AuthorisationService.DENY_ALL` (`AuthorisationService.java:34`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DenyAll;

impl AuthorisationService for DenyAll {
    fn is_authorised(&self, _: i32, _: i32, _: Option<&[u8]>) -> bool {
        false
    }
}

/// `AuthorisationService` as a boxed trait object, so that what a deployment
/// named can be resolved before it is known which one it is.
impl AuthorisationService for Box<dyn AuthorisationService> {
    fn is_authorised(
        &self,
        protocol_id: i32,
        action_id: i32,
        encoded_principal: Option<&[u8]>,
    ) -> bool {
        (**self).is_authorised(protocol_id, action_id, encoded_principal)
    }
}

/// The authenticator a deployment named.
///
/// An empty name is the default, which is what the reference's
/// `System.getProperty(name, AUTHENTICATOR_SUPPLIER_DEFAULT)` gives when the
/// property is absent (`Archive.java:958-959`).
///
/// # Errors
///
/// [`AuthError::UnknownAuthenticatorSupplier`] for a name this build cannot
/// build — see the module note for why that is a refusal and not a fallback.
pub fn authenticator(supplier: &str) -> Result<Box<dyn Authenticator>, AuthError> {
    match supplier {
        "" | DEFAULT_AUTHENTICATOR_SUPPLIER => Ok(Box::new(DefaultAuthenticator)),
        SAMPLE_AUTHENTICATOR_SUPPLIER => Ok(Box::new(SampleAuthenticator::new())),
        name => Err(AuthError::UnknownAuthenticatorSupplier {
            name: name.to_owned(),
        }),
    }
}

/// The authorisation service a deployment named.
///
/// The reference's property has **no default class**: absent or empty means its
/// own `DEFAULT_AUTHORISATION_SERVICE_SUPPLIER`, a supplier that returns
/// [`AllowAll`] (`Archive.java:602-611`). A name that is not one of the two
/// tokens is loaded by reflection there and refused here.
///
/// # Errors
///
/// [`AuthError::UnknownAuthorisationServiceSupplier`] for a name this build
/// cannot build.
pub fn authorisation_service(supplier: &str) -> Result<Box<dyn AuthorisationService>, AuthError> {
    match supplier {
        "" => Ok(Box::new(AllowAll)),
        ALLOW_ALL_NAME => Ok(Box::new(AllowAll)),
        DENY_ALL_NAME => Ok(Box::new(DenyAll)),
        name => Err(AuthError::UnknownAuthorisationServiceSupplier {
            name: name.to_owned(),
        }),
    }
}

/// `DefaultAuthenticatorSupplier`'s authenticator
/// (`DefaultAuthenticatorSupplier.java:58-77`): every client is let in, and
/// what it vouches for is the **empty** principal rather than nothing
/// (`:33`, `NULL_ENCODED_PRINCIPAL`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DefaultAuthenticator;

impl Authenticator for DefaultAuthenticator {
    fn on_connect_request(&mut self, _session_id: i64, _credentials: &[u8], _now_ms: i64) {}

    fn on_connected_session(&mut self, _session_id: i64, _now_ms: i64) -> Answer {
        Answer::Authenticate(Vec::new())
    }

    fn on_challenged_session(&mut self, _session_id: i64, _now_ms: i64) -> Answer {
        Answer::Authenticate(Vec::new())
    }

    fn on_challenge_response(
        &mut self,
        _session_id: i64,
        _credentials: &[u8],
        _now_ms: i64,
    ) -> Answer {
        Answer::None
    }
}

/// What `SampleAuthenticator` decided about one session
/// (`SampleAuthenticator.java:41-44`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SampleState {
    /// The client offered the credentials that earn a challenge.
    Challenge,
    /// The client offered credentials good enough to let in.
    Authenticated,
    /// The client offered neither.
    Reject,
}

/// `"admin:admin"` — let in with no challenge
/// (`SampleAuthenticator.java:30`).
pub const NO_CHALLENGE_CREDENTIALS: &[u8] = b"admin:admin";
/// `"admin:adminC"` — challenged before being let in (`:31`).
pub const CHALLENGE_CREDENTIALS: &[u8] = b"admin:adminC";
/// `"admin:CSadmin"` — what the challenge must be answered with (`:32`).
pub const CHALLENGE_ANSWER: &[u8] = b"admin:CSadmin";
/// `"challenge!"` — what the challenge says (`:33`).
pub const CHALLENGE: &[u8] = b"challenge!";
/// `"admin"` — what an authenticated session is vouched for (`:38`).
pub const PRINCIPAL: &[u8] = b"admin";

/// The sample archive's authenticator
/// (`aeron-samples/src/main/java/io/aeron/samples/archive/SampleAuthenticator.java`),
/// which is the one both archive test harnesses configure.
///
/// Three credentials and a challenge, with the session's place in the exchange
/// kept per session id (`:46`).
#[derive(Debug, Default)]
pub struct SampleAuthenticator {
    states: HashMap<i64, SampleState>,
}

impl SampleAuthenticator {
    /// An authenticator that has met nobody.
    pub fn new() -> Self {
        Self::default()
    }

    /// The credentials, read as the US-ASCII the reference reads them as
    /// (`:68`).
    ///
    /// A `String` built from non-ASCII bytes cannot equal any of the three
    /// credentials below, and neither can this: `None` is "no match", which is
    /// what the reference's replacement characters come to.
    fn credentials(bytes: &[u8]) -> Option<&str> {
        std::str::from_utf8(bytes).ok()
    }

    /// How many sessions this authenticator still has a place for.
    pub fn sessions(&self) -> usize {
        self.states.len()
    }
}

impl Authenticator for SampleAuthenticator {
    fn on_connect_request(&mut self, session_id: i64, credentials: &[u8], _now_ms: i64) {
        let state = match Self::credentials(credentials) {
            Some(credentials) if credentials.as_bytes() == NO_CHALLENGE_CREDENTIALS => {
                SampleState::Authenticated
            }
            Some(credentials) if credentials.as_bytes() == CHALLENGE_CREDENTIALS => {
                SampleState::Challenge
            }
            _ => SampleState::Reject,
        };

        self.states.insert(session_id, state);
    }

    fn on_connected_session(&mut self, session_id: i64, _now_ms: i64) -> Answer {
        let Some(state) = self.states.get(&session_id).copied() else {
            return Answer::None;
        };

        match state {
            SampleState::Challenge => Answer::Challenge(CHALLENGE.to_vec()),
            SampleState::Authenticated => {
                self.states.remove(&session_id);
                Answer::Authenticate(PRINCIPAL.to_vec())
            }
            SampleState::Reject => {
                self.states.remove(&session_id);
                Answer::Reject
            }
        }
    }

    /// The repeat while the challenge is out, and the one place the sample
    /// differs from itself: an authenticated session is vouched for with the
    /// **empty** principal here, not with `"admin"`
    /// (`SampleAuthenticator.java:158-163`).
    fn on_challenged_session(&mut self, session_id: i64, _now_ms: i64) -> Answer {
        let Some(state) = self.states.get(&session_id).copied() else {
            return Answer::None;
        };

        match state {
            SampleState::Challenge => Answer::None,
            SampleState::Authenticated => {
                self.states.remove(&session_id);
                Answer::Authenticate(Vec::new())
            }
            SampleState::Reject => {
                self.states.remove(&session_id);
                Answer::Reject
            }
        }
    }

    fn on_challenge_response(
        &mut self,
        session_id: i64,
        credentials: &[u8],
        _now_ms: i64,
    ) -> Answer {
        let answered = Self::credentials(credentials)
            .is_some_and(|credentials| credentials.as_bytes() == CHALLENGE_ANSWER);
        let state = self.states.get(&session_id).copied();

        if state == Some(SampleState::Challenge) && answered {
            self.states.insert(session_id, SampleState::Authenticated);
        } else if !answered {
            self.states.insert(session_id, SampleState::Reject);
        }

        Answer::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A session id, which is the key everything here is kept by.
    const SESSION: i64 = 7;

    #[test]
    fn the_default_supplier_lets_anyone_in_with_an_empty_principal() {
        let mut authenticator =
            authenticator(DEFAULT_AUTHENTICATOR_SUPPLIER).expect("a known supplier");

        authenticator.on_connect_request(SESSION, b"anything at all", 0);
        assert_eq!(
            Answer::Authenticate(Vec::new()),
            authenticator.on_connected_session(SESSION, 0)
        );
    }

    /// An absent property is the default, which is what the reference's
    /// `System.getProperty(name, default)` reads (`Archive.java:958-959`).
    #[test]
    fn an_absent_name_is_the_default_authenticator() {
        let mut authenticator = authenticator("").expect("the default");

        assert_eq!(
            Answer::Authenticate(Vec::new()),
            authenticator.on_connected_session(SESSION, 0)
        );
    }

    /// The refusal, which is the whole point of resolving names here: a name
    /// nobody knows is not a name to guess at.
    #[test]
    fn a_supplier_this_build_cannot_build_is_refused_by_name() {
        let Err(error) = authenticator("com.example.MyAuthenticator") else {
            panic!("a name this build cannot build was built anyway");
        };

        assert_eq!(
            AuthError::UnknownAuthenticatorSupplier {
                name: "com.example.MyAuthenticator".to_owned(),
            },
            error
        );
        assert!(error.to_string().contains("com.example.MyAuthenticator"));
    }

    /// The credentials the sample knows, and the three places they lead
    /// (`SampleAuthenticator.java:66-82`).
    #[test]
    fn the_sample_puts_a_client_in_one_of_three_places() {
        for (credentials, expected) in [
            (
                NO_CHALLENGE_CREDENTIALS,
                Answer::Authenticate(PRINCIPAL.to_vec()),
            ),
            (CHALLENGE_CREDENTIALS, Answer::Challenge(CHALLENGE.to_vec())),
            (b"admin:wrong".as_slice(), Answer::Reject),
            (b"".as_slice(), Answer::Reject),
        ] {
            let mut authenticator = SampleAuthenticator::new();
            authenticator.on_connect_request(SESSION, credentials, 0);

            assert_eq!(
                expected,
                authenticator.on_connected_session(SESSION, 0),
                "credentials {:?}",
                String::from_utf8_lossy(credentials)
            );
        }
    }

    /// Bytes that are not US-ASCII match nothing, which is what the
    /// reference's replacement characters come to
    /// (`SampleAuthenticator.java:68`).
    #[test]
    fn credentials_that_are_not_ascii_are_rejected() {
        let mut authenticator = SampleAuthenticator::new();
        authenticator.on_connect_request(SESSION, &[0xff, 0xfe], 0);

        assert_eq!(
            Answer::Reject,
            authenticator.on_connected_session(SESSION, 0)
        );
    }

    /// The challenge is answered with the one string the sample accepts, and
    /// its answer is what puts the session where the next turn can see it
    /// (`:91-104`, `:146-170`).
    #[test]
    fn the_challenge_is_answered_and_then_authenticated() {
        let mut authenticator = SampleAuthenticator::new();
        authenticator.on_connect_request(SESSION, CHALLENGE_CREDENTIALS, 0);

        assert_eq!(
            Answer::Challenge(CHALLENGE.to_vec()),
            authenticator.on_connected_session(SESSION, 0)
        );

        // Being challenged is not being answered: the hook says nothing until
        // a challenge response has arrived (`:155-157`).
        assert_eq!(
            Answer::None,
            authenticator.on_challenged_session(SESSION, 0)
        );

        assert_eq!(
            Answer::None,
            authenticator.on_challenge_response(SESSION, CHALLENGE_ANSWER, 0)
        );

        // And once it has, the answer is the empty principal — the sample's
        // own quirk, not `"admin"` (`:158-163`).
        assert_eq!(
            Answer::Authenticate(Vec::new()),
            authenticator.on_challenged_session(SESSION, 0)
        );
    }

    /// A challenge answered with the wrong thing is a rejection
    /// (`:100-103`).
    #[test]
    fn a_challenge_answered_wrongly_is_rejected() {
        let mut authenticator = SampleAuthenticator::new();
        authenticator.on_connect_request(SESSION, CHALLENGE_CREDENTIALS, 0);
        authenticator.on_connected_session(SESSION, 0);

        authenticator.on_challenge_response(SESSION, b"admin:nope", 0);

        assert_eq!(
            Answer::Reject,
            authenticator.on_challenged_session(SESSION, 0)
        );
    }

    /// The right answer to a challenge that was never issued changes nothing:
    /// the first branch wants the session to be challenged
    /// (`SampleAuthenticator.java:96`).
    #[test]
    fn the_right_answer_to_no_challenge_changes_nothing() {
        let mut authenticator = SampleAuthenticator::new();
        authenticator.on_connect_request(SESSION, NO_CHALLENGE_CREDENTIALS, 0);

        authenticator.on_challenge_response(SESSION, CHALLENGE_ANSWER, 0);

        assert_eq!(
            Answer::Authenticate(PRINCIPAL.to_vec()),
            authenticator.on_connected_session(SESSION, 0),
            "the session is where its connect put it"
        );
    }

    /// Sessions are kept apart, which is what the map is for.
    #[test]
    fn one_sessions_credentials_are_not_anothers() {
        let mut authenticator = SampleAuthenticator::new();
        authenticator.on_connect_request(1, NO_CHALLENGE_CREDENTIALS, 0);
        authenticator.on_connect_request(2, CHALLENGE_CREDENTIALS, 0);
        authenticator.on_connect_request(3, b"nobody", 0);

        assert_eq!(
            Answer::Authenticate(PRINCIPAL.to_vec()),
            authenticator.on_connected_session(1, 0)
        );
        assert_eq!(
            Answer::Challenge(CHALLENGE.to_vec()),
            authenticator.on_connected_session(2, 0)
        );
        assert_eq!(Answer::Reject, authenticator.on_connected_session(3, 0));
    }

    /// A session the authenticator has never been told about is left alone —
    /// the reference's null check (`SampleAuthenticator.java:117`).
    #[test]
    fn an_unknown_session_is_told_nothing() {
        let mut authenticator = SampleAuthenticator::new();

        assert_eq!(Answer::None, authenticator.on_connected_session(99, 0));
        assert_eq!(Answer::None, authenticator.on_challenged_session(99, 0));
    }

    /// The authorisation service's own default is the empty name, and its two
    /// tokens are the reference's special cases
    /// (`Archive.java:985-997`).
    #[test]
    fn the_authorisation_service_names_resolve() {
        assert!(
            authorisation_service("")
                .expect("the default")
                .is_authorised(101, 58, None)
        );
        assert!(
            authorisation_service(ALLOW_ALL_NAME)
                .expect("a token")
                .is_authorised(101, 58, None)
        );
        assert!(
            !authorisation_service(DENY_ALL_NAME)
                .expect("a token")
                .is_authorised(101, 58, None)
        );
    }

    #[test]
    fn an_authorisation_service_this_build_cannot_build_is_refused_by_name() {
        let Err(error) = authorisation_service("com.example.MyAuthorisation") else {
            panic!("a name this build cannot build was built anyway");
        };

        assert_eq!(
            AuthError::UnknownAuthorisationServiceSupplier {
                name: "com.example.MyAuthorisation".to_owned(),
            },
            error
        );
        assert!(error.to_string().contains("com.example.MyAuthorisation"));
    }
}
