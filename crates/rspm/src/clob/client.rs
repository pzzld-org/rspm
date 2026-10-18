/*
    Appellation: client <module>
    Created At: 2026.07.12:08:21:39
    Contrib: @FL03
*/
use super::{
    ClobConfig, ClobOperationError, FeeMetadataClient, PlatformFeeSchedule,
    PositionInventoryClient, PositionInventoryEntry, PostOrderOutcome, SubmissionAttemptAuthority,
    SubmissionController, SubmissionControllerBinding, classify_post_order_response,
    clob_price_decimal, clob_share_decimal, controller_pair, poll_transport_after_begin,
};
#[cfg(feature = "ws")]
use crate::auth::ws::{AuthenticatedUserWs, AuthenticatedUserWsError};
use crate::auth::{
    AuthenticatedBalanceClient, AuthenticatedBalanceSnapshot, AuthenticatedCredentialIdentity,
    AuthenticatedEndpoint, AuthenticatedEndpointError, AuthenticatedHttpClient, AuthenticatedOrder,
    AuthenticatedOrderPage, AuthenticatedOrdersClient, AuthenticatedOrdersRequest,
    AuthenticatedProtocolAuthority, AuthenticatedProtocolCacheIdentity, AuthenticatedTradePage,
    AuthenticatedTradesClient, AuthenticatedTradesRequest, venue_identifier_is_valid,
};
use crate::clob::operations::CancelOrdersOutcome;
use crate::types::{ClobSide, PrivateKeySigner, ProtocolVersion};
use crate::utils::parse_token_id;
use anyhow::Result;
use polymarket::clob::types::{request::MidpointRequest, response::OrderBookSummaryResponse};
use polymarket::{
    POLYGON,
    auth::{Credentials, LocalSigner, Normal, Signer, state::Authenticated},
    clob::{
        Client, Config,
        types::{
            AssetType, OrderType, Side as SdkSide, SignableOrder, SignatureType, SignedOrder,
            request::{BalanceAllowanceRequest, OrderBookSummaryRequest},
            response::BalanceAllowanceResponse,
        },
    },
    types::{Address, Decimal},
};
use rust_decimal::prelude::ToPrimitive;
use std::sync::Mutex as SyncMutex;
use tokio::sync::{Mutex as AsyncMutex, RwLock, RwLockReadGuard};

const PROTOCOL_AUTHORITY_REVOKED: &str = "authenticated protocol authority is revoked";
const PROTOCOL_AUTHORITY_UNAVAILABLE: &str = "authenticated protocol authority is unavailable";
const PROTOCOL_AUTHORITY_CHANGED: &str = "authenticated protocol authority changed";
const ORDER_VERSION_MISMATCH: &str = "order_version_mismatch";
const BALANCE_ALLOWANCE_AUTHORITY: &str = "balance_allowance_authority";

/// A cheaply-cloneable handle to the CLOB client.
pub type SharedClob = alloc::sync::Arc<ClobClient>;

pub(super) struct AuthenticatedClientState {
    pub(super) client: Client<Authenticated<Normal>>,
    pub(super) protocol_authority: Option<AuthenticatedProtocolAuthority>,
}

impl core::ops::Deref for AuthenticatedClientState {
    type Target = Client<Authenticated<Normal>>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[derive(Clone, Copy)]
struct ProtocolSubmissionPermit {
    authority: AuthenticatedProtocolAuthority,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum ProtocolSubmissionState {
    #[default]
    Closed,
    Active(AuthenticatedProtocolAuthority),
    Indeterminate(AuthenticatedProtocolAuthority),
}

#[derive(Default)]
struct ProtocolSubmissionGate {
    state: ProtocolSubmissionState,
    quarantined_through_generation: u64,
}

impl ProtocolSubmissionGate {
    fn close(&mut self) {
        match self.state {
            ProtocolSubmissionState::Active(authority) => {
                self.quarantined_through_generation = self
                    .quarantined_through_generation
                    .max(authority.generation());
                self.state = ProtocolSubmissionState::Closed;
            }
            // A generic outer revocation may race with post-send failure
            // handling. Never erase the stronger indeterminate state: only a
            // newer exact recovery generation may replace it.
            ProtocolSubmissionState::Indeterminate(_) | ProtocolSubmissionState::Closed => {}
        }
    }

    fn quarantine(&mut self, authority: AuthenticatedProtocolAuthority) {
        self.quarantined_through_generation = self
            .quarantined_through_generation
            .max(authority.generation());
        if !matches!(self.state, ProtocolSubmissionState::Indeterminate(_)) {
            self.state = ProtocolSubmissionState::Closed;
        }
    }

    fn mark_indeterminate(&mut self, authority: AuthenticatedProtocolAuthority) {
        self.quarantined_through_generation = self
            .quarantined_through_generation
            .max(authority.generation());
        self.state = ProtocolSubmissionState::Indeterminate(authority);
    }

    fn can_activate(&self, authority: AuthenticatedProtocolAuthority) -> bool {
        authority.generation() > self.quarantined_through_generation
            && !matches!(self.state, ProtocolSubmissionState::Active(_))
    }

    fn activate(&mut self, authority: AuthenticatedProtocolAuthority) {
        self.state = ProtocolSubmissionState::Active(authority);
    }

    fn permit(&self) -> Option<ProtocolSubmissionPermit> {
        match self.state {
            ProtocolSubmissionState::Active(authority) => {
                Some(ProtocolSubmissionPermit { authority })
            }
            ProtocolSubmissionState::Closed | ProtocolSubmissionState::Indeterminate(_) => None,
        }
    }

    fn matches(&self, permit: ProtocolSubmissionPermit) -> bool {
        self.state == ProtocolSubmissionState::Active(permit.authority)
            && permit.authority.generation() > self.quarantined_through_generation
    }

    #[cfg(test)]
    fn is_indeterminate(&self) -> bool {
        matches!(self.state, ProtocolSubmissionState::Indeterminate(_))
    }
}

/// Authenticated CLOB client. Wraps the SDK client + signer together so
/// callers don't need to manage them separately.
///
/// The SDK client and its exact protocol authority live under one lock. A
/// submission holds one read guard across build, sign, and POST; closed
/// recovery is the sole writer and replaces the pair atomically. Submission
/// attempts are serialized so one ambiguous provider rejection can close the
/// local authority before any queued attempt reaches POST.
pub struct ClobClient {
    pub(super) client: RwLock<AuthenticatedClientState>,
    authority_refresh: AsyncMutex<()>,
    submission_serial: AsyncMutex<()>,
    submission_authority: SyncMutex<ProtocolSubmissionGate>,
    submission_controller_binding: SubmissionControllerBinding,
    pub(crate) signer: PrivateKeySigner,
    authenticated_http: AuthenticatedHttpClient,
    authenticated_balance: AuthenticatedBalanceClient,
    authenticated_orders: AuthenticatedOrdersClient,
    authenticated_trades: AuthenticatedTradesClient,
    fee_metadata: FeeMetadataClient,
    positions: PositionInventoryClient,
    /// Exact venue account: signer address for EOA auth, explicit funder for
    /// Proxy auth. This single identity drives SDK rebuilds, exact balances,
    /// and public position inventory.
    account_address: Address,
    signature_type: SignatureType,
}

const MAX_CLIENT_MARKER_BYTES: usize = 128;

/// A signed CLOB order prepared without sending an order POST.
#[derive(Debug)]
pub struct PreparedClobOrder {
    signed: SignedOrder,
    client_marker: String,
    provider_order_id: String,
}

impl PreparedClobOrder {
    /// Return the durable internal correlation marker.
    #[must_use]
    pub fn client_marker(&self) -> &str {
        &self.client_marker
    }

    /// Return the provider-native EIP-712 order identifier.
    #[must_use]
    pub fn provider_order_id(&self) -> &str {
        &self.provider_order_id
    }
}

// ── Shared retry envelope ───────────────────────────────────────────────────

/// Drive one CLOB order-submission attempt through the bounded retry/backoff
/// envelope shared by the three guarded submission methods.
///
/// `authorize_attempt` grants one opaque permit immediately before each
/// `attempt_fn` call. The permit is owned by that single build + sign + post
/// future and is dropped before any retry backoff. Returning `None` closes the
/// retry envelope before another request can be built, signed, or posted.
/// `attempt_fn` is called again on each authorized retry; re-building and
/// re-signing per attempt avoids relying on `SignedOrder` being `Clone` (it is
/// not, in the vendored SDK).
///
/// On a rate-limited (429) failure, sleeps per [`crate::retry::RetryPolicy::backoff_for`]
/// and retries, up to `policy.max_attempts`. On any other failure, or once
/// retries are exhausted, returns the closed, redacted
/// [`crate::ClobOperationError`] class. SDK response bodies, request paths,
/// and order/token identifiers never cross this rspm-owned boundary.
#[cfg_attr(feature = "tracing", tracing::instrument(skip_all, target = "clob", fields(label = %label)))]
async fn retry_post_order<A, Authorization, R, F, Fut, T>(
    label: &str,
    policy: crate::retry::RetryPolicy,
    mut authorize_attempt: A,
    mut retry_authority_active: R,
    mut attempt_fn: F,
) -> core::result::Result<T, crate::ClobOperationError>
where
    A: FnMut() -> Authorization,
    Authorization: core::future::Future<Output = Option<SubmissionAttemptAuthority>>,
    R: FnMut() -> bool,
    F: FnMut(SubmissionAttemptAuthority) -> Fut,
    Fut: core::future::Future<Output = core::result::Result<T, crate::ClobOperationError>>,
{
    #[cfg(not(feature = "tracing"))]
    let _label = label;
    let mut attempt: u32 = 1;
    loop {
        let permit = authorize_attempt()
            .await
            .ok_or(crate::ClobOperationError::SubmissionRevoked)?;
        match attempt_fn(permit).await {
            Ok(resp) => return Ok(resp),
            Err(crate::ClobOperationError::RateLimited { retry_after }) => {
                if !retry_authority_active() {
                    // Authority was revoked while this attempt held a 429.
                    //
                    // The retry is a distinct network attempt and it will not
                    // be made -- not because the venue asked us to wait, but
                    // because we no longer hold the authority to submit at
                    // all. Reporting `RateLimited` collapses those two into
                    // one answer, and the adapter has no arm for it
                    // (`crates/engine/src/exec/polymarket.rs:3410`), so the
                    // caller receives `Sdk("CLOB operation was rate limited")`
                    // -- a string that reads as "back off and retry" for a
                    // submission that must never be retried.
                    //
                    // `SubmissionRevoked` is documented as exactly this case:
                    // "Live submission authority was absent or revoked before
                    // a network attempt began" (`crates/rspm/src/error.rs`).
                    // It maps to `PrivateLiveAdmissionClosed`, which is what a
                    // caller branches on to stop.
                    //
                    // Retries exhausted with authority still ACTIVE remains
                    // `RateLimited` below -- that one really is "wait".
                    return Err(crate::ClobOperationError::SubmissionRevoked);
                }
                if policy.should_retry(attempt) {
                    let delay = policy.backoff_for(attempt, retry_after);
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "CLOB rate-limited (429) - retrying order submit"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                return Err(crate::ClobOperationError::RateLimited { retry_after });
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn classify_operation_error(
    error: &polymarket::error::Error,
) -> crate::ClobOperationError {
    use polymarket::error::{Kind, Status, Validation};

    if let crate::retry::RateLimitSignal::RateLimited { retry_after } =
        crate::retry::classify_clob_error(error)
    {
        return crate::ClobOperationError::RateLimited { retry_after };
    }
    if error
        .downcast_ref::<Validation>()
        .is_some_and(|validation| {
            matches!(
                validation.reason.as_str(),
                PROTOCOL_AUTHORITY_REVOKED
                    | PROTOCOL_AUTHORITY_UNAVAILABLE
                    | PROTOCOL_AUTHORITY_CHANGED
            )
        })
    {
        return crate::ClobOperationError::ProtocolAuthorityRevoked;
    }
    match error.kind() {
        Kind::Validation => crate::ClobOperationError::InvalidRequest,
        Kind::Status => {
            let Some(status) = error.downcast_ref::<Status>() else {
                return crate::ClobOperationError::Rejected;
            };
            if matches!(status.status_code.as_u16(), 401 | 403) {
                return crate::ClobOperationError::Authentication;
            }
            let message = status.message.to_ascii_lowercase();
            if status_revokes_protocol_authority(status) {
                return crate::ClobOperationError::ProtocolAuthorityRevoked;
            }
            if message == "no_match"
                || message.contains("no orders")
                || (message.contains("fak") && message.contains("match"))
            {
                return crate::ClobOperationError::NoMatch;
            }
            if status.status_code.is_server_error() {
                crate::ClobOperationError::Transport
            } else {
                crate::ClobOperationError::Rejected
            }
        }
        Kind::Geoblock => crate::ClobOperationError::Rejected,
        Kind::Synchronization | Kind::Internal | Kind::WebSocket => {
            crate::ClobOperationError::Transport
        }
        _ => crate::ClobOperationError::Transport,
    }
}

fn status_revokes_protocol_authority(status: &polymarket::error::Status) -> bool {
    matches!(
        status.message.as_str(),
        ORDER_VERSION_MISMATCH | BALANCE_ALLOWANCE_AUTHORITY
    )
}

/// Whether a submit error leaves venue acceptance unknowable.
///
/// A definite client rejection, authentication rejection, geoblock, or 429
/// response proves that the venue answered. Transport/internal failures and
/// every 5xx can occur after request bytes were sent, so they quarantine the
/// exact generation instead of entering the retry envelope.
fn post_response_is_ambiguous(error: &polymarket::error::Error) -> bool {
    use polymarket::error::{Kind, Status};

    match error.kind() {
        Kind::Status => error
            .downcast_ref::<Status>()
            .is_none_or(|status| status.status_code.is_server_error()),
        Kind::Internal | Kind::Synchronization | Kind::WebSocket => true,
        Kind::Validation | Kind::Geoblock => false,
        _ => true,
    }
}

#[cfg(any(feature = "tracing", test))]
fn protocol_version_retry_error_class<E>(_error: &E) -> &'static str {
    "clob_protocol_version_retry"
}

fn canonical_account_address(
    signer: Address,
    signature_type: SignatureType,
    funder: Option<Address>,
) -> crate::Result<Address> {
    match (signature_type, funder) {
        (SignatureType::Eoa, None) => Ok(signer),
        (SignatureType::Proxy, Some(funder)) if funder != Address::ZERO && funder != signer => {
            Ok(funder)
        }
        _ => Err(crate::Error::InvalidClobConfiguration),
    }
}

/// Build the SDK submission client from the same declared authority used by
/// owned balance, position, refresh, and recovery reads.
///
/// Credentials are reused only as L2 authentication material. The SDK order
/// authority itself is rebuilt here from the signer, signature type, and
/// canonical account address, so an opaque pre-authenticated client can never
/// smuggle a different funder or signature type into [`ClobClient`].
async fn authenticate_declared_authority(
    host: &str,
    credentials: Credentials,
    signer: &PrivateKeySigner,
    account_address: Address,
    signature_type: SignatureType,
) -> crate::Result<Client<Authenticated<Normal>>> {
    let mut builder = Client::new(host, Config::default())?
        .authentication_builder(signer)
        .signature_type(signature_type)
        .credentials(credentials);
    if signature_type == SignatureType::Proxy {
        builder = builder.funder(account_address);
    }
    let client = builder.authenticate().await?;
    if client.address() != signer.address() {
        return Err(crate::Error::InvalidClobConfiguration);
    }
    Ok(client)
}

impl ClobClient {
    /// Construct the client from an explicit [`ClobConfig`] and private key.
    ///
    /// Returns `None` if the private key is invalid or authentication fails.
    ///
    /// **Proxy**: if `config.proxy_url` is `Some`, the caller must configure
    /// the process HTTP client before calling this (e.g. set `ALL_PROXY` in
    /// the binary entry point before the async runtime starts). This function
    /// never mutates environment variables.
    pub async fn new(config: ClobConfig, pk: impl AsRef<str>) -> crate::Result<Self> {
        use core::str::FromStr;
        config.validate_authenticated()?;
        let fee_metadata = FeeMetadataClient::new(config.clone())?;
        // initialize a local signer from the given private key
        let signer = LocalSigner::from_str(pk.as_ref())
            .map_err(|_| crate::Error::InvalidPrivateKey)?
            .with_chain_id(Some(POLYGON));
        // parse the funder
        let funder = match std::env::var("POLYMARKET_WALLET_ADDRESS") {
            Ok(raw) => Some(
                Address::from_str(raw.trim())
                    .ok()
                    .filter(|address| *address != Address::ZERO)
                    .ok_or(crate::Error::InvalidClobConfiguration)?,
            ),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(crate::Error::InvalidClobConfiguration);
            }
        };
        let signature_type = if funder.is_some() {
            SignatureType::Proxy
        } else {
            SignatureType::Eoa
        };
        let account_address = canonical_account_address(signer.address(), signature_type, funder)?;

        #[cfg(feature = "tracing")]
        tracing::info!(
            host_class = config.host_class(),
            proxy_configured = config.proxy_url.is_some(),
            account_kind = if funder.is_some() { "proxy" } else { "eoa" },
            "CLOB authentication configured"
        );

        let mut auth_builder = Client::new(&config.host, Config::default())?
            .authentication_builder(&signer)
            .signature_type(signature_type);

        if let Some(f) = funder {
            auth_builder = auth_builder.funder(f);
        }

        let client = auth_builder.authenticate().await?;
        let (submission_controller_binding, _submission_controller) = controller_pair()?;
        #[cfg(feature = "tracing")]
        tracing::info!(
            host_class = config.host_class(),
            account_kind = if funder.is_some() { "proxy" } else { "eoa" },
            "CLOB client authenticated"
        );
        Ok(Self {
            client: RwLock::new(AuthenticatedClientState {
                client,
                protocol_authority: None,
            }),
            authority_refresh: AsyncMutex::new(()),
            submission_serial: AsyncMutex::new(()),
            submission_authority: SyncMutex::new(ProtocolSubmissionGate::default()),
            submission_controller_binding,
            signer,
            authenticated_http: AuthenticatedHttpClient::try_new()?,
            authenticated_balance: AuthenticatedBalanceClient,
            authenticated_orders: AuthenticatedOrdersClient,
            authenticated_trades: AuthenticatedTradesClient,
            fee_metadata,
            positions: PositionInventoryClient::try_new()?,
            account_address,
            signature_type,
        })
    }
    /// Build from an explicitly named environment variable.
    ///
    /// This compatibility constructor defaults to the raw
    /// `POLYMARKET_PRIVATE_KEY` name. Runtime callers should instead load
    /// `AxiomSettings.clients.polymarket.private_key` once and pass it to
    /// [`Self::from_private_key`]. This function reads env vars but never
    /// mutates them.
    pub async fn from_env(key: Option<&str>) -> crate::Result<Self> {
        let key = key.unwrap_or("POLYMARKET_PRIVATE_KEY");
        let pk = std::env::var(key)?;
        let config = ClobConfig::default();
        Self::new(config, &pk).await
    }

    /// Rebuild one exact authority from explicit in-memory credentials.
    ///
    /// This is the only compatibility path for callers that authenticated an
    /// SDK client earlier in boot. Callers pass that client's host and cloned
    /// credentials through [`ClobConfig`] and [`Credentials`]; they never pass
    /// the opaque client itself. RSPM then authenticates a fresh SDK client
    /// with the same signer, funder, and signature type used by every owned
    /// account-authority read. Invalid EOA/proxy combinations fail before the
    /// SDK client is built.
    pub async fn from_authenticated_authority(
        config: ClobConfig,
        credentials: Credentials,
        signer: PrivateKeySigner,
        funder: Option<Address>,
        signature_type: SignatureType,
    ) -> crate::Result<Self> {
        Self::from_authenticated_authority_with_submission_controller(
            config,
            credentials,
            signer,
            funder,
            signature_type,
        )
        .await
        .map(|(client, _submission_controller)| client)
    }

    /// Rebuild one exact authority and return its separately-owned submission
    /// controller. Runtime composition must transfer the controller directly
    /// to engine private-recovery admission; the client cannot mint attempts.
    pub async fn from_authenticated_authority_with_submission_controller(
        config: ClobConfig,
        credentials: Credentials,
        signer: PrivateKeySigner,
        funder: Option<Address>,
        signature_type: SignatureType,
    ) -> crate::Result<(Self, SubmissionController)> {
        config.validate_authenticated()?;
        let account_address = canonical_account_address(signer.address(), signature_type, funder)?;
        let fee_metadata = FeeMetadataClient::new(config.clone())?;
        let client = authenticate_declared_authority(
            config.host(),
            credentials,
            &signer,
            account_address,
            signature_type,
        )
        .await?;
        let (submission_controller_binding, submission_controller) = controller_pair()?;
        let client = Self {
            client: RwLock::new(AuthenticatedClientState {
                client,
                protocol_authority: None,
            }),
            authority_refresh: AsyncMutex::new(()),
            submission_serial: AsyncMutex::new(()),
            submission_authority: SyncMutex::new(ProtocolSubmissionGate::default()),
            submission_controller_binding,
            signer,
            authenticated_http: AuthenticatedHttpClient::try_new()?,
            authenticated_balance: AuthenticatedBalanceClient,
            authenticated_orders: AuthenticatedOrdersClient,
            authenticated_trades: AuthenticatedTradesClient,
            fee_metadata,
            positions: PositionInventoryClient::try_new()?,
            account_address,
            signature_type,
        };
        Ok((client, submission_controller))
    }

    /// Exact account queried for balance/position authority.
    #[must_use]
    pub const fn account_address(&self) -> Address {
        self.account_address
    }

    /// Exact wallet-signature contract used by SDK auth and balance reads.
    #[must_use]
    pub const fn signature_type(&self) -> SignatureType {
        self.signature_type
    }

    /// Install a loopback-only Data API authority for deterministic downstream
    /// integration tests. Production builds cannot compile this seam.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub fn with_position_inventory_endpoint_for_test(mut self, endpoint: &str) -> Self {
        self.positions = PositionInventoryClient::for_test(endpoint);
        self
    }
    /// Build from an explicit private key string.
    ///
    /// Prefer this constructor in new call sites - supply the key from
    /// `axiom_config::AxiomSettings::load_or_default().clients.polymarket.private_key`
    /// rather than reading from the environment directly.
    pub async fn from_private_key(pk: &str) -> crate::Result<Self> {
        let config = ClobConfig::default();
        Self::new(config, pk).await
    }
    fn submission_gate(&self) -> std::sync::MutexGuard<'_, ProtocolSubmissionGate> {
        self.submission_authority
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Acquire the opaque current-generation permit and SDK authority used by
    /// one entire submission.
    async fn submission_client(
        &self,
    ) -> core::result::Result<
        (
            RwLockReadGuard<'_, AuthenticatedClientState>,
            ProtocolSubmissionPermit,
        ),
        crate::ClobOperationError,
    > {
        let permit = self
            .submission_gate()
            .permit()
            .ok_or(crate::ClobOperationError::ProtocolAuthorityRevoked)?;
        let guard = self.client.read().await;
        let authority = guard
            .protocol_authority
            .ok_or(crate::ClobOperationError::ProtocolAuthorityRevoked)?;
        let sdk_version = guard
            .version()
            .await
            .map_err(|error| classify_operation_error(&error))?;
        if authority != permit.authority
            || sdk_version != authority.version().as_u32()
            || !self.submission_gate().matches(permit)
        {
            self.submission_gate().quarantine(permit.authority);
            return Err(crate::ClobOperationError::ProtocolAuthorityRevoked);
        }
        #[cfg(feature = "tracing")]
        tracing::info!(
            protocol_version = sdk_version,
            protocol_generation = authority.generation(),
            "CLOB submission acquired exact protocol authority"
        );
        Ok((guard, permit))
    }

    async fn post_prepared_sdk(
        &self,
        guard: &AuthenticatedClientState,
        permit: ProtocolSubmissionPermit,
        attempt_authority: &SubmissionAttemptAuthority,
        signed: SignedOrder,
    ) -> core::result::Result<PostOrderOutcome, crate::ClobOperationError> {
        let authority = guard
            .protocol_authority
            .ok_or(crate::ClobOperationError::ProtocolAuthorityRevoked)?;
        let before_version = guard
            .version()
            .await
            .map_err(|error| classify_operation_error(&error))?;
        if authority != permit.authority
            || signed.payload.version() != before_version
            || before_version != authority.version().as_u32()
            || !self.submission_gate().matches(permit)
            || !attempt_authority.revalidate(self.submission_controller_binding, permit.authority)
        {
            self.submission_gate().quarantine(permit.authority);
            return Err(crate::ClobOperationError::ProtocolAuthorityRevoked);
        }
        let posted = poll_transport_after_begin(
            attempt_authority,
            self.submission_controller_binding,
            permit.authority,
            guard.post_order(signed),
        )
        .await
        .map_err(|()| {
            self.submission_gate().quarantine(permit.authority);
            crate::ClobOperationError::SubmissionRevoked
        })?;
        let response = match posted {
            Ok(response) => match classify_post_order_response(response) {
                Ok(outcome) => Ok(outcome),
                Err(()) => {
                    self.submission_gate().mark_indeterminate(permit.authority);
                    return Err(crate::ClobOperationError::PostSendIndeterminate);
                }
            },
            Err(error) if post_response_is_ambiguous(&error) => {
                self.submission_gate().mark_indeterminate(permit.authority);
                return Err(crate::ClobOperationError::PostSendIndeterminate);
            }
            Err(error) => Err(classify_operation_error(&error)),
        };
        let after_version_matches = guard
            .version()
            .await
            .is_ok_and(|after_version| after_version == before_version);
        let permit_still_matches = self.submission_gate().matches(permit);
        let response_revokes_authority = matches!(
            response,
            Err(crate::ClobOperationError::ProtocolAuthorityRevoked)
        );
        if response_revokes_authority || !after_version_matches || !permit_still_matches {
            self.submission_gate().quarantine(permit.authority);
        }
        response
    }

    /// Sign and POST with the same client guard that built the order.
    async fn sign_and_post_sdk(
        &self,
        guard: &AuthenticatedClientState,
        permit: ProtocolSubmissionPermit,
        attempt_authority: &SubmissionAttemptAuthority,
        signable: SignableOrder,
    ) -> core::result::Result<PostOrderOutcome, crate::ClobOperationError> {
        let authority = guard
            .protocol_authority
            .ok_or(crate::ClobOperationError::ProtocolAuthorityRevoked)?;
        let before_version = guard
            .version()
            .await
            .map_err(|error| classify_operation_error(&error))?;
        let order_version = signable.payload.version();
        if authority != permit.authority
            || order_version != before_version
            || before_version != authority.version().as_u32()
            || !self.submission_gate().matches(permit)
            || !attempt_authority.revalidate(self.submission_controller_binding, permit.authority)
        {
            self.submission_gate().quarantine(permit.authority);
            return Err(crate::ClobOperationError::ProtocolAuthorityRevoked);
        }
        let signed = guard
            .sign(&self.signer, signable)
            .await
            .map_err(|error| classify_operation_error(&error))?;
        if !self.submission_gate().matches(permit) {
            self.submission_gate().quarantine(permit.authority);
            return Err(crate::ClobOperationError::ProtocolAuthorityRevoked);
        }
        // `poll_transport_after_begin` holds the authority mutex through the
        // first poll of this credential-bearing future. If revocation wins,
        // `guard.post_order_initial` receives zero polls. If the poll wins,
        // the retained attempt read fence makes the revoker drain the initial
        // POST. Trade-resolution reads happen later through authenticated
        // reconciliation, outside this boundary.
        let posted = poll_transport_after_begin(
            attempt_authority,
            self.submission_controller_binding,
            permit.authority,
            guard.post_order(signed),
        )
        .await
        .map_err(|()| {
            self.submission_gate().quarantine(permit.authority);
            crate::ClobOperationError::SubmissionRevoked
        })?;
        let response = match posted {
            Ok(response) => match classify_post_order_response(response) {
                Ok(outcome) => Ok(outcome),
                Err(()) => {
                    self.submission_gate().mark_indeterminate(permit.authority);
                    return Err(crate::ClobOperationError::PostSendIndeterminate);
                }
            },
            Err(error) if post_response_is_ambiguous(&error) => {
                // Request bytes may have reached the venue. Publish the
                // stronger state while this submission still owns the serial
                // section so a queued caller cannot reach POST first.
                self.submission_gate().mark_indeterminate(permit.authority);
                return Err(crate::ClobOperationError::PostSendIndeterminate);
            }
            Err(error) => Err(classify_operation_error(&error)),
        };

        let after_version_matches = guard
            .version()
            .await
            .is_ok_and(|after_version| after_version == before_version);
        let permit_still_matches = self.submission_gate().matches(permit);
        let response_revokes_authority = matches!(
            response,
            Err(crate::ClobOperationError::ProtocolAuthorityRevoked)
        );
        if response_revokes_authority || !after_version_matches || !permit_still_matches {
            // A trusted POST response remains authoritative for this attempt.
            // Quarantine only future submission authority; do not erase an
            // accepted order ID or change a definite rejection into a generic
            // protocol error.
            self.submission_gate().quarantine(permit.authority);
        }
        response
    }

    /// Resolve Polymarket V2 platform-fee parameters for one outcome token.
    ///
    /// Returns the exact rspm-owned market metadata `fd.r`, `fd.e`, and
    /// `fd.to`. Missing or malformed policy is an error, never inferred.
    pub async fn platform_fee_params(
        &self,
        token_id: &str,
    ) -> core::result::Result<PlatformFeeSchedule, crate::clob::FeeMetadataError> {
        self.fee_metadata.platform_fee_schedule(token_id).await
    }

    /// Fetch and atomically decode one authenticated CLOB `/data/trades` page.
    ///
    /// rspm owns this recovery request, L2 HMAC headers, and response schema.
    /// The SDK remains responsible for order construction and signing.
    pub async fn authenticated_trades(
        &self,
        request: &AuthenticatedTradesRequest,
        next_cursor: Option<String>,
    ) -> core::result::Result<AuthenticatedTradePage, AuthenticatedEndpointError> {
        let client = self.client.read().await.client.clone();
        self.authenticated_trades
            .fetch(
                &self.authenticated_http,
                &client,
                request,
                next_cursor.as_deref(),
            )
            .await
    }

    /// Recover one authenticated order by its venue-assigned identifier.
    ///
    /// The response preserves exact original/matched sizes, status, routing
    /// dimensions, and side for durable open-order reconciliation.
    pub async fn authenticated_order(
        &self,
        order_id: &str,
    ) -> core::result::Result<AuthenticatedOrder, AuthenticatedEndpointError> {
        let client = self.client.read().await.client.clone();
        self.authenticated_orders
            .fetch_one(&self.authenticated_http, &client, order_id)
            .await
    }

    /// Fetch and atomically decode one authenticated account-order inventory
    /// page. Terminal and nonterminal cursor invariants are enforced by rspm.
    pub async fn authenticated_orders(
        &self,
        request: &AuthenticatedOrdersRequest,
        next_cursor: Option<String>,
    ) -> core::result::Result<AuthenticatedOrderPage, AuthenticatedEndpointError> {
        let client = self.client.read().await.client.clone();
        self.authenticated_orders
            .fetch(
                &self.authenticated_http,
                &client,
                request,
                next_cursor.as_deref(),
            )
            .await
    }

    /// Recover an authenticated collateral balance and allowance snapshot.
    ///
    /// The response remains exact. Failures retain only the typed endpoint
    /// class and never expose the authenticated response body.
    pub async fn authenticated_balance_allowance(
        &self,
    ) -> core::result::Result<AuthenticatedBalanceSnapshot, AuthenticatedEndpointError> {
        let guard = self.client.read().await;
        let authority = guard.protocol_authority.ok_or_else(|| {
            AuthenticatedEndpointError::request_failed(AuthenticatedEndpoint::ProtocolVersion)
        })?;
        self.authenticated_balance
            .fetch(
                &self.authenticated_http,
                &guard,
                self.account_address,
                self.signature_type,
                authority,
            )
            .await
    }

    /// Recover capital for one exact SDK protocol version and replacement
    /// generation, holding the client state stable for the entire read.
    pub async fn authenticated_balance_allowance_for(
        &self,
        expected: AuthenticatedProtocolAuthority,
    ) -> core::result::Result<AuthenticatedBalanceSnapshot, AuthenticatedEndpointError> {
        let guard = self.client.read().await;
        if guard.protocol_authority != Some(expected) {
            return Err(AuthenticatedEndpointError::request_failed(
                AuthenticatedEndpoint::ProtocolVersion,
            ));
        }
        self.authenticated_balance
            .fetch(
                &self.authenticated_http,
                &guard,
                self.account_address,
                self.signature_type,
                expected,
            )
            .await
    }

    /// Legacy node read projection backed by rspm's strict authenticated
    /// collateral parser. Conditional-token requests remain unsupported here;
    /// callers needing those must use a separately typed contract.
    pub async fn balance_allowance(
        &self,
        request: BalanceAllowanceRequest,
    ) -> Result<BalanceAllowanceResponse> {
        if request.asset_type != AssetType::Collateral
            || request.token_id.is_some()
            || request
                .signature_type
                .is_some_and(|signature_type| signature_type != self.signature_type)
        {
            return Err(anyhow::anyhow!(
                "rspm balance compatibility accepts only the configured collateral authority"
            ));
        }
        self.authenticated_balance_allowance()
            .await?
            .sdk_compatibility_response()
            .map_err(Into::into)
    }

    /// Recover the complete exact Data API position inventory for the
    /// exact EOA or explicitly configured proxy/funder wallet. No CLOB credential headers
    /// are sent to this public fixed-HTTPS endpoint.
    pub async fn position_inventory(
        &self,
    ) -> core::result::Result<Vec<PositionInventoryEntry>, AuthenticatedEndpointError> {
        self.positions.fetch(self.account_address).await
    }

    // ── Balance ───────────────────────────────────────────────────────────────

    /// Exact owned USDC collateral balance projected to the legacy `f64` API.
    pub async fn get_balance(&self) -> Result<f64> {
        self.authenticated_balance_allowance()
            .await?
            .balance()?
            .to_f64()
            .ok_or_else(|| anyhow::anyhow!("exact collateral balance is not representable as f64"))
    }

    /// Install one fresh SDK version and refresh its exact collateral
    /// authority while submission remains locally revoked.
    ///
    /// Closed recovery is the only caller. A failed SDK rebuild, version
    /// lookup, or balance/allowance update leaves the previous pair installed
    /// and keeps the local submission latch closed. Successful installation
    /// returns a monotonic authority token which every later capital read and
    /// final guarded open must match exactly.
    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
    pub async fn refresh_authenticated_capital_authority(
        &self,
    ) -> Result<AuthenticatedProtocolAuthority> {
        let _refresh = self.authority_refresh.lock().await;
        self.submission_gate().close();
        let (host, credentials, next_generation) = {
            let guard = self.client.read().await;
            (
                guard.host().as_str().to_owned(),
                guard.credentials().clone(),
                guard
                    .protocol_authority
                    .map(AuthenticatedProtocolAuthority::generation)
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or(crate::Error::InvalidClobConfiguration)?,
            )
        };

        let credential_identity = AuthenticatedCredentialIdentity::from_credentials(&credentials);
        let fresh = authenticate_declared_authority(
            &host,
            credentials,
            &self.signer,
            self.account_address,
            self.signature_type,
        )
        .await?;

        // One retry: a transient network blip on this single GET shouldn't
        // leave the (possibly still-poisoned) old client in place until the
        // next closed recovery if a second attempt would have succeeded.
        let version_raw = match fresh.version().await {
            Ok(v) => v,
            Err(first_error) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    error_class = protocol_version_retry_error_class(&first_error),
                    "CLOB protocol-version refresh: first GET /version attempt failed - retrying once"
                );
                fresh.version().await?
            }
        };
        let version = ProtocolVersion::try_from(version_raw)?;
        let authority =
            AuthenticatedProtocolAuthority::new(version, next_generation, credential_identity)?;

        self.authenticated_balance
            .refresh(
                &self.authenticated_http,
                &fresh,
                self.account_address,
                self.signature_type,
            )
            .await?;

        #[cfg(feature = "tracing")]
        tracing::info!(
            protocol_version = version_raw,
            protocol_generation = authority.generation(),
            host_class = "validated_clob",
            "CLOB protocol and capital authority refreshed while submission remained closed"
        );

        *self.client.write().await = AuthenticatedClientState {
            client: fresh,
            protocol_authority: Some(authority),
        };
        Ok(authority)
    }

    /// Verify one exact protocol authority against a fresh server observation
    /// and activate it only for the caller's synchronous guarded-open closure.
    ///
    /// The authority refresh mutex and client read guard remain held through
    /// `activate`, preventing a concurrent SDK replacement from interleaving
    /// between the final version check and publication of Live admission.
    pub async fn activate_authenticated_protocol_authority_if<F>(
        &self,
        expected: AuthenticatedProtocolAuthority,
        activate: F,
    ) -> core::result::Result<bool, crate::Error>
    where
        F: FnOnce() -> bool,
    {
        let _refresh = self.authority_refresh.lock().await;
        let guard = self.client.read().await;
        if guard.protocol_authority != Some(expected) {
            return Err(AuthenticatedEndpointError::request_failed(
                AuthenticatedEndpoint::ProtocolVersion,
            )
            .into());
        }
        let observed = self
            .authenticated_balance
            .protocol_version(&self.authenticated_http, &guard)
            .await?;
        if observed != expected.version() {
            return Err(AuthenticatedEndpointError::request_failed(
                AuthenticatedEndpoint::ProtocolVersion,
            )
            .into());
        }

        let mut submission_gate = self.submission_gate();
        if !submission_gate.can_activate(expected) {
            return Err(AuthenticatedEndpointError::request_failed(
                AuthenticatedEndpoint::ProtocolVersion,
            )
            .into());
        }
        let activated = activate();
        if activated {
            submission_gate.activate(expected);
        }
        Ok(activated)
    }

    /// Return a non-authorizing cache projection of the installed protocol.
    /// This does not imply that Live submission is open and cannot be used to
    /// mint a submission attempt.
    #[must_use]
    pub async fn authenticated_protocol_cache_identity(
        &self,
    ) -> Option<AuthenticatedProtocolCacheIdentity> {
        self.client
            .read()
            .await
            .protocol_authority
            .map(AuthenticatedProtocolAuthority::cache_identity)
    }

    /// Synchronously close rspm's local protocol submission latch.
    ///
    /// Engine admission owns the outer permit fence. This inner latch makes
    /// protocol recovery and mismatch handling fail closed even before the
    /// engine has completed its exclusive fence acquisition.
    pub fn revoke_authenticated_protocol_authority(&self) {
        self.submission_gate().close();
    }

    // ── Book ──────────────────────────────────────────────────────────────────

    /// Best bid price in the tradeable range [0.15, 0.85].
    /// Returns `None` when the book is empty or outside range.
    pub async fn get_best_bid(&self, token_id: &str) -> Option<f64> {
        let tid = parse_token_id(token_id).ok()?;
        let req = OrderBookSummaryRequest::builder().token_id(tid).build();
        let book = self.client.read().await.order_book(&req).await.ok()?;
        book.bids
            .iter()
            .filter_map(|b| b.price.to_f64())
            .filter(|&p| (0.15..=0.85).contains(&p))
            .reduce(f64::max)
    }

    /// Best ask price in the tradeable range [0.15, 0.85].
    /// Returns `None` when the book is empty or outside range.
    pub async fn get_best_ask(&self, token_id: &str) -> Option<f64> {
        let tid = parse_token_id(token_id).ok()?;
        let req = OrderBookSummaryRequest::builder().token_id(tid).build();
        let book = self.client.read().await.order_book(&req).await.ok()?;
        book.asks
            .iter()
            .filter_map(|a| a.price.to_f64())
            .filter(|&p| (0.15..=0.85).contains(&p))
            .reduce(f64::min)
    }

    /// Returns the mid-point price: (best_bid + best_ask) / 2.
    ///
    /// Returns `None` if the book is empty or either side is unavailable.
    pub async fn get_midpoint(&self, token_id: &str) -> Option<f64> {
        let bid = self.get_best_bid(token_id).await?;
        let ask = self.get_best_ask(token_id).await?;
        Some((bid + ask) / 2.0)
    }

    /// Returns the bid-ask spread: best_ask - best_bid.
    ///
    /// Returns `None` if either side is unavailable.
    pub async fn get_spread(&self, token_id: &str) -> Option<f64> {
        let bid = self.get_best_bid(token_id).await?;
        let ask = self.get_best_ask(token_id).await?;
        Some(ask - bid)
    }

    // ── Orders ────────────────────────────────────────────────────────────────

    /// Place a FAK limit order. Returns the order ID string on fill, or an
    /// error. A FAK that finds no match is treated as a non-fatal miss
    /// (returns `Ok(None)`).
    ///
    /// Bounded-retries a CLOB 429 (Too Many Requests) response per
    /// [`crate::retry::RetryPolicy::default`], honoring a `Retry-After` hint
    /// when the SDK exposes one - see
    /// [`crate::retry::classify_clob_error`]'s doc for the SDK's
    /// header-visibility limitation. Retries are bounded; exhaustion returns
    /// [`crate::ClobOperationError::RateLimited`] without retaining the SDK
    /// response.
    /// Place a FAK limit order while requiring fresh caller authority for
    /// every network attempt, including retries after rate-limit backoff.
    ///
    /// The opaque permit returned by `authorize_attempt` is held across the
    /// exact build + sign + POST attempt and dropped before retry backoff.
    /// Returning `None` prevents that attempt and every subsequent attempt.
    ///
    /// Outcome identity cannot cross this action boundary:
    ///
    /// ```compile_fail
    /// # async fn outcome_is_not_an_action(client: &rspm::ClobClient) {
    /// let _ = client
    ///     .submit_fak("no-token", rspm::Side::No, 0.5, 1.0, || async { None })
    ///     .await;
    /// # }
    /// ```
    ///
    /// The external SDK type is also private to the RSPM construction seam:
    ///
    /// ```compile_fail
    /// # async fn sdk_type_is_not_the_public_action(client: &rspm::ClobClient) {
    /// use polymarket_client_sdk_v2::clob::types::Side as SdkSide;
    /// let _ = client
    ///     .submit_fak("no-token", SdkSide::Buy, 0.5, 1.0, || async { None })
    ///     .await;
    /// # }
    /// ```
    #[allow(
        clippy::too_many_arguments,
        reason = "these parameters preserve the three public prepare_* boundaries"
    )]
    async fn prepare_limit_order(
        &self,
        token_id: &str,
        side: SdkSide,
        price: f64,
        size: f64,
        order_type: OrderType,
        client_marker: &str,
    ) -> core::result::Result<PreparedClobOrder, crate::ClobOperationError> {
        if client_marker.trim().is_empty() || client_marker.len() > MAX_CLIENT_MARKER_BYTES {
            return Err(crate::ClobOperationError::InvalidRequest);
        }
        let tid =
            parse_token_id(token_id).map_err(|_| crate::ClobOperationError::InvalidRequest)?;
        let price_dec =
            clob_price_decimal(price).map_err(|_| crate::ClobOperationError::InvalidRequest)?;
        let size_dec =
            clob_share_decimal(size).map_err(|_| crate::ClobOperationError::InvalidRequest)?;
        let _submission = self.submission_serial.lock().await;
        let (guard, permit) = self.submission_client().await?;
        let post_only = matches!(order_type, OrderType::GTC);
        let builder = guard
            .limit_order()
            .token_id(tid)
            .price(price_dec)
            .size(size_dec)
            .side(side)
            .order_type(order_type);
        let signable = if post_only {
            builder.post_only(true)
        } else {
            builder
        }
        .build()
        .await
        .map_err(|error| classify_operation_error(&error))?;
        let signed = guard
            .sign(&self.signer, signable)
            .await
            .map_err(|error| classify_operation_error(&error))?;
        if !self.submission_gate().matches(permit) {
            self.submission_gate().quarantine(permit.authority);
            return Err(crate::ClobOperationError::ProtocolAuthorityRevoked);
        }
        let chain_id = self
            .signer
            .chain_id()
            .ok_or(crate::ClobOperationError::InvalidRequest)?;
        let provider_order_id = guard
            .provider_order_id(&signed, chain_id)
            .await
            .map_err(|error| classify_operation_error(&error))?;
        Ok(PreparedClobOrder {
            signed,
            client_marker: client_marker.to_owned(),
            provider_order_id,
        })
    }

    /// Prepare a fill-and-kill order without sending an order POST.
    pub async fn prepare_fak(
        &self,
        token_id: &str,
        side: ClobSide,
        price: f64,
        size: f64,
        client_marker: &str,
    ) -> core::result::Result<PreparedClobOrder, crate::ClobOperationError> {
        self.prepare_limit_order(
            token_id,
            side.into(),
            price,
            size,
            OrderType::FAK,
            client_marker,
        )
        .await
    }

    /// Prepare a resting GTC maker buy without sending an order POST.
    pub async fn prepare_gtc_buy(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        client_marker: &str,
    ) -> core::result::Result<PreparedClobOrder, crate::ClobOperationError> {
        self.prepare_limit_order(
            token_id,
            SdkSide::Buy,
            price,
            size,
            OrderType::GTC,
            client_marker,
        )
        .await
    }

    /// Prepare a resting GTC maker sell without sending an order POST.
    pub async fn prepare_gtc_sell(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        client_marker: &str,
    ) -> core::result::Result<PreparedClobOrder, crate::ClobOperationError> {
        self.prepare_limit_order(
            token_id,
            SdkSide::Sell,
            price,
            size,
            OrderType::GTC,
            client_marker,
        )
        .await
    }

    /// Submit one prepared payload with bounded authorization-aware retries.
    pub async fn submit_prepared<A, Authorization>(
        &self,
        PreparedClobOrder { signed, provider_order_id, .. }: PreparedClobOrder,
        authorize_attempt: A,
    ) -> core::result::Result<Option<String>, crate::ClobOperationError>
    where
        A: FnMut() -> Authorization,
        Authorization: core::future::Future<Output = Option<SubmissionAttemptAuthority>>,
    {
        let expected_order_id = provider_order_id.clone();
        let response = retry_post_order(
            "submit_prepared",
            crate::retry::RetryPolicy::default(),
            authorize_attempt,
            || self.submission_gate().permit().is_some(),
            |attempt_authority| {
                let expected_order_id = expected_order_id.clone();
                async move {
                    let _submission = self.submission_serial.lock().await;
                    let (guard, protocol_permit) = self.submission_client().await?;
                    if !attempt_authority.revalidate(
                        self.submission_controller_binding,
                        protocol_permit.authority,
                    ) {
                        self.submission_gate().quarantine(protocol_permit.authority);
                        return Err(crate::ClobOperationError::SubmissionRevoked);
                    }
                    match self
                        .post_prepared_sdk(&guard, protocol_permit, &attempt_authority, signed)
                        .await?
                    {
                        PostOrderOutcome::Accepted(order_id) if order_id == expected_order_id => {
                            Ok(PostOrderOutcome::Accepted(order_id))
                        }
                        PostOrderOutcome::Accepted(_) => {
                            Err(crate::ClobOperationError::PreparedOrderMismatch)
                        }
                        outcome => Ok(outcome),
                    }
                }
            },
        )
        .await;
        match response {
            Ok(PostOrderOutcome::Accepted(order_id)) => Ok(Some(order_id)),
            Ok(PostOrderOutcome::NotAccepted) => Ok(None),
            Err(crate::ClobOperationError::NoMatch) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn submit_fak<A, Authorization>(
        &self,
        token_id: &str,
        side: ClobSide,
        price: f64,
        size: f64,
        authorize_attempt: A,
    ) -> core::result::Result<Option<String>, crate::ClobOperationError>
    where
        A: FnMut() -> Authorization,
        Authorization: core::future::Future<Output = Option<SubmissionAttemptAuthority>>,
    {
        let tid =
            parse_token_id(token_id).map_err(|_| crate::ClobOperationError::InvalidRequest)?;

        let price_dec =
            clob_price_decimal(price).map_err(|_| crate::ClobOperationError::InvalidRequest)?;
        let size_dec =
            clob_share_decimal(size).map_err(|_| crate::ClobOperationError::InvalidRequest)?;

        let policy = crate::retry::RetryPolicy::default();
        let resp = retry_post_order(
            "place_order",
            policy,
            authorize_attempt,
            || self.submission_gate().permit().is_some(),
            |attempt_authority| async move {
                let _submission = self.submission_serial.lock().await;
                let (guard, protocol_permit) = self.submission_client().await?;
                if !attempt_authority.revalidate(
                    self.submission_controller_binding,
                    protocol_permit.authority,
                ) {
                    self.submission_gate().quarantine(protocol_permit.authority);
                    return Err(crate::ClobOperationError::SubmissionRevoked);
                }
                let signable = guard
                    .limit_order()
                    .token_id(tid)
                    .price(price_dec)
                    .size(size_dec)
                    .side(side.into())
                    .order_type(OrderType::FAK)
                    .build()
                    .await
                    .map_err(|error| classify_operation_error(&error))?;
                self.sign_and_post_sdk(&guard, protocol_permit, &attempt_authority, signable)
                    .await
            },
        )
        .await;

        match resp {
            Ok(PostOrderOutcome::Accepted(order_id)) => Ok(Some(order_id)),
            Ok(PostOrderOutcome::NotAccepted) => Ok(None),
            Err(crate::ClobOperationError::NoMatch) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Place a GTC limit sell order. Returns `Ok(Some(order_id))` on success,
    /// `Ok(None)` when the order finds no counterpart.
    ///
    /// See [`Self::submit_fak`]'s doc for the bounded-retry / Retry-After
    /// contract - identical here.
    /// Place a GTC limit sell while requiring fresh caller authority for
    /// every network attempt, including retries after rate-limit backoff.
    ///
    /// The opaque permit returned by `authorize_attempt` is held across the
    /// exact build + sign + POST attempt and dropped before retry backoff.
    /// Returning `None` prevents that attempt and every subsequent attempt.
    pub async fn submit_gtc_sell<A, Authorization>(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        authorize_attempt: A,
    ) -> core::result::Result<Option<String>, ClobOperationError>
    where
        A: FnMut() -> Authorization,
        Authorization: core::future::Future<Output = Option<SubmissionAttemptAuthority>>,
    {
        let tid = parse_token_id(token_id).map_err(|_| ClobOperationError::InvalidRequest)?;

        let price_dec =
            clob_price_decimal(price).map_err(|_| ClobOperationError::InvalidRequest)?;
        let size_dec = clob_share_decimal(size).map_err(|_| ClobOperationError::InvalidRequest)?;

        let policy = crate::retry::RetryPolicy::default();
        retry_post_order(
            "sell",
            policy,
            authorize_attempt,
            || self.submission_gate().permit().is_some(),
            |attempt_authority| async move {
                let _submission = self.submission_serial.lock().await;
                let (guard, protocol_permit) = self.submission_client().await?;
                if !attempt_authority.revalidate(
                    self.submission_controller_binding,
                    protocol_permit.authority,
                ) {
                    self.submission_gate().quarantine(protocol_permit.authority);
                    return Err(crate::ClobOperationError::SubmissionRevoked);
                }
                let signable = guard
                    .limit_order()
                    .token_id(tid)
                    .price(price_dec)
                    .size(size_dec)
                    .side(SdkSide::Sell)
                    .post_only(true)
                    .build()
                    .await
                    .map_err(|error| classify_operation_error(&error))?;
                self.sign_and_post_sdk(&guard, protocol_permit, &attempt_authority, signable)
                    .await
            },
        )
        .await
        .map(|outcome| match outcome {
            PostOrderOutcome::Accepted(order_id) => Some(order_id),
            PostOrderOutcome::NotAccepted => None,
        })
    }

    /// Place a resting GTC maker buy (bid) at `price`.
    ///
    /// Posts a [`OrderType::GTC`] (Good-Til-Cancelled) limit buy - the SDK's
    /// GTC order rests on the book as a maker until it is crossed or cancelled.
    /// Returns `Ok(Some(order_id))` on success.
    ///
    /// Note: rspm's internal wire type [`crate::types::OrderType::GtcMaker`]
    /// serialises to the same `"GTC"` wire string; the SDK builder takes the SDK
    /// enum ([`polymarket::clob::types::OrderType`]) which has no `GtcMaker`
    /// variant - only `GTC`, `FAK`, `FOK`, and `GTD`.
    ///
    /// This is the entry point for the K2 maker best-fill watcher: after
    /// posting, callers hand the returned `order_id` to the watcher loop
    /// which monitors the book for a cross and calls [`Self::cancel_order`]
    /// for cancel-replace. In paper mode the caller bypasses this entirely
    /// and runs cross-detection against the in-process `LiveBookCache`.
    ///
    /// See [`Self::submit_fak`]'s doc for the bounded-retry / Retry-After
    /// contract - identical here.
    /// Place a resting GTC maker buy while requiring fresh caller authority
    /// for every network attempt, including retries after rate-limit backoff.
    ///
    /// The opaque permit returned by `authorize_attempt` is held across the
    /// exact build + sign + POST attempt and dropped before retry backoff.
    /// Returning `None` prevents that attempt and every subsequent attempt.
    pub async fn submit_gtc_buy<A, Authorization>(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        authorize_attempt: A,
    ) -> core::result::Result<Option<String>, crate::ClobOperationError>
    where
        A: FnMut() -> Authorization,
        Authorization: core::future::Future<Output = Option<SubmissionAttemptAuthority>>,
    {
        let tid =
            parse_token_id(token_id).map_err(|_| crate::ClobOperationError::InvalidRequest)?;

        let price_dec =
            clob_price_decimal(price).map_err(|_| crate::ClobOperationError::InvalidRequest)?;
        let size_dec =
            clob_share_decimal(size).map_err(|_| crate::ClobOperationError::InvalidRequest)?;

        let policy = crate::retry::RetryPolicy::default();
        retry_post_order(
            "place_gtc_buy",
            policy,
            authorize_attempt,
            || self.submission_gate().permit().is_some(),
            |attempt_authority| async move {
                let _submission = self.submission_serial.lock().await;
                let (guard, protocol_permit) = self.submission_client().await?;
                if !attempt_authority.revalidate(
                    self.submission_controller_binding,
                    protocol_permit.authority,
                ) {
                    self.submission_gate().quarantine(protocol_permit.authority);
                    return Err(crate::ClobOperationError::SubmissionRevoked);
                }
                let signable = guard
                    .limit_order()
                    .token_id(tid)
                    .price(price_dec)
                    .size(size_dec)
                    .side(SdkSide::Buy)
                    .order_type(OrderType::GTC)
                    .post_only(true)
                    .build()
                    .await
                    .map_err(|error| classify_operation_error(&error))?;
                self.sign_and_post_sdk(&guard, protocol_permit, &attempt_authority, signable)
                    .await
            },
        )
        .await
        .map(|outcome| match outcome {
            PostOrderOutcome::Accepted(order_id) => Some(order_id),
            PostOrderOutcome::NotAccepted => None,
        })
    }

    // ── Public market-trade feed ─────────────────────────────────────────────────

    /// Subscribe to the PUBLIC Polymarket market-wide taker-trade stream.
    ///
    /// Returns a stream of [`ClobTrade`] prints for each matched trade on the
    /// specified outcome tokens. Uses the unauthenticated public market WebSocket
    /// channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`), which
    /// carries aggressor side (taker buy vs. taker sell) on every print.
    ///
    /// This is distinct from the authenticated user-fill channel, which carries
    /// user-specific trade events without aggressor-side - do NOT conflate them.
    ///
    /// # Arguments
    ///
    /// * `token_ids` - decimal-string outcome token IDs to subscribe to
    ///   (e.g., the BTC-yes token for a BTC-USD-5m sprint market).
    ///
    /// # Notes
    ///
    /// * `slug` on each emitted [`ClobTrade`] defaults to the `token_id` string.
    ///   The worker task enriches this field before writing to QuestDB.
    /// * `ingest_ts`, `sequence`, and `trade_id` are `None` from the WS feed;
    ///   they are filled at write time or left null in QuestDB.
    /// * Fidelity-first: all prints are forwarded - no quality or spread filtering.
    #[cfg(feature = "watch")]
    pub fn subscribe_market_trades(
        &self,
        token_ids: Vec<String>,
    ) -> Result<impl futures::Stream<Item = Result<crate::types::ClobTrade>>> {
        use futures::StreamExt as _;
        use polymarket::clob::ws;

        let asset_ids: Vec<polymarket::types::U256> = token_ids
            .iter()
            .filter_map(|id| parse_token_id(id).ok())
            .collect();

        // Unauthenticated public WS client, market channel, no credentials needed.
        // The patched SDK's public subscription stream owns a `ClientInner`
        // keepalive. Returning the stream therefore keeps its connection alive,
        // while dropping the final stream deterministically cancels and releases
        // the channel tasks.
        let ws_client = ws::Client::default();
        let stream = ws_client.subscribe_last_trade_price(asset_ids)?;

        Ok(stream.map(|result| {
            result
                .map(crate::types::ClobTrade::from)
                .map_err(anyhow::Error::from)
        }))
    }
}

impl ClobClient {
    /// Fetch one public order-book snapshot through the owned client.
    pub async fn order_book(
        &self,
        token_id: &str,
    ) -> Result<OrderBookSummaryResponse, ClobOperationError> {
        let token_id = parse_token_id(token_id).map_err(|_| ClobOperationError::InvalidRequest)?;
        let request = OrderBookSummaryRequest::builder()
            .token_id(token_id)
            .build();
        self.client
            .read()
            .await
            .order_book(&request)
            .await
            .map_err(|error| classify_operation_error(&error))
    }

    /// Fetch the venue midpoint as an exact decimal value.
    pub async fn midpoint(&self, token_id: &str) -> Result<Decimal, crate::ClobOperationError> {
        let token_id = parse_token_id(token_id).map_err(|_| ClobOperationError::InvalidRequest)?;
        let request = MidpointRequest::builder().token_id(token_id).build();
        self.client
            .read()
            .await
            .midpoint(&request)
            .await
            .map(|response| response.mid)
            .map_err(|error| classify_operation_error(&error))
    }

    /// Cancel one exact venue order and retain the venue's typed evidence.
    pub async fn cancel_order(
        &self,
        order_id: &str,
    ) -> Result<CancelOrdersOutcome, crate::ClobOperationError> {
        if !venue_identifier_is_valid(order_id) {
            return Err(crate::ClobOperationError::InvalidRequest);
        }
        self.client
            .read()
            .await
            .cancel_order(order_id)
            .await
            .map(|response| CancelOrdersOutcome {
                canceled: response.canceled,
                not_canceled: response.not_canceled.into_iter().collect(),
            })
            .map_err(|error| classify_operation_error(&error))
    }

    /// Cancel every resting order and retain the venue's typed evidence.
    pub async fn cancel_all_orders(
        &self,
    ) -> Result<CancelOrdersOutcome, crate::ClobOperationError> {
        self.client
            .read()
            .await
            .cancel_all_orders()
            .await
            .map(|response| CancelOrdersOutcome {
                canceled: response.canceled,
                not_canceled: response.not_canceled.into_iter().collect(),
            })
            .map_err(|error| classify_operation_error(&error))
    }

    /// Open the credential-bound private user stream without exporting the
    /// authenticated SDK client or its order-building surface.
    #[cfg(feature = "ws")]
    pub async fn authenticated_user_ws(
        &self,
    ) -> Result<AuthenticatedUserWs, AuthenticatedUserWsError> {
        let credentials = self.client.read().await.credentials().clone();
        AuthenticatedUserWs::connect(credentials)
    }

    /// Seed deterministic order metadata without exposing the SDK test client.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub async fn seed_order_metadata_for_test(
        &self,
        token_id: &str,
    ) -> Result<(), crate::ClobOperationError> {
        let token_id =
            parse_token_id(token_id).map_err(|_| crate::ClobOperationError::InvalidRequest)?;
        let client = self.client.read().await;
        client.set_tick_size(token_id, polymarket::clob::types::TickSize::Hundredth);
        client.set_neg_risk(token_id, false);
        client.set_fee_rate_bps(token_id, 0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::str::FromStr as _;
    use polymarket::auth::Credentials;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    const TEST_PRIVATE_KEY: &str =
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

    #[test]
    fn share_serialization_truncates_without_inflating() {
        assert_eq!(clob_share_decimal(10.609).unwrap().to_string(), "10.60");
        assert_eq!(clob_share_decimal(10.6).unwrap().to_string(), "10.60");
        assert!(clob_share_decimal(0.001).is_err());
    }

    #[test]
    fn price_serialization_preserves_thousandth_ticks() {
        assert_eq!(clob_price_decimal(0.001).unwrap().to_string(), "0.001");
        assert_eq!(clob_price_decimal(0.555).unwrap().to_string(), "0.555");
    }

    #[tokio::test]
    async fn declared_authority_constructor_rebuilds_eoa_and_proxy_identity() {
        let signer = LocalSigner::from_str(TEST_PRIVATE_KEY)
            .expect("test signer")
            .with_chain_id(Some(POLYGON));
        let config = ClobConfig::new("http://127.0.0.1:1", None);
        let eoa = ClobClient::from_authenticated_authority(
            config.clone(),
            Credentials::default(),
            signer.clone(),
            None,
            SignatureType::Eoa,
        )
        .await
        .expect("exact EOA authority");
        assert_eq!(eoa.signature_type(), SignatureType::Eoa);
        assert_eq!(eoa.account_address(), signer.address());

        let proxy =
            Address::from_str("0x1111111111111111111111111111111111111111").expect("proxy address");
        let rebuilt = ClobClient::from_authenticated_authority(
            config.clone(),
            Credentials::default(),
            signer.clone(),
            Some(proxy),
            SignatureType::Proxy,
        )
        .await
        .expect("matching proxy parts");
        assert_eq!(rebuilt.signature_type(), SignatureType::Proxy);
        assert_eq!(rebuilt.account_address(), proxy);

        assert!(
            ClobClient::from_authenticated_authority(
                config.clone(),
                Credentials::default(),
                signer.clone(),
                None,
                SignatureType::Proxy,
            )
            .await
            .is_err(),
            "Proxy auth without an explicit funder must fail closed"
        );
        assert!(
            ClobClient::from_authenticated_authority(
                config.clone(),
                Credentials::default(),
                signer.clone(),
                Some(proxy),
                SignatureType::Eoa,
            )
            .await
            .is_err(),
            "EOA auth with a funder must fail closed"
        );
        assert!(
            ClobClient::from_authenticated_authority(
                config,
                Credentials::default(),
                signer.clone(),
                Some(signer.address()),
                SignatureType::Proxy,
            )
            .await
            .is_err(),
            "Proxy funder equal to the signer is an ambiguous EOA identity"
        );
    }

    #[tokio::test]
    async fn declared_authority_rejects_foreign_https_before_authentication() {
        let signer = LocalSigner::from_str(TEST_PRIVATE_KEY)
            .expect("test signer")
            .with_chain_id(Some(POLYGON));
        let result = ClobClient::from_authenticated_authority(
            ClobConfig::new("https://attacker.invalid", None),
            Credentials::default(),
            signer,
            None,
            SignatureType::Eoa,
        )
        .await;
        let Err(error) = result else {
            panic!("foreign credential-bearing authority must fail before a request");
        };
        assert!(matches!(error, crate::Error::InvalidClobConfiguration));
    }

    #[test]
    fn declared_authority_is_the_only_owned_authenticated_constructor() {
        let (source, _) = include_str!("client.rs")
            .split_once("#[cfg(test)]\nmod tests {")
            .expect("client.rs must retain one inline unit-test boundary");
        assert!(!source.contains("pub fn from_parts("));
        let constructor = source
            .split("pub async fn from_authenticated_authority(")
            .nth(1)
            .expect("owned authority constructor")
            .split("pub const fn account_address")
            .next()
            .expect("authority constructor boundary");
        assert!(constructor.contains("canonical_account_address("));
        assert!(constructor.contains("authenticate_declared_authority("));

        let rebuild = source
            .split("async fn authenticate_declared_authority(")
            .nth(1)
            .expect("authority rebuild")
            .split("impl ClobClient")
            .next()
            .expect("authority rebuild boundary");
        assert!(rebuild.contains(".signature_type(signature_type)"));
        assert!(rebuild.contains(".credentials(credentials)"));
        assert!(rebuild.contains("builder.funder(account_address)"));
        assert!(rebuild.contains("builder.authenticate().await?"));
    }

    #[test]
    fn every_constructor_uses_the_same_eoa_or_distinct_proxy_identity_contract() {
        let signer = LocalSigner::from_str(TEST_PRIVATE_KEY)
            .expect("test signer")
            .with_chain_id(Some(POLYGON));
        assert_eq!(
            canonical_account_address(signer.address(), SignatureType::Eoa, None)
                .expect("canonical EOA"),
            signer.address()
        );
        assert!(
            canonical_account_address(
                signer.address(),
                SignatureType::Proxy,
                Some(signer.address()),
            )
            .is_err()
        );
        assert!(
            canonical_account_address(signer.address(), SignatureType::Proxy, Some(Address::ZERO),)
                .is_err()
        );
        assert!(
            canonical_account_address(
                signer.address(),
                SignatureType::Eoa,
                Some(Address::from([7_u8; 20])),
            )
            .is_err()
        );
    }

    #[test]
    fn failed_activation_never_publishes_a_submission_permit() {
        let authority = AuthenticatedProtocolAuthority::for_test(ProtocolVersion::V2, 1);
        let mut gate = ProtocolSubmissionGate::default();
        assert!(gate.can_activate(authority));
        assert!(gate.permit().is_none());

        let failed = false;
        if failed {
            gate.activate(authority);
        }
        assert!(gate.permit().is_none());

        gate.activate(authority);
        let permit = gate
            .permit()
            .expect("successful activation publishes permit");
        assert!(gate.matches(permit));
        gate.quarantine(authority);
        assert!(!gate.matches(permit));
        assert!(
            !gate.can_activate(authority),
            "a quarantined generation requires a full refresh"
        );

        let fresh = AuthenticatedProtocolAuthority::for_test(ProtocolVersion::V2, 2);
        assert!(gate.can_activate(fresh));
        gate.activate(fresh);
        let fresh_permit = gate.permit().expect("fresh permit");
        gate.mark_indeterminate(fresh);
        assert!(gate.is_indeterminate());
        assert!(!gate.matches(fresh_permit));
        gate.close();
        assert!(
            gate.is_indeterminate(),
            "generic revocation must not erase an indeterminate submit"
        );
        assert!(!gate.can_activate(fresh));
        assert!(gate.can_activate(AuthenticatedProtocolAuthority::for_test(
            ProtocolVersion::V2,
            3,
        )));
    }

    #[test]
    fn dropped_or_server_failed_post_is_ambiguous_but_definite_rejection_is_not() {
        use polymarket::error::{Kind, Method, StatusCode};

        let dropped = polymarket::error::Error::with_source(
            Kind::Internal,
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "synthetic drop"),
        );
        let failed = polymarket::error::Error::status(
            StatusCode::BAD_GATEWAY,
            Method::POST,
            "/order".to_owned(),
            "synthetic server failure",
        );
        let rejected = polymarket::error::Error::status(
            StatusCode::BAD_REQUEST,
            Method::POST,
            "/order".to_owned(),
            "synthetic rejection",
        );
        assert!(post_response_is_ambiguous(&dropped));
        assert!(post_response_is_ambiguous(&failed));
        assert!(!post_response_is_ambiguous(&rejected));
    }

    #[test]
    fn submit_error_classification_discards_sdk_body_path_and_identifiers() {
        let sdk_error = polymarket::error::Error::status(
            polymarket::error::StatusCode::BAD_REQUEST,
            polymarket::error::Method::POST,
            "/order/private-order-id".to_owned(),
            "secret-body token=private-token no orders found to match with FAK order",
        );
        let classified = classify_operation_error(&sdk_error);
        assert_eq!(classified, crate::ClobOperationError::NoMatch);
        let rendered = format!("{classified:?} {classified}");
        for secret in [
            "secret-body",
            "private-order-id",
            "private-token",
            "/order/",
        ] {
            assert!(
                !rendered.contains(secret),
                "operation error leaked {secret}"
            );
        }
    }

    #[test]
    fn stale_domain_balance_zero_revokes_protocol_authority_without_leaking_body() {
        let sdk_error = polymarket::error::Error::status(
            polymarket::error::StatusCode::BAD_REQUEST,
            polymarket::error::Method::POST,
            "/order/private-order-id".to_owned(),
            "not enough balance / allowance: balance: 0 private-token",
        );
        let status = sdk_error
            .downcast_ref::<polymarket::error::Status>()
            .expect("status error");
        assert!(status_revokes_protocol_authority(status));
        let classified = classify_operation_error(&sdk_error);
        assert_eq!(
            classified,
            crate::ClobOperationError::ProtocolAuthorityRevoked
        );
        let rendered = format!("{sdk_error:?} {sdk_error} {classified:?} {classified}");
        for secret in [
            "private-order-id",
            "private-token",
            "not enough balance",
            "balance: 0",
        ] {
            assert!(
                !rendered.contains(secret),
                "authority error leaked {secret}"
            );
        }
    }

    #[test]
    fn protocol_version_retry_diagnostic_discards_provider_error() {
        let hostile =
            "Bearer api-key raw-body https://secret.invalid/order/private-order-id".to_owned();
        let rendered = protocol_version_retry_error_class(&hostile);
        assert_eq!(rendered, "clob_protocol_version_retry");
        for secret in [
            "Bearer",
            "api-key",
            "raw-body",
            "secret.invalid",
            "private-order-id",
        ] {
            assert!(!rendered.contains(secret));
        }
    }

    #[tokio::test]
    async fn revoked_attempt_authority_prevents_the_first_post() {
        let post_attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_post = Arc::clone(&post_attempts);
        let result = retry_post_order(
            "revoked_before_first_attempt",
            crate::retry::RetryPolicy {
                max_attempts: 2,
                base_backoff: core::time::Duration::from_millis(10),
                max_backoff: core::time::Duration::from_millis(10),
            },
            || core::future::ready(None::<SubmissionAttemptAuthority>),
            || true,
            move |_authority: SubmissionAttemptAuthority| {
                attempts_for_post.fetch_add(1, Ordering::AcqRel);
                async { Err::<(), _>(crate::ClobOperationError::RateLimited { retry_after: None }) }
            },
        )
        .await;

        assert_eq!(result, Err(crate::ClobOperationError::SubmissionRevoked));
        assert_eq!(post_attempts.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn revocation_during_retry_backoff_prevents_a_second_post() {
        let post_attempts = Arc::new(AtomicUsize::new(0));
        let (first_post_tx, mut first_post_rx) = tokio::sync::mpsc::unbounded_channel();

        // A real controller/authority pair, exercising the exact `authorize_if`
        // path production callers use, rather than a synthetic stand-in - the
        // capability's fields are private outside `submission`, so this is the
        // only way to hand `retry_post_order` a genuine `SubmissionAttemptAuthority`.
        let (_binding, controller) = controller_pair().expect("controller identity");
        let protocol_authority = AuthenticatedProtocolAuthority::for_test(ProtocolVersion::V2, 1);
        let epoch = controller.revocation_epoch();
        let exclusive = controller.acquire_exclusive().await;
        assert!(exclusive.activate(epoch, protocol_authority));
        drop(exclusive);

        let controller_for_task = controller.clone();
        let attempts_for_task = Arc::clone(&post_attempts);
        let submit = tokio::spawn(async move {
            retry_post_order(
                "revoked_during_backoff",
                crate::retry::RetryPolicy {
                    max_attempts: 2,
                    base_backoff: core::time::Duration::from_millis(50),
                    max_backoff: core::time::Duration::from_millis(50),
                },
                move || {
                    let controller = controller_for_task.clone();
                    async move { controller.authorize_if(protocol_authority, || true).await }
                },
                || true,
                move |_authority: SubmissionAttemptAuthority| {
                    let attempt = attempts_for_task.fetch_add(1, Ordering::AcqRel) + 1;
                    if attempt == 1 {
                        assert!(first_post_tx.send(()).is_ok());
                    }
                    async {
                        Err::<(), _>(crate::ClobOperationError::RateLimited { retry_after: None })
                    }
                },
            )
            .await
        });

        first_post_rx
            .recv()
            .await
            .expect("first simulated POST reached the retry boundary");
        // Real revocation - `authorize_if` naturally denies every attempt after
        // this, since `request_revocation` clears the active authority.
        controller.request_revocation();
        let result = submit.await.expect("retry task joined");

        assert_eq!(result, Err(crate::ClobOperationError::SubmissionRevoked));
        assert_eq!(post_attempts.load(Ordering::Acquire), 1);
    }
}
