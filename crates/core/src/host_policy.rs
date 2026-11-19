//! Which hosts the service may connect to on a tenant's behalf.
//!
//! A sync's origin (database, broker, custom endpoint) and its Meilisearch
//! destination are tenant-supplied addresses that the service connects to
//! from inside its own network, where the cloud metadata endpoint
//! (`169.254.169.254`), the state database and every internal service are
//! reachable. [`HostPolicy`] decides, before any connection is opened,
//! whether an address may be used:
//!
//! | `CONNECT_HOST_POLICY` | Accepts |
//! |---|---|
//! | `public` (default with `LAB_SERVICE_TOKEN`) | names whose **every** resolved address is public, and public literal addresses |
//! | `any` (default standalone) | every host |
//! | `allowlist` | the hosts and networks of `CONNECT_HOST_ALLOWLIST` only |
//!
//! In every mode the service's own state database (`DATABASE_URL`) is
//! refused as a source origin ([`HostPolicy::forbid_origin`]).
//!
//! Errors name the host as given, never the addresses it resolved to.
//!
//! This is the one place in `core` that does I/O: a DNS lookup through the
//! runtime's resolver ([`tokio::net::lookup_host`]).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Environment variable selecting the mode: `public`, `any` or `allowlist`.
pub const MODE_ENV: &str = "CONNECT_HOST_POLICY";
/// Environment variable listing the allowed hosts and networks of the
/// `allowlist` mode: comma-separated `host`, `host:port`, IP addresses and
/// CIDRs (`10.0.0.0/8`, `fd00::/8`).
pub const ALLOWLIST_ENV: &str = "CONNECT_HOST_ALLOWLIST";

/// Longest DNS lookup before the host counts as unresolvable.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

/// One address a connector connects to: a host name or literal IP (IPv6
/// without brackets) and a port.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    /// Normalizes the host: trimmed, brackets of an IPv6 literal removed,
    /// lowercased, trailing dot removed.
    pub fn new(host: impl AsRef<str>, port: u16) -> Self {
        Self {
            host: normalize_host(host.as_ref()),
            port,
        }
    }

    /// Parses `host:port`, `[v6]:port`, or a bare host with `default_port`.
    pub fn parse(raw: &str, default_port: u16) -> Option<Self> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        if let Some(rest) = raw.strip_prefix('[') {
            let (host, after) = rest.split_once(']')?;
            let port = match after {
                "" => default_port,
                p => p.strip_prefix(':')?.parse().ok()?,
            };
            return (!host.is_empty()).then(|| Self::new(host, port));
        }
        // A bare IPv6 literal has several colons and no port.
        if raw.matches(':').count() > 1 {
            return raw
                .parse::<Ipv6Addr>()
                .ok()
                .map(|_| Self::new(raw, default_port));
        }
        match raw.split_once(':') {
            Some((host, port)) => {
                let port = port.parse().ok()?;
                (!host.is_empty()).then(|| Self::new(host, port))
            }
            None => Some(Self::new(raw, default_port)),
        }
    }

    /// The host as a literal IP, when it is one.
    pub fn ip(&self) -> Option<IpAddr> {
        self.host.parse().ok()
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

fn normalize_host(host: &str) -> String {
    let h = host.trim();
    let h = h
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(h);
    h.trim_end_matches('.').to_ascii_lowercase()
}

/// What kind of address a host resolved to. Only [`AddressClass::Public`]
/// passes the `public` mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressClass {
    Public,
    /// `127.0.0.0/8`, `::1`.
    Loopback,
    /// RFC 1918: `10/8`, `172.16/12`, `192.168/16`.
    Private,
    /// `169.254/16` (the cloud metadata endpoint), `fe80::/10`.
    LinkLocal,
    /// Carrier-grade NAT `100.64/10`.
    Cgnat,
    /// IPv6 unique local `fc00::/7`.
    UniqueLocal,
    /// `0.0.0.0`, `::`.
    Unspecified,
    /// Multicast and broadcast.
    Multicast,
    /// Special-purpose blocks no public host lives in: `0/8`, `192.0.0/24`,
    /// documentation and benchmarking ranges, `240/4`, and their IPv6
    /// counterparts.
    Reserved,
}

/// Classify one address. An IPv6 address carrying an IPv4 one (mapped,
/// NAT64, 6to4, compatible) is classified as that IPv4 address, so
/// `::ffff:127.0.0.1` is loopback.
pub fn classify(ip: IpAddr) -> AddressClass {
    match ip {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => classify_v6(v6),
    }
}

fn classify_v4(ip: Ipv4Addr) -> AddressClass {
    let [a, b, c, _] = ip.octets();
    if ip.is_unspecified() {
        AddressClass::Unspecified
    } else if ip.is_loopback() {
        AddressClass::Loopback
    } else if ip.is_link_local() {
        AddressClass::LinkLocal
    } else if a == 100 && (64..128).contains(&b) {
        AddressClass::Cgnat
    } else if ip.is_private() {
        AddressClass::Private
    } else if ip.is_multicast() || ip.is_broadcast() {
        AddressClass::Multicast
    } else if is_reserved_v4(a, b, c) {
        AddressClass::Reserved
    } else {
        AddressClass::Public
    }
}

fn is_reserved_v4(a: u8, b: u8, c: u8) -> bool {
    match (a, b, c) {
        // 0.0.0.0/8, "this network".
        (0, ..) => true,
        // 192.0.0.0/24 protocol assignments, TEST-NET-1.
        (192, 0, 0 | 2) => true,
        // 198.18.0.0/15 benchmarking.
        (198, 18 | 19, _) => true,
        // TEST-NET-2, TEST-NET-3.
        (198, 51, 100) | (203, 0, 113) => true,
        // 240.0.0.0/4 reserved (broadcast included).
        (240.., ..) => true,
        _ => false,
    }
}

fn is_reserved_v6(s: [u16; 8]) -> bool {
    // 64:ff9b:1::/48 local-use NAT64.
    s[..3] == [0x64, 0xff9b, 1]
        // 2001::/32 Teredo, 2001:db8::/32 documentation.
        || s[..2] == [0x2001, 0]
        || s[..2] == [0x2001, 0xdb8]
        // 3fff::/20 documentation.
        || (s[0] == 0x3fff && s[1] < 0x1000)
        // 100::/64 discard-only.
        || s[..4] == [0x100, 0, 0, 0]
}

fn classify_v6(ip: Ipv6Addr) -> AddressClass {
    let s = ip.segments();
    let v4 = |hi: u16, lo: u16| Ipv4Addr::from((u32::from(hi) << 16) | u32::from(lo));
    if ip.is_unspecified() {
        AddressClass::Unspecified
    } else if ip.is_loopback() {
        AddressClass::Loopback
    } else if let Some(mapped) = ip.to_ipv4_mapped() {
        classify_v4(mapped)
    } else if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        // NAT64 (RFC 6052): the IPv4 address it translates to.
        classify_v4(v4(s[6], s[7]))
    } else if s[0] == 0x2002 {
        // 6to4: the IPv4 address of the relay.
        classify_v4(v4(s[1], s[2]))
    } else if s[..6] == [0; 6] {
        // IPv4-compatible `::a.b.c.d`, deprecated and never routed.
        match classify_v4(v4(s[6], s[7])) {
            AddressClass::Public => AddressClass::Reserved,
            other => other,
        }
    } else if (s[0] & 0xffc0) == 0xfe80 {
        AddressClass::LinkLocal
    } else if (s[0] & 0xfe00) == 0xfc00 {
        AddressClass::UniqueLocal
    } else if ip.is_multicast() {
        AddressClass::Multicast
    } else if is_reserved_v6(s) {
        AddressClass::Reserved
    } else {
        AddressClass::Public
    }
}

/// One entry of `CONNECT_HOST_ALLOWLIST`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllowEntry {
    /// A host name (any address it resolves to is trusted), optionally
    /// pinned to one port.
    Host { name: String, port: Option<u16> },
    /// A network: every resolved address must fall in one.
    Net { addr: IpAddr, prefix: u8 },
}

impl AllowEntry {
    fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        let bad = |why: &str| {
            format!(
                "invalid {ALLOWLIST_ENV} entry {raw:?}: {why}; expected host, host:port, an IP \
                 address or a CIDR, comma-separated"
            )
        };
        if raw.is_empty() {
            return Err(bad("empty entry"));
        }
        if raw.contains("://") || raw.contains('@') {
            return Err(bad("give a bare host, not a URL"));
        }
        if let Some((addr, prefix)) = raw.split_once('/') {
            let addr: IpAddr = addr
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse()
                .map_err(|_| bad("not an IP network"))?;
            let max = if addr.is_ipv4() { 32 } else { 128 };
            let prefix: u8 = prefix.parse().map_err(|_| bad("bad prefix length"))?;
            if prefix > max {
                return Err(bad("prefix length too long"));
            }
            return Ok(AllowEntry::Net { addr, prefix });
        }
        if let Ok(addr) = raw
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
        {
            let prefix = if addr.is_ipv4() { 32 } else { 128 };
            return Ok(AllowEntry::Net { addr, prefix });
        }
        let ep = Endpoint::parse(raw, 0).ok_or_else(|| bad("not host[:port]"))?;
        if ep
            .host
            .bytes()
            .any(|b| !(b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_'))
        {
            return Err(bad("invalid host name"));
        }
        Ok(AllowEntry::Host {
            name: ep.host,
            port: (ep.port != 0).then_some(ep.port),
        })
    }

    fn contains(&self, ip: IpAddr) -> bool {
        let AllowEntry::Net { addr, prefix } = self else {
            return false;
        };
        match (canonical(*addr), canonical(ip)) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = u32::MAX.checked_shl(32 - u32::from(*prefix)).unwrap_or(0);
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = u128::MAX.checked_shl(128 - u32::from(*prefix)).unwrap_or(0);
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

/// IPv4-mapped IPv6 addresses compare as IPv4.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 => v4,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyMode {
    /// Public addresses only.
    Public,
    /// No restriction (but the forbidden origins).
    Any,
    /// The listed hosts and networks only.
    Allowlist(Vec<AllowEntry>),
}

/// Why a host was refused. The message names the host as given, never what
/// it resolved to.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HostPolicyError {
    #[error("`{host}` is not allowed by this service's host policy ({hint})")]
    Blocked { host: String, hint: &'static str },
    #[error("`{host}` cannot be resolved")]
    Unresolvable { host: String },
}

/// See the module documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPolicy {
    mode: PolicyMode,
    /// Never allowed as a source origin, in any mode: the state database.
    forbidden_origins: Vec<Endpoint>,
}

impl Default for HostPolicy {
    /// The safe default: public addresses only.
    fn default() -> Self {
        Self::public()
    }
}

impl HostPolicy {
    pub fn public() -> Self {
        Self::with_mode(PolicyMode::Public)
    }

    pub fn any() -> Self {
        Self::with_mode(PolicyMode::Any)
    }

    pub fn with_mode(mode: PolicyMode) -> Self {
        Self {
            mode,
            forbidden_origins: Vec::new(),
        }
    }

    /// Reads the mode and allowlist values (`CONNECT_HOST_POLICY`,
    /// `CONNECT_HOST_ALLOWLIST`). `default` applies when `mode` is unset.
    pub fn parse(
        mode: Option<&str>,
        allowlist: Option<&str>,
        default: PolicyMode,
    ) -> Result<Self, String> {
        let entries = || -> Result<Vec<AllowEntry>, String> {
            let raw = allowlist.map(str::trim).unwrap_or_default();
            if raw.is_empty() {
                return Err(format!(
                    "{MODE_ENV}=allowlist needs {ALLOWLIST_ENV} (comma-separated hosts, \
                     host:port, IP addresses or CIDRs)"
                ));
            }
            raw.split(',').map(AllowEntry::parse).collect()
        };
        let mode = match mode.map(|m| m.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") => default,
            Some("public") => PolicyMode::Public,
            Some("any") => PolicyMode::Any,
            Some("allowlist") => PolicyMode::Allowlist(entries()?),
            Some(_) => {
                return Err(format!("{MODE_ENV} must be `public`, `any` or `allowlist`"));
            }
        };
        if !matches!(mode, PolicyMode::Allowlist(_))
            && allowlist.is_some_and(|a| !a.trim().is_empty())
        {
            return Err(format!(
                "{ALLOWLIST_ENV} is set but {MODE_ENV} is not `allowlist`"
            ));
        }
        Ok(Self::with_mode(mode))
    }

    /// Refuse this endpoint as a source origin in every mode (the state
    /// database). Matched by name and by resolved address.
    pub fn forbid_origin(mut self, endpoint: Endpoint) -> Self {
        self.forbidden_origins.push(endpoint);
        self
    }

    pub fn mode(&self) -> &PolicyMode {
        &self.mode
    }

    pub fn mode_name(&self) -> &'static str {
        match self.mode {
            PolicyMode::Public => "public",
            PolicyMode::Any => "any",
            PolicyMode::Allowlist(_) => "allowlist",
        }
    }

    pub fn is_any(&self) -> bool {
        self.mode == PolicyMode::Any
    }

    fn hint(&self) -> &'static str {
        match self.mode {
            PolicyMode::Public => "only public internet addresses are allowed",
            PolicyMode::Any => "it is reserved by the service",
            PolicyMode::Allowlist(_) => "only the hosts its operator allowed are",
        }
    }

    fn blocked(&self, host: &str) -> HostPolicyError {
        HostPolicyError::Blocked {
            host: host.to_owned(),
            hint: self.hint(),
        }
    }

    fn name_allowed(&self, host: &str, port: Option<u16>) -> bool {
        match &self.mode {
            PolicyMode::Allowlist(entries) => entries.iter().any(|e| match e {
                AllowEntry::Host { name, port: p } => {
                    *name == host && (p.is_none() || port.is_none() || *p == port)
                }
                AllowEntry::Net { .. } => false,
            }),
            _ => false,
        }
    }

    fn address_allowed(&self, ip: IpAddr) -> bool {
        match &self.mode {
            PolicyMode::Any => true,
            PolicyMode::Public => classify(ip) == AddressClass::Public,
            PolicyMode::Allowlist(entries) => entries.iter().any(|e| e.contains(ip)),
        }
    }

    /// Vet a literal address, without any lookup. `port` is `None` when
    /// unknown (any port).
    pub fn check_ip(&self, ip: IpAddr, port: Option<u16>) -> Result<(), HostPolicyError> {
        let shown = ip.to_string();
        if self.address_allowed(ip) || self.name_allowed(&shown, port) {
            Ok(())
        } else {
            Err(self.blocked(&shown))
        }
    }

    /// Resolve `host` and return its addresses (with `port`) when the policy
    /// allows **every** one of them. A literal IP is vetted as is. In the
    /// `public` and `allowlist` modes a name that does not resolve is
    /// reported like a refused one, so a tenant cannot probe which internal
    /// names exist.
    pub async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, HostPolicyError> {
        let shown = host.trim();
        let host = normalize_host(host);
        if host.is_empty() {
            return Err(HostPolicyError::Unresolvable {
                host: shown.to_owned(),
            });
        }
        let pinned = (port != 0).then_some(port);
        if let Ok(ip) = host.parse::<IpAddr>() {
            self.check_ip(ip, pinned)?;
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        let by_name = self.name_allowed(&host, pinned);
        let addrs = match lookup(&host, port).await {
            Some(addrs) if !addrs.is_empty() => addrs,
            _ if self.is_any() || by_name => {
                return Err(HostPolicyError::Unresolvable {
                    host: shown.to_owned(),
                });
            }
            _ => return Err(self.blocked(shown)),
        };
        if by_name || addrs.iter().all(|a| self.address_allowed(a.ip())) {
            Ok(addrs)
        } else {
            Err(self.blocked(shown))
        }
    }

    /// Vet a source origin: [`HostPolicy::resolve`] plus the forbidden
    /// origins. In the `any` mode a name that does not resolve passes (the
    /// connector reports it).
    pub async fn vet_origin(&self, endpoint: &Endpoint) -> Result<(), HostPolicyError> {
        let addrs = match self.resolve(&endpoint.host, endpoint.port).await {
            Ok(addrs) => addrs,
            Err(HostPolicyError::Unresolvable { .. }) if self.is_any() => Vec::new(),
            Err(e) => return Err(e),
        };
        for forbidden in &self.forbidden_origins {
            if forbidden.port != endpoint.port {
                continue;
            }
            if forbidden.host == normalize_host(&endpoint.host) {
                return Err(self.reserved(&endpoint.host));
            }
            let theirs: Vec<IpAddr> = match forbidden.ip() {
                Some(ip) => vec![ip],
                None => lookup(&forbidden.host, forbidden.port)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|a| a.ip())
                    .collect(),
            };
            if addrs
                .iter()
                .any(|a| theirs.iter().any(|t| same_place(a.ip(), *t)))
            {
                return Err(self.reserved(&endpoint.host));
            }
        }
        Ok(())
    }

    fn reserved(&self, host: &str) -> HostPolicyError {
        HostPolicyError::Blocked {
            host: host.trim().to_owned(),
            hint: match self.mode {
                PolicyMode::Any => "it is reserved by the service",
                _ => self.hint(),
            },
        }
    }
}

/// Two addresses reach the same server: equal, or both local to this host
/// (loopback and unspecified addresses all land on the local machine).
fn same_place(a: IpAddr, b: IpAddr) -> bool {
    let local = |ip: IpAddr| ip.is_loopback() || ip.is_unspecified();
    let (a, b) = (canonical(a), canonical(b));
    a == b || (local(a) && local(b))
}

async fn lookup(host: &str, port: u16) -> Option<Vec<SocketAddr>> {
    match tokio::time::timeout(LOOKUP_TIMEOUT, tokio::net::lookup_host((host, port))).await {
        Ok(Ok(addrs)) => Some(addrs.collect()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn classification_covers_the_internal_ranges() {
        for (addr, class) in [
            ("127.0.0.1", AddressClass::Loopback),
            ("127.3.4.5", AddressClass::Loopback),
            ("::1", AddressClass::Loopback),
            ("10.1.2.3", AddressClass::Private),
            ("172.16.0.1", AddressClass::Private),
            ("172.31.255.255", AddressClass::Private),
            ("192.168.1.1", AddressClass::Private),
            ("100.64.0.1", AddressClass::Cgnat),
            ("100.127.255.255", AddressClass::Cgnat),
            ("169.254.169.254", AddressClass::LinkLocal),
            ("fe80::1", AddressClass::LinkLocal),
            ("fc00::1", AddressClass::UniqueLocal),
            ("fd12:3456::1", AddressClass::UniqueLocal),
            ("0.0.0.0", AddressClass::Unspecified),
            ("::", AddressClass::Unspecified),
            ("224.0.0.1", AddressClass::Multicast),
            ("ff02::1", AddressClass::Multicast),
            ("255.255.255.255", AddressClass::Multicast),
            ("192.0.2.1", AddressClass::Reserved),
            ("198.51.100.1", AddressClass::Reserved),
            ("203.0.113.9", AddressClass::Reserved),
            ("2001:db8::1", AddressClass::Reserved),
            ("0.1.2.3", AddressClass::Reserved),
            // IPv4 hidden in IPv6.
            ("::ffff:127.0.0.1", AddressClass::Loopback),
            ("::ffff:169.254.169.254", AddressClass::LinkLocal),
            ("64:ff9b::a00:5", AddressClass::Private),
            ("2002:7f00:1::", AddressClass::Loopback),
            ("::10.0.0.1", AddressClass::Private),
            // Public.
            ("8.8.8.8", AddressClass::Public),
            ("1.1.1.1", AddressClass::Public),
            ("100.128.0.1", AddressClass::Public),
            ("2606:4700::1111", AddressClass::Public),
            ("64:ff9b::808:808", AddressClass::Public),
        ] {
            assert_eq!(classify(ip(addr)), class, "{addr}");
        }
    }

    #[test]
    fn modes_parse() {
        assert_eq!(
            HostPolicy::parse(None, None, PolicyMode::Any).unwrap().mode,
            PolicyMode::Any
        );
        assert_eq!(
            HostPolicy::parse(None, None, PolicyMode::Public)
                .unwrap()
                .mode,
            PolicyMode::Public
        );
        assert_eq!(
            HostPolicy::parse(Some(" Any "), None, PolicyMode::Public)
                .unwrap()
                .mode,
            PolicyMode::Any
        );
        let p = HostPolicy::parse(
            Some("allowlist"),
            Some("db.internal:5432, meili, 10.0.0.0/8,[fd00::]/8, 192.168.1.7"),
            PolicyMode::Public,
        )
        .unwrap();
        assert_eq!(
            p.mode,
            PolicyMode::Allowlist(vec![
                AllowEntry::Host {
                    name: "db.internal".into(),
                    port: Some(5432)
                },
                AllowEntry::Host {
                    name: "meili".into(),
                    port: None
                },
                AllowEntry::Net {
                    addr: ip("10.0.0.0"),
                    prefix: 8
                },
                AllowEntry::Net {
                    addr: ip("fd00::"),
                    prefix: 8
                },
                AllowEntry::Net {
                    addr: ip("192.168.1.7"),
                    prefix: 32
                },
            ])
        );
        for (mode, list) in [
            (Some("nope"), None),
            (Some("allowlist"), None),
            (Some("allowlist"), Some("a,,b")),
            (Some("allowlist"), Some("http://x")),
            (Some("allowlist"), Some("10.0.0.0/33")),
            (Some("allowlist"), Some("a b")),
            (Some("public"), Some("x")),
        ] {
            assert!(
                HostPolicy::parse(mode, list, PolicyMode::Any).is_err(),
                "{mode:?} {list:?}"
            );
        }
    }

    #[test]
    fn endpoints_parse() {
        assert_eq!(
            Endpoint::parse("Db.Example.com.:6543", 1),
            Some(Endpoint::new("db.example.com", 6543))
        );
        assert_eq!(Endpoint::parse("h", 9092), Some(Endpoint::new("h", 9092)));
        assert_eq!(Endpoint::parse("[::1]:9", 1), Some(Endpoint::new("::1", 9)));
        assert_eq!(Endpoint::parse("::1", 7), Some(Endpoint::new("::1", 7)));
        assert_eq!(Endpoint::parse("h:nope", 1), None);
        assert_eq!(Endpoint::parse("", 1), None);
        assert_eq!(Endpoint::new("::1", 5).to_string(), "[::1]:5");
    }

    #[tokio::test]
    async fn public_refuses_internal_literals_and_names() {
        let p = HostPolicy::public();
        for host in [
            "127.0.0.1",
            "10.0.0.5",
            "169.254.169.254",
            "::ffff:127.0.0.1",
            "[::1]",
            "0.0.0.0",
            "localhost",
            "LOCALHOST.",
        ] {
            let err = p.resolve(host, 5432).await.unwrap_err();
            assert!(
                matches!(err, HostPolicyError::Blocked { .. }),
                "{host}: {err:?}"
            );
            let msg = err.to_string();
            assert!(
                !msg.contains("127.0.0.1") || host.contains("127.0.0.1"),
                "{msg}"
            );
        }
        // A name that does not resolve looks like a refused one.
        let err = p.resolve("does-not-exist.invalid", 1).await.unwrap_err();
        assert!(matches!(err, HostPolicyError::Blocked { .. }), "{err:?}");
        assert!(p.resolve("8.8.8.8", 53).await.is_ok());
        assert!(p.check_ip(ip("1.1.1.1"), None).is_ok());
    }

    #[tokio::test]
    async fn the_message_names_the_host_not_what_it_resolved_to() {
        let err = HostPolicy::public()
            .resolve("localhost", 80)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("`localhost`"), "{err}");
        assert!(!err.contains("127.0.0.1") && !err.contains("::1"), "{err}");
    }

    #[tokio::test]
    async fn any_allows_everything_but_the_forbidden_origins() {
        let p = HostPolicy::any().forbid_origin(Endpoint::new("127.0.0.1", 5432));
        assert!(p.resolve("127.0.0.1", 5432).await.is_ok());
        assert!(p.vet_origin(&Endpoint::new("10.0.0.1", 5432)).await.is_ok());
        assert!(
            p.vet_origin(&Endpoint::new("127.0.0.1", 5433))
                .await
                .is_ok()
        );
        // Unresolvable names pass in `any`: the connector reports them.
        assert!(
            p.vet_origin(&Endpoint::new("does-not-exist.invalid", 5432))
                .await
                .is_ok()
        );
        for host in [
            "127.0.0.1",
            "localhost",
            "0.0.0.0",
            "::ffff:127.0.0.1",
            "127.0.0.2",
        ] {
            let err = p.vet_origin(&Endpoint::new(host, 5432)).await.unwrap_err();
            assert!(matches!(err, HostPolicyError::Blocked { .. }), "{host}");
        }
        let by_name = HostPolicy::any().forbid_origin(Endpoint::new("State-DB", 5432));
        assert!(
            by_name
                .vet_origin(&Endpoint::new("state-db.", 5432))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn allowlist_accepts_exactly_its_entries() {
        let p = HostPolicy::parse(
            Some("allowlist"),
            Some("localhost:5432,10.0.0.0/8,meili.internal"),
            PolicyMode::Public,
        )
        .unwrap();
        assert!(p.resolve("localhost", 5432).await.is_ok());
        assert!(p.resolve("localhost", 5433).await.is_err(), "pinned port");
        assert!(p.resolve("10.9.8.7", 1).await.is_ok());
        assert!(p.resolve("11.0.0.1", 1).await.is_err());
        assert!(p.resolve("127.0.0.1", 5432).await.is_err());
        assert!(p.check_ip(ip("::ffff:10.1.1.1"), None).is_ok());
        let err = p.resolve("8.8.8.8", 53).await.unwrap_err().to_string();
        assert!(
            !err.contains("10.0.0.0") && !err.contains("meili.internal"),
            "{err}"
        );
    }
}
