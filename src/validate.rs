//! Checks for strings that peers and browsers supply and the hub shows to everyone.

pub const MAX_FIELD_LEN: usize = 64;

/// Classifier labels the hub knows. Anything else is shown as "other".
pub const KNOWN_LABELS: [&str; 4] = ["swarm", "time", "weather", "other"];

/// Keeps `[A-Za-z0-9._:-]`, replaces every other character with `_`, and cuts the
/// result to `MAX_FIELD_LEN` characters. An empty input becomes `fallback`.
pub fn sanitize_field(s: &str, fallback: &str) -> String {
    let out: String = s
        .chars()
        .take(MAX_FIELD_LEN)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() {
        fallback.to_string()
    } else {
        out
    }
}

/// Returns the label if the hub knows it, otherwise "other".
pub fn known_label(s: &str) -> &'static str {
    KNOWN_LABELS
        .iter()
        .copied()
        .find(|l| *l == s)
        .unwrap_or("other")
}

/// Which browser origins may open a websocket.
#[derive(Debug, Clone, PartialEq)]
pub enum OriginPolicy {
    Localhost,
    List(Vec<String>),
    Any,
}

impl OriginPolicy {
    /// `HUB_ALLOWED_ORIGINS`: unset means localhost only, `*` means any origin,
    /// otherwise a comma-separated list of exact origins.
    pub fn from_env_value(v: Option<&str>) -> Self {
        match v.map(str::trim) {
            None | Some("") => OriginPolicy::Localhost,
            Some("*") => OriginPolicy::Any,
            Some(list) => OriginPolicy::List(
                list.split(',')
                    .map(|o| o.trim().trim_end_matches('/').to_ascii_lowercase())
                    .filter(|o| !o.is_empty())
                    .collect(),
            ),
        }
    }

    /// A request without an Origin header comes from a non-browser client and is allowed.
    pub fn allows(&self, origin: Option<&str>) -> bool {
        let origin = match origin {
            None => return true,
            Some(o) => o.trim().trim_end_matches('/').to_ascii_lowercase(),
        };
        match self {
            OriginPolicy::Any => true,
            OriginPolicy::List(list) => list.contains(&origin),
            OriginPolicy::Localhost => is_localhost_origin(&origin),
        }
    }
}

fn is_localhost_origin(origin: &str) -> bool {
    let rest = match origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
    {
        Some(r) => r,
        None => return false,
    };
    if rest.contains('/') || rest.contains('@') {
        return false;
    }
    let host = if let Some(v6) = rest.strip_prefix('[') {
        match v6.split_once(']') {
            Some((h, port)) if port.is_empty() || port.starts_with(':') => {
                return h == "::1" && port_ok(port);
            }
            _ => return false,
        }
    } else {
        match rest.split_once(':') {
            Some((h, port)) => {
                if !port_ok(&format!(":{port}")) {
                    return false;
                }
                h
            }
            None => rest,
        }
    };
    host == "localhost" || host.ends_with(".localhost") || host == "127.0.0.1"
}

fn port_ok(p: &str) -> bool {
    p.is_empty() || (p.len() > 1 && p[1..].chars().all(|c| c.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_keeps_allowed_and_replaces_rest() {
        assert_eq!(sanitize_field("Peer_1.a:b-c", "x"), "Peer_1.a:b-c");
        assert_eq!(sanitize_field("a b\"<script>", "x"), "a_b__script_");
        assert_eq!(sanitize_field("org/Model q4", "x"), "org_Model_q4");
    }

    #[test]
    fn sanitize_caps_length_and_falls_back() {
        assert_eq!(sanitize_field(&"a".repeat(500), "x").len(), MAX_FIELD_LEN);
        assert_eq!(sanitize_field("", "unknown"), "unknown");
        assert_eq!(sanitize_field(&"é".repeat(100), "x").chars().count(), 64);
    }

    #[test]
    fn labels_outside_the_known_set_become_other() {
        assert_eq!(known_label("swarm"), "swarm");
        assert_eq!(known_label("weather"), "weather");
        assert_eq!(known_label("buy my product"), "other");
        assert_eq!(known_label(""), "other");
    }

    #[test]
    fn default_policy_is_localhost_only() {
        let p = OriginPolicy::from_env_value(None);
        assert!(p.allows(None));
        assert!(p.allows(Some("http://localhost:5173")));
        assert!(p.allows(Some("http://127.0.0.1:8080")));
        assert!(p.allows(Some("http://[::1]:3000")));
        assert!(p.allows(Some("http://tts.localhost:8000")));
        assert!(!p.allows(Some("https://evil.example")));
        assert!(!p.allows(Some("http://localhost.evil.example")));
        assert!(!p.allows(Some("http://localhost@evil.example")));
        assert!(!p.allows(Some("null")));
    }

    #[test]
    fn env_list_replaces_the_default() {
        let p = OriginPolicy::from_env_value(Some("https://ruche.world, https://hive.pages.dev/"));
        assert!(p.allows(Some("https://ruche.world")));
        assert!(p.allows(Some("https://HIVE.pages.dev")));
        assert!(!p.allows(Some("http://localhost:5173")));
        assert!(!p.allows(Some("https://ruche.world.evil.example")));
    }

    #[test]
    fn star_allows_any_and_empty_means_default() {
        assert!(OriginPolicy::from_env_value(Some("*")).allows(Some("https://x.example")));
        assert_eq!(
            OriginPolicy::from_env_value(Some("")),
            OriginPolicy::Localhost
        );
    }
}
