use crate::orderbook::Outcome;
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashSet, path::Path, process::Command};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue};

pub const DEFAULT_WS: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const DEFAULT_REST: &str = "https://external-api.kalshi.com/trade-api/v2";

#[derive(Debug)]
pub enum BookEvent {
    Invalidate(Vec<String>),
    Snapshot {
        symbol: String,
        yes: Vec<(String, String)>,
        no: Vec<(String, String)>,
        received: DateTime<Utc>,
        exchange: DateTime<Utc>,
    },
    Delta {
        symbol: String,
        outcome: Outcome,
        price: i64,
        delta: i64,
        received: DateTime<Utc>,
        exchange: DateTime<Utc>,
    },
}

fn millis(ms: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(ms).context("invalid millisecond timestamp")
}
fn message_time(value: &Value, key: &str) -> Option<DateTime<Utc>> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

/// Parse only the orderbook messages. Control messages are handled by the worker.
pub fn parse_message(value: &Value) -> Result<Option<BookEvent>> {
    let received = Utc::now();
    let kind = value["type"].as_str().unwrap_or("");
    if kind != "orderbook_snapshot" && kind != "orderbook_delta" {
        return Ok(None);
    }
    let msg = &value["msg"];
    let symbol = msg["market_ticker"]
        .as_str()
        .context("missing market_ticker")?
        .to_string();
    let exchange = message_time(msg, "ts_ms")
        .or_else(|| message_time(value, "sending_ts_ms"))
        .unwrap_or(received);
    match kind {
        "orderbook_snapshot" => {
            fn price_levels(value: &Value) -> Result<Vec<(String, String)>> {
                let array = match value.as_array() {
                    Some(array) => array,
                    None if value.is_null() => return Ok(Vec::new()),
                    None => bail!("snapshot side is not an array"),
                };
                array
                    .iter()
                    .map(|row| {
                        let level = row.as_array().context("snapshot level not an array")?;
                        if level.len() != 2 {
                            bail!("snapshot level must have price and size");
                        }
                        Ok((
                            level[0].as_str().context("price not a string")?.to_owned(),
                            level[1].as_str().context("size not a string")?.to_owned(),
                        ))
                    })
                    .collect()
            }
            let yes = price_levels(&msg["yes_dollars_fp"])?;
            let no = price_levels(&msg["no_dollars_fp"])?;
            Ok(Some(BookEvent::Snapshot {
                symbol,
                yes,
                no,
                received,
                exchange,
            }))
        }
        "orderbook_delta" => {
            let outcome = Outcome::parse(msg["side"].as_str().context("missing side")?)?;
            let price = crate::orderbook::fixed(
                msg["price_dollars"]
                    .as_str()
                    .context("missing price_dollars")?,
            )?;
            let delta =
                crate::orderbook::fixed(msg["delta_fp"].as_str().context("missing delta_fp")?)?;
            Ok(Some(BookEvent::Delta {
                symbol,
                outcome,
                price,
                delta,
                received,
                exchange,
            }))
        }
        _ => unreachable!(),
    }
}

#[derive(Deserialize)]
struct MarketPage {
    markets: Vec<Market>,
    cursor: String,
}
#[derive(Deserialize)]
struct Market {
    ticker: String,
}

/// Returns all currently open non-MVE markets. An error leaves the caller's prior list untouched.

pub async fn live_markets(rest: &str, client: &reqwest::Client) -> Result<Vec<String>> {
    let max_markets = std::env::var("TEST_MAX_MARKETS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0);

    let page_limit = max_markets.unwrap_or(1000).min(1000).to_string();

    let mut cursor = String::new();
    let mut all = HashSet::new();

    loop {
        let mut request = client
            .get(format!("{}/markets", rest.trim_end_matches('/')))
            .query(&[
                ("status", "open"),
                ("mve_filter", "exclude"),
                ("limit", page_limit.as_str()),
            ]);

        if !cursor.is_empty() {
            request = request.query(&[("cursor", cursor.as_str())]);
        }

        let page: MarketPage = request.send().await?.error_for_status()?.json().await?;

        for market in page.markets {
            all.insert(market.ticker);

            if max_markets.is_some_and(|max| all.len() >= max) {
                break;
            }
        }

        // In test mode, stop once we have enough markets.
        if max_markets.is_some_and(|max| all.len() >= max) {
            break;
        }

        if page.cursor.is_empty() {
            break;
        }

        if page.cursor == cursor {
            bail!("market pagination made no progress");
        }

        cursor = page.cursor;
    }

    let mut names: Vec<String> = all.into_iter().collect();
    names.sort();

    eprintln!("DEBUG: market discovery completed: {} markets", names.len());

    Ok(names)
}

#[derive(Clone)]
pub struct Credentials {
    pub key_id: String,
    pub private_key: String,
}

impl Credentials {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            key_id: std::env::var("KALSHI_API_KEY_ID").context("set KALSHI_API_KEY_ID")?,
            private_key: std::env::var("KALSHI_PRIVATE_KEY_PATH")
                .context("set KALSHI_PRIVATE_KEY_PATH")?,
        })
    }

    /// Uses the system OpenSSL binary to sign with Ed25519 or RSA-PSS without
    /// bringing another cryptographic implementation into the project.
    fn sign(&self, preimage: &str) -> Result<String> {
        let private_key = Path::new(&self.private_key);
        if !private_key.is_file() {
            bail!("private key file does not exist");
        }
        let key_info = Command::new("openssl")
            .args(["pkey", "-in"])
            .arg(private_key)
            .args(["-text", "-noout"])
            .output()
            .context("run openssl pkey (install openssl)")?;
        if !key_info.status.success() {
            bail!("openssl could not load the private key");
        }
        let is_ed25519 = String::from_utf8_lossy(&key_info.stdout)
            .to_uppercase()
            .contains("ED25519");
        let input = tempfile::NamedTempFile::new()?;
        std::fs::write(input.path(), preimage.as_bytes())?;
        let output = tempfile::NamedTempFile::new()?;
        let mut command = Command::new("openssl");
        if is_ed25519 {
            command
                .args(["pkeyutl", "-sign", "-rawin", "-inkey"])
                .arg(private_key)
                .arg("-in")
                .arg(input.path())
                .arg("-out")
                .arg(output.path());
        } else {
            command
                .args(["dgst", "-sha256", "-sign"])
                .arg(private_key)
                .args([
                    "-sigopt",
                    "rsa_padding_mode:pss",
                    "-sigopt",
                    "rsa_pss_saltlen:digest",
                    "-sigopt",
                    "rsa_mgf1_md:sha256",
                    "-out",
                ])
                .arg(output.path())
                .arg(input.path());
        }
        let result = command
            .output()
            .context("OpenSSL signing failed to launch")?;
        if !result.status.success() {
            bail!(
                "OpenSSL signing failed: {}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
        Ok(STANDARD.encode(std::fs::read(output.path())?))
    }

    pub fn websocket_request(
        &self,
        ws_url: &str,
    ) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signature = self.sign(&format!("{timestamp}GET/trade-api/ws/v2"))?;
        let mut request = ws_url.into_client_request()?;
        request
            .headers_mut()
            .insert("KALSHI-ACCESS-KEY", HeaderValue::from_str(&self.key_id)?);
        request.headers_mut().insert(
            "KALSHI-ACCESS-TIMESTAMP",
            HeaderValue::from_str(&timestamp)?,
        );
        request.headers_mut().insert(
            "KALSHI-ACCESS-SIGNATURE",
            HeaderValue::from_str(&signature)?,
        );
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn snapshot_allows_missing_or_null_sides() {
        let no_empty = serde_json::json!({
            "type":"orderbook_snapshot",
            "msg":{"market_ticker":"TEST", "yes_dollars_fp":[["0.40","2.00"]]}
        });
        match parse_message(&no_empty).unwrap() {
            Some(BookEvent::Snapshot { yes, no, .. }) => {
                assert_eq!(yes.len(), 1);
                assert!(no.is_empty());
            }
            _ => panic!("expected snapshot"),
        }

        let both_empty = serde_json::json!({
            "type":"orderbook_snapshot",
            "msg":{"market_ticker":"TEST", "yes_dollars_fp": null}
        });
        match parse_message(&both_empty).unwrap() {
            Some(BookEvent::Snapshot { yes, no, .. }) => {
                assert!(yes.is_empty());
                assert!(no.is_empty());
            }
            _ => panic!("expected snapshot"),
        }

        let bad_side = serde_json::json!({
            "type":"orderbook_snapshot",
            "msg":{"market_ticker":"TEST", "no_dollars_fp":"invalid"}
        });
        assert!(parse_message(&bad_side).is_err());
    }

    #[test]
    fn kalshi_snapshot_and_delta() {
        let snapshot = serde_json::json!({"type":"orderbook_snapshot", "sending_ts_ms":1669149841234_i64,
            "msg":{"market_ticker":"TEST","yes_dollars_fp":[["0.4","20.00"]],"no_dollars_fp":[["0.55","10.00"]]}});
        let delta = serde_json::json!({"type":"orderbook_delta","msg":{"market_ticker":"TEST", "side":"no","price_dollars":"0.55","delta_fp":"-2.00"}});
        assert!(matches!(
            parse_message(&snapshot).unwrap(),
            Some(BookEvent::Snapshot { .. })
        ));
        assert!(matches!(
            parse_message(&delta).unwrap(),
            Some(BookEvent::Delta { .. })
        ));
    }
}
