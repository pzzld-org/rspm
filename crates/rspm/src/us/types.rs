/*
    Appellation: types <module>
    Contrib: @FL03
*/
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A money value as the US API sends it: a decimal string and a currency code.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Amount {
    pub value: Decimal,
    pub currency: String,
}

/// Order intent for a request. Serialized as `ORDER_INTENT_*`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum UsOrderIntent {
    #[serde(rename = "ORDER_INTENT_BUY_LONG")]
    BuyLong,
    #[serde(rename = "ORDER_INTENT_SELL_LONG")]
    SellLong,
    #[serde(rename = "ORDER_INTENT_BUY_SHORT")]
    BuyShort,
    #[serde(rename = "ORDER_INTENT_SELL_SHORT")]
    SellShort,
}

/// Time in force for a request. Serialized as `TIME_IN_FORCE_*`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum UsTimeInForce {
    #[serde(rename = "TIME_IN_FORCE_GOOD_TILL_CANCEL")]
    GoodTillCancel,
    #[serde(rename = "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL")]
    ImmediateOrCancel,
    #[serde(rename = "TIME_IN_FORCE_FILL_OR_KILL")]
    FillOrKill,
}

/// Body of `POST /v1/orders`, and the inner `request` of a preview.
///
/// `quantity` is a JSON number. `max_block_time` is a JSON string.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderRequest {
    pub market_slug: String,
    pub r#type: &'static str,
    pub price: Amount,
    pub quantity: f64,
    pub tif: UsTimeInForce,
    pub intent: UsOrderIntent,
    pub participate_dont_initiate: bool,
    pub manual_order_indicator: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synchronous_execution: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_block_time: Option<String>,
}

/// Response of `POST /v1/orders`. `executions` is present only with synchronous execution.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderResponse {
    pub id: String,
    #[serde(default)]
    pub executions: Vec<UsExecution>,
}

/// An order as the US API reports it.
///
/// The OpenAPI lists no required fields on `Order`, and a preview has no
/// exchange-assigned `id` yet. An absent `id`, `market_slug` or `state`
/// deserializes as an empty string.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsOrder {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub market_slug: String,
    #[serde(default)]
    pub state: String,
    pub intent: Option<String>,
    pub tif: Option<String>,
    pub price: Option<Amount>,
    pub quantity: Option<f64>,
    #[serde(rename = "cumQuantity")]
    pub filled_quantity: Option<f64>,
    #[serde(rename = "leavesQuantity")]
    pub remaining_quantity: Option<f64>,
    #[serde(rename = "avgPx")]
    pub avg_price: Option<Amount>,
    /// The commission on all of the order's fills. Absent from older payloads.
    #[serde(rename = "commissionNotionalTotalCollected")]
    pub commission_total: Option<Amount>,
}

/// A single execution (fill, reject, cancel or replace) on an order.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsExecution {
    pub id: String,
    pub order: UsOrder,
    pub r#type: String,
    #[serde(rename = "lastShares")]
    pub fill_quantity: Option<Decimal>,
    #[serde(rename = "lastPx")]
    pub fill_price: Option<Amount>,
    pub trade_id: Option<String>,
    #[serde(rename = "aggressor")]
    pub is_taker: Option<bool>,
    pub order_reject_reason: Option<String>,
    pub text: Option<String>,
    #[serde(rename = "transactTime")]
    pub executed_at: Option<String>,
    #[serde(rename = "commissionNotionalCollected")]
    pub commission: Option<Amount>,
}

/// One market position from `GET /v1/portfolio/positions`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsUserPosition {
    pub net_position_decimal: Decimal,
    pub qty_available_decimal: Option<Decimal>,
    pub cost: Option<Amount>,
    pub expired: Option<bool>,
}

/// A page of positions, keyed by market slug.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsPositionsPage {
    #[serde(default)]
    pub positions: BTreeMap<String, UsUserPosition>,
    pub next_cursor: Option<String>,
    pub eof: Option<bool>,
}

/// Filters for `GET /v1/portfolio/activities`. Every one is optional.
#[derive(Clone, Debug, Default)]
pub struct UsActivitiesQuery {
    pub types: Vec<UsActivityType>,
    pub market_slug: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub sort_order: Option<UsSortOrder>,
}

/// An activity type filter, as its wire value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsActivityType {
    Trade,
}

impl UsActivityType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trade => "ACTIVITY_TYPE_TRADE",
        }
    }
}

/// Activity order by timestamp. The venue's default is newest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsSortOrder {
    Descending,
    Ascending,
}

impl UsSortOrder {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Descending => "SORT_ORDER_DESCENDING",
            Self::Ascending => "SORT_ORDER_ASCENDING",
        }
    }
}

/// A page of activities.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsActivitiesPage {
    #[serde(default)]
    pub activities: Vec<UsActivity>,
    pub next_cursor: Option<String>,
    pub eof: Option<bool>,
}

/// One activity. Only a trade activity's body is parsed.
#[derive(Clone, Debug, Deserialize)]
pub struct UsActivity {
    pub trade: Option<UsTradeActivity>,
}

/// A trade activity. It names no order, intent or fee.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsTradeActivity {
    /// The exchange-assigned trade id.
    pub id: String,
    pub market_slug: Option<String>,
    pub state: Option<UsTradeState>,
    #[serde(rename = "createTime")]
    pub created_at: Option<String>,
    pub price: Option<Amount>,
    #[serde(rename = "qtyDecimal")]
    pub quantity: Option<Decimal>,
    #[serde(rename = "isAggressor")]
    pub is_taker: Option<bool>,
}

/// A trade's state. Every state but these three is `Other`.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub enum UsTradeState {
    #[serde(rename = "TRADE_STATE_CLEARED")]
    Cleared,
    #[serde(rename = "TRADE_STATE_BUSTED")]
    Busted,
    #[serde(rename = "TRADE_STATE_REJECTED")]
    Rejected,
    #[serde(other)]
    Other,
}

/// One currency balance. Money fields are JSON numbers.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsBalance {
    pub current_balance: f64,
    pub buying_power: f64,
    pub currency: String,
}

/// One side of a binary market. `long` is true for the Yes side.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsMarketSide {
    pub id: String,
    pub long: bool,
    pub tradable: Option<bool>,
}

/// The asset reference inside `assetPriceTerms`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsAssetRef {
    pub symbol: String,
}

/// Asset-price terms of an Up/Down market: asset, horizon, window and strike.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsAssetPriceTerms {
    pub market_type: String,
    pub asset: UsAssetRef,
    pub horizon: String,
    /// RFC 3339 UTC.
    pub window_start: String,
    /// RFC 3339 UTC.
    pub window_end: String,
    pub price_to_beat: Option<Amount>,
    pub index_symbol: Option<String>,
    pub settlement_price: Option<Amount>,
}

/// A market from the gateway.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsMarket {
    pub id: String,
    pub slug: String,
    pub status: String,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub order_price_min_tick_size: Option<f64>,
    pub minimum_trade_qty: Option<f64>,
    pub fee_coefficient: Option<f64>,
    pub end_date: Option<String>,
    #[serde(default)]
    pub market_sides: Vec<UsMarketSide>,
    pub asset_price_terms: Option<UsAssetPriceTerms>,
}

/// One price level in a book. `qty` is a JSON string.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsBookLevel {
    pub px: Amount,
    pub qty: Decimal,
}

/// Book snapshot for one market. Bids are sorted best first.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsMarketData {
    pub market_slug: String,
    #[serde(default)]
    pub bids: Vec<UsBookLevel>,
    #[serde(default)]
    pub offers: Vec<UsBookLevel>,
    pub state: Option<String>,
    pub transact_time: Option<String>,
}

/// Filters for `GET /v1/markets`. `None` fields are omitted from the query.
#[derive(Clone, Debug, Default)]
pub struct UsMarketsQuery {
    pub categories: Vec<String>,
    pub closed: Option<bool>,
    pub end_date_min: Option<String>,
    pub end_date_max: Option<String>,
    pub limit: Option<u32>,
}
