/*
    Appellation: fill <module>
    Created At: 2026.03.17:19:46:41
    Contrib: @FL03
*/
use crate::fees::{FeeSchedule, Fees, Liquidity, PowerFeeError};
use crate::{ClobSide, Side};
use alloc::string::String;
use core::fmt;

/// An order-execution result for one action on one prediction-market outcome token.
///
/// `outcome` and `action` are independent: YES/NO identifies the token, while
/// BUY/SELL identifies what the order did to that token. All four combinations
/// are valid.
///
/// A fill records transaction `notional`, not realized profit or loss. In
/// particular, a SELL fill's PnL requires the position's cost basis and cannot
/// be inferred from the fill alone.
///
/// `Fill` intentionally has no [`Default`] implementation: callers must name
/// both axes at construction time.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "snake_case")
)]
#[repr(C)]
pub struct Fill {
    /// Exchange-assigned order identifier.
    pub order_id: String,
    /// Order status string returned by the exchange (e.g., `"filled"`, `"cancelled"`).
    pub status: String,
    /// Asset or market slug this fill is for.
    pub asset: String,
    /// Outcome token traded: YES or NO.
    pub outcome: Side,
    /// Trade action applied to the outcome token: BUY or SELL.
    pub action: ClobSide,
    /// Fill price per token.
    pub price: f64,
    /// Number of tokens filled.
    pub size: f64,
    /// Direction-neutral fill notional in USD (`price × size`).
    ///
    /// This is cash paid before fees for a BUY and gross proceeds before fees
    /// for a SELL. It is not profit or loss.
    pub notional: f64,
    /// Unix timestamp (seconds) when the fill was reported.
    pub timestamp: i64,
}

impl Fill {
    /// Construct a new [`Fill`] from its constituent fields.
    ///
    /// `notional` is computed automatically as `price × size`. `outcome` and
    /// `action` must both be supplied because neither can be inferred from the
    /// other.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rspm::types::{ClobSide, Fill, Side};
    ///
    /// let fill = Fill::new(
    ///     "ord-001",
    ///     "filled",
    ///     "btc-sprint",
    ///     Side::No,
    ///     ClobSide::Buy,
    ///     0.40,
    ///     20.0,
    ///     1_700_000_000,
    /// );
    /// assert_eq!(fill.outcome, Side::No);
    /// assert_eq!(fill.action, ClobSide::Buy);
    /// assert_eq!(fill.notional, 8.0);
    /// ```
    // All non-derived fields are required, and both typed axes must stay in
    // the signature so neither can be inferred from the other.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        order_id: impl Into<String>,
        status: impl Into<String>,
        asset: impl Into<String>,
        outcome: Side,
        action: ClobSide,
        price: f64,
        size: f64,
        timestamp: i64,
    ) -> Self {
        Self {
            order_id: order_id.into(),
            status: status.into(),
            asset: asset.into(),
            outcome,
            action,
            price,
            size,
            notional: price * size,
            timestamp,
        }
    }

    /// Returns `true` when the fill status is `"filled"` (fully matched).
    pub fn is_filled(&self) -> bool {
        self.status.eq_ignore_ascii_case("filled")
    }

    /// Returns `true` when this fill traded the YES outcome token.
    pub fn is_yes(&self) -> bool {
        self.outcome == Side::Yes
    }

    /// Returns `true` when this fill traded the NO outcome token.
    pub fn is_no(&self) -> bool {
        self.outcome == Side::No
    }

    /// This fill's taker fee under `fees` (for example [`crate::fees::PmusFees`]), rounded as the
    /// venue rounds it. An empty fill pays nothing. This is a transaction fee, not a
    /// position-level PnL calculation.
    ///
    /// # Errors
    ///
    /// A price outside `[0, 1]`, or arithmetic that overflows.
    pub fn fee<S: FeeSchedule>(&self, fees: &Fees<f64, S>) -> Result<f64, PowerFeeError> {
        if self.size <= 0.0 {
            return Ok(0.0);
        }
        fees.fee(Liquidity::Taker, self.size, self.price)
    }
}

impl fmt::Display for Fill {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Fill(id={id} asset={asset} outcome={outcome} action={action} p={price:.4} sz={size:.2} notional={notional:.2} [{status}])",
            id = self.order_id,
            asset = self.asset,
            outcome = self.outcome,
            action = self.action,
            price = self.price,
            size = self.size,
            notional = self.notional,
            status = self.status,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString as _;

    // [REGRESSION] A default would silently collapse the two required axes to
    // YES + BUY instead of making callers choose both values.
    static_assertions::assert_not_impl_any!(Fill: Default);

    fn sample_fill() -> Fill {
        Fill::new(
            "ord-001",
            "filled",
            "btc-sprint",
            Side::Yes,
            ClobSide::Buy,
            0.60,
            20.0,
            1_700_000_000,
        )
    }

    fn fill_with_axes(outcome: Side, action: ClobSide) -> Fill {
        Fill::new(
            "ord-axis",
            "filled",
            "btc-sprint",
            outcome,
            action,
            0.50,
            10.0,
            1_700_000_000,
        )
    }

    #[test]
    fn notional_is_price_times_size_for_either_action() {
        let buy = sample_fill();
        let sell = fill_with_axes(Side::Yes, ClobSide::Sell);

        assert!((buy.notional - 12.0).abs() < 1e-9);
        assert!((sell.notional - 5.0).abs() < 1e-9);
    }

    #[test]
    fn is_filled_check() {
        let fill = sample_fill();
        assert!(fill.is_filled());

        let partial = Fill::new(
            "ord-002",
            "partial",
            "btc-sprint",
            Side::Yes,
            ClobSide::Buy,
            0.55,
            5.0,
            0,
        );
        assert!(!partial.is_filled());
    }

    #[test]
    fn outcome_flags_do_not_claim_a_trade_action() {
        let yes_sell = fill_with_axes(Side::Yes, ClobSide::Sell);
        assert!(yes_sell.is_yes());
        assert!(!yes_sell.is_no());
        assert_eq!(yes_sell.action, ClobSide::Sell);

        let no_buy = fill_with_axes(Side::No, ClobSide::Buy);
        assert!(no_buy.is_no());
        assert!(!no_buy.is_yes());
        assert_eq!(no_buy.action, ClobSide::Buy);
    }

    /// [REGRESSION][EVAL] BUY NO and SELL YES must remain distinct, valid
    /// fills; neither axis may be derived from or overwrite the other.
    #[test]
    fn outcome_and_action_form_the_full_cartesian_product() {
        let fills = [
            fill_with_axes(Side::Yes, ClobSide::Buy),
            fill_with_axes(Side::Yes, ClobSide::Sell),
            fill_with_axes(Side::No, ClobSide::Buy),
            fill_with_axes(Side::No, ClobSide::Sell),
        ];
        let axes = fills.each_ref().map(|fill| (fill.outcome, fill.action));

        assert_eq!(
            axes,
            [
                (Side::Yes, ClobSide::Buy),
                (Side::Yes, ClobSide::Sell),
                (Side::No, ClobSide::Buy),
                (Side::No, ClobSide::Sell),
            ]
        );

        let buy_no = &fills[2];
        assert!(buy_no.is_no());
        assert_eq!(buy_no.action, ClobSide::Buy);

        let sell_yes = &fills[1];
        assert!(sell_yes.is_yes());
        assert_eq!(sell_yes.action, ClobSide::Sell);
    }

    #[test]
    fn fee_is_the_venue_taker_fee_for_either_action() {
        let fees = crate::fees::Pmus::API.to_f64().unwrap();
        let buy = fill_with_axes(Side::No, ClobSide::Buy);
        let sell = fill_with_axes(Side::No, ClobSide::Sell);

        // 0.0695 × 10 × 0.5 × 0.5 = 0.17375 → 0.17.
        assert_eq!(buy.fee(&fees), Ok(0.17));
        assert_eq!(buy.fee(&fees), sell.fee(&fees));
    }

    #[test]
    fn display_names_both_axes() {
        assert_eq!(
            sample_fill().to_string(),
            "Fill(id=ord-001 asset=btc-sprint outcome=YES action=BUY p=0.6000 sz=20.00 notional=12.00 [filled])"
        );

        let sell_yes = fill_with_axes(Side::Yes, ClobSide::Sell).to_string();
        assert!(sell_yes.contains("outcome=YES"));
        assert!(sell_yes.contains("action=SELL"));
    }

    #[cfg(feature = "json")]
    #[test]
    fn serde_preserves_outcome_and_action_independently() {
        let buy_no = serde_json::to_value(fill_with_axes(Side::No, ClobSide::Buy))
            .expect("BUY NO fill should serialize");
        assert_eq!(buy_no["outcome"], "NO");
        assert_eq!(buy_no["action"], "BUY");

        let sell_yes = serde_json::to_value(fill_with_axes(Side::Yes, ClobSide::Sell))
            .expect("SELL YES fill should serialize");
        assert_eq!(sell_yes["outcome"], "YES");
        assert_eq!(sell_yes["action"], "SELL");
    }
}
