/*
    Appellation: fees <module>
    Contrib: @FL03
*/
//! Polymarket platform fees: the one fee formula every consumer reads.
//!
//! A market's metadata supplies the fee coefficient (`fd.r`, or the US `feeCoefficient`) and its
//! power exponent (`fd.e`). A taker pays, per share, at the fill price `p`:
//!
//! ```text
//! fee per share = rate × (p × (1 − p))^exponent
//! fee           = shares × fee per share, rounded to 5 decimals
//! ```
//!
//! A maker pays nothing, and redemption at settlement is free. The fee comes out of the money
//! that crosses the book, never from outside it: a buy's bid pays the shares and their fee
//! ([`Fees::shares_for_bid`]), and a sale's proceeds are its notional less the fee
//! ([`Fees::proceeds`]), so a trade never loses more than its bid.
//!
//! [`Fees`] is generic over the number: [`Decimal`] for money that settles, `f64` for model
//! arithmetic (sizing, edge gates). Both run the same formula.

use rust_decimal::{Decimal, RoundingStrategy};

/// Decimal places used by the current Polymarket platform-fee schedule.
pub const POLYMARKET_FEE_PRECISION: u32 = 5;

/// Failure returned by exact power-curve fee arithmetic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PowerFeeError {
    NonPositiveShares,
    PriceOutOfBounds,
    NegativeRate,
    ArithmeticOverflow,
    FloatConversion,
}

impl core::fmt::Display for PowerFeeError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::NonPositiveShares => "shares must be greater than zero",
            Self::PriceOutOfBounds => "price must be within the inclusive range [0, 1]",
            Self::NegativeRate => "fee rate must be nonnegative",
            Self::ArithmeticOverflow => "fee arithmetic overflowed Decimal",
            Self::FloatConversion => "f64 compatibility conversion failed",
        })
    }
}

impl core::error::Error for PowerFeeError {}

const CURRENT_CRYPTO_RATE: Decimal = Decimal::from_parts(7, 0, 0, false, 2);
const CURRENT_CRYPTO_EXPONENT: u32 = 1;

/// Sprint window label used by market-discovery callers.
///
/// Fee parameters are not selected from this label. They come from each
/// market's `fd.r` and `fd.e` metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum WindowKind {
    /// 5-minute sprint window.
    W5m,
    /// 15-minute sprint window.
    W15m,
    /// 4-hour sprint window.
    W4h,
}

/// Which side of the book a fill takes: the taker pays the fee, the maker pays nothing.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(rename_all = "snake_case")
)]
pub enum Liquidity {
    /// Crosses the book (a market or FAK/FOK order, or a limit that matches at once).
    #[default]
    Taker,
    /// Rests on the book and is matched later.
    Maker,
}

/// A number the fee formula runs on: [`Decimal`] or `f64`.
pub trait FeeScalar: Copy + PartialOrd {
    const ZERO: Self;
    const ONE: Self;
    /// The smallest fee the venue charges, `0.00001`.
    fn fee_tick() -> Self;
    fn checked_add(self, other: Self) -> Option<Self>;
    fn checked_sub(self, other: Self) -> Option<Self>;
    fn checked_mul(self, other: Self) -> Option<Self>;
    fn checked_div(self, other: Self) -> Option<Self>;
    /// Rounds a fee amount to [`POLYMARKET_FEE_PRECISION`] places, midpoint away from zero.
    fn round_fee(self) -> Self;
}

impl FeeScalar for Decimal {
    const ZERO: Self = Decimal::ZERO;
    const ONE: Self = Decimal::ONE;

    fn fee_tick() -> Self {
        Decimal::new(1, POLYMARKET_FEE_PRECISION)
    }
    fn checked_add(self, other: Self) -> Option<Self> {
        Decimal::checked_add(self, other)
    }
    fn checked_sub(self, other: Self) -> Option<Self> {
        Decimal::checked_sub(self, other)
    }
    fn checked_mul(self, other: Self) -> Option<Self> {
        Decimal::checked_mul(self, other)
    }
    fn checked_div(self, other: Self) -> Option<Self> {
        Decimal::checked_div(self, other)
    }
    fn round_fee(self) -> Self {
        self.round_dp_with_strategy(
            POLYMARKET_FEE_PRECISION,
            RoundingStrategy::MidpointAwayFromZero,
        )
    }
}

impl FeeScalar for f64 {
    const ZERO: Self = 0.0;
    const ONE: Self = 1.0;

    fn fee_tick() -> Self {
        1e-5
    }
    fn checked_add(self, other: Self) -> Option<Self> {
        Some(self + other).filter(|value| value.is_finite())
    }
    fn checked_sub(self, other: Self) -> Option<Self> {
        Some(self - other).filter(|value| value.is_finite())
    }
    fn checked_mul(self, other: Self) -> Option<Self> {
        Some(self * other).filter(|value| value.is_finite())
    }
    fn checked_div(self, other: Self) -> Option<Self> {
        Some(self / other).filter(|value| value.is_finite())
    }
    fn round_fee(self) -> Self {
        // `f64::round` rounds half away from zero, like the Decimal path.
        (self * 1e5).round() / 1e5
    }
}

/// A market's platform-fee schedule: `rate × (p × (1 − p))^exponent` per taker share.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(rename_all = "snake_case")
)]
pub struct Fees<T = Decimal> {
    #[cfg_attr(feature = "serde", serde(rename = "fee_rate"))]
    rate: T,
    #[cfg_attr(feature = "serde", serde(rename = "fee_exponent"))]
    exponent: u32,
}

/// The exact schedule, as market metadata states it.
pub type FeeCalculator = Fees<Decimal>;

// The negated comparisons are deliberate: a NaN `f64` fails every check, so it is refused.
#[allow(clippy::neg_cmp_op_on_partial_ord)]
impl<T: FeeScalar> Fees<T> {
    /// The schedule from a market's metadata: `fd.r` (or the US `feeCoefficient`) and `fd.e`.
    ///
    /// # Errors
    ///
    /// Returns [`PowerFeeError::NegativeRate`] when `rate` is negative (or not a number).
    pub fn new(rate: T, exponent: u32) -> Result<Self, PowerFeeError> {
        if !(rate >= T::ZERO) {
            return Err(PowerFeeError::NegativeRate);
        }
        Ok(Self { rate, exponent })
    }

    /// The fee-rate coefficient.
    pub const fn rate(&self) -> T {
        self.rate
    }

    /// The power exponent.
    pub const fn exponent(&self) -> u32 {
        self.exponent
    }

    /// Whether the schedule charges nothing at any price.
    pub fn is_free(&self) -> bool {
        self.rate == T::ZERO
    }

    /// The fee on one share at `price`, unrounded: `rate × (p × (1 − p))^exponent` for a
    /// taker, zero for a maker.
    ///
    /// # Errors
    ///
    /// A `price` outside `[0, 1]`, or arithmetic that overflows.
    pub fn per_share(&self, liquidity: Liquidity, price: T) -> Result<T, PowerFeeError> {
        if !(T::ZERO <= price && price <= T::ONE) {
            return Err(PowerFeeError::PriceOutOfBounds);
        }
        if liquidity == Liquidity::Maker || self.is_free() {
            return Ok(T::ZERO);
        }
        let base = T::ONE
            .checked_sub(price)
            .and_then(|complement| price.checked_mul(complement))
            .ok_or(PowerFeeError::ArithmeticOverflow)?;
        pow(base, self.exponent)
            .and_then(|curve| self.rate.checked_mul(curve))
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }

    /// The fee on `shares` filled at `price`, rounded to five decimals as the venue charges it.
    ///
    /// # Errors
    ///
    /// Non-positive `shares`, a `price` outside `[0, 1]`, or arithmetic that overflows.
    pub fn fee(&self, liquidity: Liquidity, shares: T, price: T) -> Result<T, PowerFeeError> {
        if !(shares > T::ZERO) {
            return Err(PowerFeeError::NonPositiveShares);
        }
        self.per_share(liquidity, price)?
            .checked_mul(shares)
            .map(T::round_fee)
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }

    /// What a buy of `shares` at `price` takes from the bid: notional plus fee.
    ///
    /// # Errors
    ///
    /// As [`Fees::fee`].
    pub fn cost(&self, liquidity: Liquidity, shares: T, price: T) -> Result<T, PowerFeeError> {
        let fee = self.fee(liquidity, shares, price)?;
        shares
            .checked_mul(price)
            .and_then(|notional| notional.checked_add(fee))
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }

    /// What a sale of `shares` at `price` pays out: notional less fee.
    ///
    /// # Errors
    ///
    /// As [`Fees::fee`].
    pub fn proceeds(&self, liquidity: Liquidity, shares: T, price: T) -> Result<T, PowerFeeError> {
        let fee = self.fee(liquidity, shares, price)?;
        shares
            .checked_mul(price)
            .and_then(|notional| notional.checked_sub(fee))
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }

    /// The shares a `bid` buys at `price` with the fee taken out of the bid, so that
    /// [`Fees::cost`] never exceeds it:
    ///
    /// ```text
    /// shares = (bid − tick) / (p + rate × (p × (1 − p))^exponent)
    /// ```
    ///
    /// One fee tick is held back because the fee rounds to five decimals. A fill at a lower price
    /// costs less per share (`d/dp [p + r·p(1−p)] = 1 + r(1 − 2p) > 0` for `r < 1`), and a venue
    /// grid only floors shares, so neither can push the spend past the bid. A maker leg, or a free
    /// schedule, buys exactly `bid / price`.
    ///
    /// # Errors
    ///
    /// A non-positive `bid` (or one no larger than the tick), a `price` outside `(0, 1]`, or
    /// arithmetic that overflows.
    pub fn shares_for_bid(
        &self,
        liquidity: Liquidity,
        bid: T,
        price: T,
    ) -> Result<T, PowerFeeError> {
        if !(price > T::ZERO) {
            return Err(PowerFeeError::PriceOutOfBounds);
        }
        let per_share = self.per_share(liquidity, price)?;
        let spendable = if per_share == T::ZERO {
            bid
        } else {
            bid.checked_sub(T::fee_tick())
                .ok_or(PowerFeeError::ArithmeticOverflow)?
        };
        if !(spendable > T::ZERO) {
            return Err(PowerFeeError::NonPositiveShares);
        }
        price
            .checked_add(per_share)
            .and_then(|all_in| spendable.checked_div(all_in))
            .filter(|shares| *shares > T::ZERO)
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }
}

impl Fees<Decimal> {
    /// Alias of [`Fees::new`] under the metadata names.
    ///
    /// # Errors
    ///
    /// As [`Fees::new`].
    pub fn from_market_metadata(rate: Decimal, exponent: u32) -> Result<Self, PowerFeeError> {
        Self::new(rate, exponent)
    }

    /// The current Crypto schedule: rate `0.07`, exponent `1`. Metadata stays authoritative
    /// for live markets.
    pub const fn crypto() -> Self {
        Self {
            rate: CURRENT_CRYPTO_RATE,
            exponent: CURRENT_CRYPTO_EXPONENT,
        }
    }

    /// The same schedule over `f64`, for model arithmetic.
    ///
    /// # Errors
    ///
    /// [`PowerFeeError::FloatConversion`] when the rate has no finite `f64`.
    pub fn to_f64(&self) -> Result<Fees<f64>, PowerFeeError> {
        let rate = f64::try_from(self.rate).map_err(|_| PowerFeeError::FloatConversion)?;
        Fees::new(rate, self.exponent)
    }

    /// The V2 `fd.r` coefficient.
    pub const fn fee_rate(&self) -> Decimal {
        self.rate
    }

    /// The V2 `fd.e` exponent.
    pub const fn fee_exponent(&self) -> u32 {
        self.exponent
    }

    /// The taker fee on `shares` at `price`, rounded to five decimals.
    ///
    /// # Errors
    ///
    /// As [`Fees::fee`].
    pub fn taker_fee(&self, shares: Decimal, price: Decimal) -> Result<Decimal, PowerFeeError> {
        self.fee(Liquidity::Taker, shares, price)
    }

    /// The maker fee, exactly zero.
    pub const fn maker_fee(&self) -> Decimal {
        Decimal::ZERO
    }

    /// [`Fees::taker_fee`] for callers that still hold `f64`: the inputs become [`Decimal`]
    /// before the formula runs.
    ///
    /// # Errors
    ///
    /// [`PowerFeeError::FloatConversion`] on a non-finite input, else as [`Fees::fee`].
    pub fn taker_fee_f64(&self, shares: f64, price: f64) -> Result<f64, PowerFeeError> {
        let shares = Decimal::try_from(shares).map_err(|_| PowerFeeError::FloatConversion)?;
        let price = Decimal::try_from(price).map_err(|_| PowerFeeError::FloatConversion)?;
        let fee = self.taker_fee(shares, price)?;
        f64::try_from(fee).map_err(|_| PowerFeeError::FloatConversion)
    }
}

fn pow<T: FeeScalar>(mut base: T, mut exponent: u32) -> Option<T> {
    let mut product = T::ONE;
    while exponent > 0 {
        if exponent & 1 == 1 {
            product = product.checked_mul(base)?;
        }
        exponent >>= 1;
        if exponent > 0 {
            base = base.checked_mul(base)?;
        }
    }
    Some(product)
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn metadata_rate_and_exponent_drive_taker_fee() {
        let calculator = FeeCalculator::from_market_metadata(Decimal::new(7, 2), 2)
            .expect("fd.r=0.07 and fd.e=2 are valid");

        let fee = calculator
            .taker_fee(Decimal::ONE_HUNDRED, Decimal::new(5, 1))
            .expect("official midpoint inputs are valid");

        assert_eq!(calculator.fee_rate(), Decimal::new(7, 2));
        assert_eq!(calculator.fee_exponent(), 2);
        assert_eq!(fee, Decimal::new(4375, 4));
    }

    #[test]
    fn current_crypto_helper_matches_official_anchor() {
        let calculator = FeeCalculator::crypto();

        let fee = calculator
            .taker_fee(Decimal::ONE_HUNDRED, Decimal::new(5, 1))
            .expect("official midpoint inputs are valid");

        assert_eq!(calculator.fee_rate(), Decimal::new(7, 2));
        assert_eq!(calculator.fee_exponent(), 1);
        assert_eq!(fee, Decimal::new(175, 2));
    }

    #[test]
    fn maker_fee_is_exactly_zero() {
        let calculator = FeeCalculator::from_market_metadata(Decimal::new(7, 2), 2)
            .expect("fd.r=0.07 and fd.e=2 are valid");

        assert_eq!(calculator.maker_fee(), Decimal::ZERO);
    }

    #[test]
    fn endpoints_return_exact_zero() {
        let calculator = FeeCalculator::crypto();

        assert_eq!(
            calculator
                .taker_fee(Decimal::ONE_HUNDRED, Decimal::ZERO)
                .expect("zero is a valid endpoint"),
            Decimal::ZERO
        );
        assert_eq!(
            calculator
                .taker_fee(Decimal::ONE_HUNDRED, Decimal::ONE)
                .expect("one is a valid endpoint"),
            Decimal::ZERO
        );
    }

    #[test]
    fn f64_projection_preserves_the_official_anchor() {
        let calculator = FeeCalculator::from_market_metadata(Decimal::new(7, 2), 1)
            .expect("fd.r=0.07 and fd.e=1 are valid");

        assert_eq!(
            calculator
                .taker_fee_f64(100.0, 0.5)
                .expect("official f64 projection inputs are valid"),
            1.75
        );
    }

    #[test]
    fn invalid_metadata_rate_is_rejected() {
        assert_eq!(
            FeeCalculator::from_market_metadata(Decimal::NEGATIVE_ONE, 1),
            Err(PowerFeeError::NegativeRate)
        );
    }

    /// [EVAL] The metadata formula must remain total over valid endpoints and
    /// fail closed over invalid numbers, overflow, and non-finite projections.
    #[test]
    fn fee_contract_generalizes_beyond_the_official_anchor() {
        let linear = FeeCalculator::from_market_metadata(Decimal::new(7, 2), 1).unwrap();
        for shares in [Decimal::ZERO, Decimal::NEGATIVE_ONE] {
            assert_eq!(
                linear.taker_fee(shares, Decimal::new(5, 1)),
                Err(PowerFeeError::NonPositiveShares)
            );
        }
        for price in [Decimal::new(-1, 1), Decimal::new(11, 1)] {
            assert_eq!(
                linear.taker_fee(Decimal::ONE, price),
                Err(PowerFeeError::PriceOutOfBounds)
            );
        }
        assert_eq!(
            linear.taker_fee_f64(f64::NAN, 0.5),
            Err(PowerFeeError::FloatConversion)
        );

        let exponent_zero = FeeCalculator::from_market_metadata(Decimal::new(7, 2), 0).unwrap();
        assert_eq!(
            exponent_zero.taker_fee(Decimal::ONE_HUNDRED, Decimal::ZERO),
            Ok(Decimal::new(7, 0))
        );
        assert_eq!(
            exponent_zero.taker_fee(Decimal::ONE_HUNDRED, Decimal::ONE),
            Ok(Decimal::new(7, 0))
        );

        let overflowing = FeeCalculator::from_market_metadata(Decimal::MAX, 1).unwrap();
        assert_eq!(
            overflowing.taker_fee(Decimal::MAX, Decimal::new(5, 1)),
            Err(PowerFeeError::ArithmeticOverflow)
        );
    }

    /// The bid pays the shares and their fee: a buy never spends more than its bid, at the ask or
    /// at any lower fill, and the Decimal and f64 schedules agree.
    #[test]
    fn a_bid_pays_the_shares_and_their_fee() {
        let exact = FeeCalculator::crypto();
        let model = exact.to_f64().unwrap();
        for bid in [1.0_f64, 4.0, 10.8825, 100.0] {
            for cents in 15..=85 {
                let ask = f64::from(cents) / 100.0;
                let shares = model.shares_for_bid(Liquidity::Taker, bid, ask).unwrap();
                for fill in [ask, ask - 0.01, ask - 0.05] {
                    let spend = model.cost(Liquidity::Taker, shares, fill).unwrap();
                    assert!(
                        spend <= bid,
                        "bid {bid} ask {ask} fill {fill}: spend {spend}"
                    );
                }
                let d = |x: f64| Decimal::try_from(x).unwrap();
                let exact_spend = exact.cost(Liquidity::Taker, d(shares), d(ask)).unwrap();
                assert!(
                    exact_spend <= d(bid),
                    "bid {bid} ask {ask}: exact spend {exact_spend}"
                );
                let per_share = model.per_share(Liquidity::Taker, ask).unwrap();
                assert!((per_share - 0.07 * ask * (1.0 - ask)).abs() < 1e-15);
            }
        }
    }

    /// Worked numbers: 4 USD at 0.80 buys 4.93 shares after the grid floor; selling 4.93 at 0.90
    /// pays 4.437 less a 0.03106 fee; a maker pays nothing either way.
    #[test]
    fn worked_entry_sale_and_maker_numbers() {
        let fees = FeeCalculator::crypto();
        let shares = fees
            .shares_for_bid(Liquidity::Taker, Decimal::new(4, 0), Decimal::new(80, 2))
            .unwrap();
        // (4 − 0.00001) / (0.80 + 0.07 × 0.16) = 3.99999 / 0.8112
        assert_eq!(shares.round_dp(6), Decimal::new(4_930_954, 6));
        let held = Decimal::new(493, 2);
        assert_eq!(
            fees.fee(Liquidity::Taker, held, Decimal::new(80, 2))
                .unwrap(),
            Decimal::new(5522, 5)
        );
        assert_eq!(
            fees.proceeds(Liquidity::Taker, held, Decimal::new(90, 2))
                .unwrap(),
            Decimal::new(4_437, 3) - Decimal::new(3106, 5)
        );
        assert_eq!(
            fees.proceeds(Liquidity::Maker, held, Decimal::new(90, 2))
                .unwrap(),
            Decimal::new(4_437, 3)
        );
        assert_eq!(
            fees.shares_for_bid(Liquidity::Maker, Decimal::new(4, 0), Decimal::new(80, 2))
                .unwrap(),
            Decimal::new(5, 0)
        );
    }

    #[test]
    fn f64_rounds_like_decimal() {
        let model = FeeCalculator::crypto().to_f64().unwrap();
        // 100 × 0.07 × 0.25 = 1.75 exactly; 3 × 0.07 × 0.2475 = 0.051975 → 0.05198 (away from zero)
        assert_eq!(model.fee(Liquidity::Taker, 100.0, 0.5).unwrap(), 1.75);
        assert_eq!(model.fee(Liquidity::Taker, 3.0, 0.55).unwrap(), 0.05198);
        assert_eq!(
            FeeCalculator::crypto()
                .taker_fee(Decimal::new(3, 0), Decimal::new(55, 2))
                .unwrap(),
            Decimal::new(5198, 5)
        );
        assert_eq!(Fees::new(f64::NAN, 1), Err(PowerFeeError::NegativeRate));
    }

    #[cfg(feature = "json")]
    #[test]
    fn serde_round_trip_preserves_metadata_schedule() {
        let calculator = FeeCalculator::from_market_metadata(Decimal::new(7, 2), 2)
            .expect("fd.r=0.07 and fd.e=2 are valid");
        let encoded = serde_json::to_string(&calculator).expect("fee calculator serializes");
        let decoded: FeeCalculator =
            serde_json::from_str(&encoded).expect("fee calculator deserializes");

        assert_eq!(decoded, calculator);
        assert_eq!(
            decoded
                .taker_fee(Decimal::ONE_HUNDRED, Decimal::new(5, 1))
                .expect("round-tripped schedule remains valid"),
            Decimal::new(4375, 4)
        );
    }
}
