/*
    Appellation: fees <module>
    Contrib: @FL03
*/
//! Polymarket platform fees: the one fee formula every consumer reads.
//!
//! Polymarket US (<https://docs.polymarket.us/fees>) charges a taker, per fill of `C` contracts
//! at price `p`:
//!
//! ```text
//! fee = Θ × C × p × (1 − p)        rounded to $0.01, round half to even
//! ```
//!
//! where `Θ` is the taker coefficient: tiered by trailing 30-day volume for app and web orders,
//! a flat 0.0695 for API orders. A maker pays the maker coefficient on the same curve, which is
//! zero or negative (a rebate credited at the fill) by maker tier. Taker rebates (the API
//! program's 70%, or the volume tiers) are a share of the taker fee paid back later, so they
//! never reduce what a fill charges. Across one order's fills the total charged never exceeds
//! the rounding of the order's exact fee ([`Fees::order_fees`]). The international CLOB uses the
//! same curve with a market-supplied exponent and five-decimal rounding.
//!
//! The venue takes the taker fee from the balance at the fill, on top of the notional. Sizing
//! takes it out of the bid instead ([`Fees::shares_for_bid`]), so notional plus fee never
//! exceeds the bid and a trade never loses more than it bid. A sale pays out its notional less
//! the fee ([`Fees::proceeds`]); redemption at settlement is free.
//!
//! [`Fees<T, S>`](Fees) is generic over the number and the venue. `T` is [`Decimal`] for money
//! that settles or `f64` for model arithmetic (sizing, edge gates). `S` is a [`FeeSchedule`]
//! marker naming the venue: its rounding and its standard coefficients. [`Pmus`] is Polymarket US
//! ([`PmusFees`]), [`Clob`] the international CLOB ([`FeeCalculator`]). The formula is written
//! once, on the generic impl; a venue only supplies amounts. Another venue, or a consumer's own
//! schedule (a paper venue, say), is a new marker, never a second formula. Each instance carries
//! the account's coefficients (tier, program, market metadata), which come from configuration.

use core::marker::PhantomData;

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

/// Which side of the book a fill takes.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(rename_all = "snake_case")
)]
pub enum Liquidity {
    /// Removes liquidity: a market or FAK/FOK order, or a limit that matches at once.
    #[default]
    Taker,
    /// Adds liquidity: a resting order matched later.
    Maker,
}

/// How a venue rounds a fee amount.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(rename_all = "snake_case")
)]
pub enum FeeRounding {
    /// The international CLOB: five decimals, midpoint away from zero.
    #[default]
    FiveDecimals,
    /// Polymarket US: whole cents, round half to even (banker's rounding).
    Cents,
}

impl FeeRounding {
    const fn places(self) -> u32 {
        match self {
            Self::FiveDecimals => POLYMARKET_FEE_PRECISION,
            Self::Cents => 2,
        }
    }

    const fn half_even(self) -> bool {
        matches!(self, Self::Cents)
    }
}

/// A number the fee formula runs on: [`Decimal`] or `f64`.
pub trait FeeScalar: Copy + PartialOrd {
    const ZERO: Self;
    const ONE: Self;
    /// One unit in the last place `rounding` keeps: `0.01` or `0.00001`.
    fn tick(rounding: FeeRounding) -> Self;
    /// Rounds to the places `rounding` keeps, half to even or half away from zero.
    fn round_fee(self, rounding: FeeRounding) -> Self;
    fn checked_add(self, other: Self) -> Option<Self>;
    fn checked_sub(self, other: Self) -> Option<Self>;
    fn checked_mul(self, other: Self) -> Option<Self>;
    fn checked_div(self, other: Self) -> Option<Self>;
}

impl FeeScalar for Decimal {
    const ZERO: Self = Decimal::ZERO;
    const ONE: Self = Decimal::ONE;

    fn tick(rounding: FeeRounding) -> Self {
        Decimal::new(1, rounding.places())
    }
    fn round_fee(self, rounding: FeeRounding) -> Self {
        let strategy = if rounding.half_even() {
            RoundingStrategy::MidpointNearestEven
        } else {
            RoundingStrategy::MidpointAwayFromZero
        };
        self.round_dp_with_strategy(rounding.places(), strategy)
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
}

impl FeeScalar for f64 {
    const ZERO: Self = 0.0;
    const ONE: Self = 1.0;

    fn tick(rounding: FeeRounding) -> Self {
        10f64.powi(-(rounding.places() as i32))
    }
    fn round_fee(self, rounding: FeeRounding) -> Self {
        let scale = 10f64.powi(rounding.places() as i32);
        let scaled = self * scale;
        let rounded = if rounding.half_even() {
            scaled.round_ties_even()
        } else {
            scaled.round()
        };
        rounded / scale
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
}

/// A venue's fee schedule: how it rounds a fee and its standard coefficients. Implemented by
/// fieldless markers ([`Pmus`], [`Clob`]) that type a [`Fees`]; the arithmetic never varies by
/// venue, only these amounts.
pub trait FeeSchedule:
    Copy + Default + core::fmt::Debug + Eq + core::hash::Hash + Send + Sync + 'static
{
    /// A short name for logs.
    const NAME: &'static str;
    /// How the venue rounds each fill's fee.
    const ROUNDING: FeeRounding;
    /// The curve's standard exponent.
    const EXPONENT: u32;
    /// The standard taker Θ: what an account with no tier or program pays.
    const TAKER: Decimal;
    /// The standard maker Θ: zero, or negative for a rebate.
    const MAKER: Decimal;
    /// The standard share of a taker fee paid back later.
    const TAKER_REBATE: Decimal;
}

/// Polymarket US (<https://docs.polymarket.us/fees>): `Θ × C × p × (1 − p)`, whole cents, half to
/// even. The standard Θ is taker tier 1, the app and web rate a market's `feeCoefficient` carries.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Pmus;

impl FeeSchedule for Pmus {
    const NAME: &'static str = "polymarket-us";
    const ROUNDING: FeeRounding = FeeRounding::Cents;
    const EXPONENT: u32 = 1;
    const TAKER: Decimal = Self::TAKER_TIERS[0];
    const MAKER: Decimal = Decimal::ZERO;
    const TAKER_REBATE: Decimal = Decimal::ZERO;
}

impl Pmus {
    /// The API program: every API taker fill pays Θ 0.0695 and gets 70% of that fee back weekly,
    /// 12:01 AM ET 2026-11-21 through 2026-12-31 ET, straight trades only. Add the account's maker
    /// rebate with [`Fees::with_maker`]; the end date belongs in configuration ([`FeePlan`]).
    pub const API: PmusFees = Fees {
        taker: Decimal::from_parts(695, 0, 0, false, 4),
        exponent: 1,
        maker: Decimal::ZERO,
        taker_rebate: Decimal::from_parts(70, 0, 0, false, 2),
        schedule: PhantomData,
    };

    /// App and web taker Θ by taker fee tier 1 to 4 (trailing 30-day taker volume below $10k,
    /// to $50k, to $250k, above), from 9:45 PM ET 2026-11-25.
    pub const TAKER_TIERS: [Decimal; 4] = [
        Decimal::from_parts(11, 0, 0, false, 2),
        Decimal::from_parts(10, 0, 0, false, 2),
        Decimal::from_parts(9, 0, 0, false, 2),
        Decimal::from_parts(695, 0, 0, false, 4),
    ];

    /// Maker Θ by maker rebate tier 1 to 4 (qualifying maker volume below $10M, to $50M, to
    /// $200M, above), credited at the fill.
    pub const MAKER_TIERS: [Decimal; 4] = [
        Decimal::ZERO,
        Decimal::from_parts(35, 0, 0, true, 4),
        Decimal::from_parts(70, 0, 0, true, 4),
        Decimal::from_parts(104, 0, 0, true, 4),
    ];

    /// Share of taker fees paid back weekly by taker rebate tier 1 to 6 (qualifying taker
    /// volume below $250k, to $1M, to $10M, to $25M, to $50M, above). Set independently of the
    /// taker fee tier.
    pub const TAKER_REBATE_TIERS: [Decimal; 6] = [
        Decimal::ZERO,
        Decimal::from_parts(3, 0, 0, false, 2),
        Decimal::from_parts(5, 0, 0, false, 2),
        Decimal::from_parts(10, 0, 0, false, 2),
        Decimal::from_parts(15, 0, 0, false, 2),
        Decimal::from_parts(30, 0, 0, false, 2),
    ];
}

/// The international CLOB: rate and exponent from each market's `fd.r` and `fd.e`, five decimals,
/// midpoint away from zero. The standard is the current Crypto schedule, rate `0.07`, exponent 1.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Clob;

impl FeeSchedule for Clob {
    const NAME: &'static str = "polymarket-clob";
    const ROUNDING: FeeRounding = FeeRounding::FiveDecimals;
    const EXPONENT: u32 = CURRENT_CRYPTO_EXPONENT;
    const TAKER: Decimal = CURRENT_CRYPTO_RATE;
    const MAKER: Decimal = Decimal::ZERO;
    const TAKER_REBATE: Decimal = Decimal::ZERO;
}

/// One account's fees at a venue `S`: the taker and maker coefficients on the
/// `(p × (1 − p))^exponent` curve and the share of the taker fee paid back later. Rounding is the
/// venue's ([`FeeSchedule::ROUNDING`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(bound(
        serialize = "T: serde::Serialize",
        deserialize = "T: serde::Deserialize<'de> + FeeScalar"
    ))
)]
pub struct Fees<T = Decimal, S: FeeSchedule = Pmus> {
    /// Θ for a taker fill (`fd.r`, or the US tier or program coefficient).
    #[cfg_attr(feature = "serde", serde(rename = "fee_rate"))]
    taker: T,
    /// The curve's exponent (`fd.e`; 1 on Polymarket US).
    #[cfg_attr(feature = "serde", serde(rename = "fee_exponent"))]
    exponent: u32,
    /// Θ for a maker fill: zero, or negative for a maker rebate.
    #[cfg_attr(feature = "serde", serde(default = "zero"))]
    maker: T,
    /// The share of a taker fee paid back later (`0.70` for the US API program).
    #[cfg_attr(feature = "serde", serde(default = "zero"))]
    taker_rebate: T,
    #[cfg_attr(feature = "serde", serde(skip))]
    schedule: PhantomData<S>,
}

#[cfg(feature = "serde")]
fn zero<T: FeeScalar>() -> T {
    T::ZERO
}

/// Polymarket US fees.
pub type PmusFees<T = Decimal> = Fees<T, Pmus>;

/// The international CLOB's exact schedule, as market metadata states it.
pub type FeeCalculator = Fees<Decimal, Clob>;

// The negated comparisons are deliberate: a NaN `f64` fails every check, so it is refused.
#[allow(clippy::neg_cmp_op_on_partial_ord)]
impl<T: FeeScalar, S: FeeSchedule> Fees<T, S> {
    /// A taker-only schedule: no maker fee and no taker rebate until set.
    ///
    /// # Errors
    ///
    /// [`PowerFeeError::NegativeRate`] when `taker` is negative or not a number.
    pub fn new(taker: T, exponent: u32) -> Result<Self, PowerFeeError> {
        if !(taker >= T::ZERO) {
            return Err(PowerFeeError::NegativeRate);
        }
        Ok(Self {
            taker,
            exponent,
            maker: T::ZERO,
            taker_rebate: T::ZERO,
            schedule: PhantomData,
        })
    }

    /// A taker-only schedule at the venue's standard exponent: `taker` is the account's Θ (tier,
    /// program or market metadata).
    ///
    /// # Errors
    ///
    /// As [`Fees::new`].
    pub fn with_rate(taker: T) -> Result<Self, PowerFeeError> {
        Self::new(taker, S::EXPONENT)
    }

    /// The maker coefficient: zero, or negative for a maker rebate credited at the fill.
    ///
    /// # Errors
    ///
    /// [`PowerFeeError::NegativeRate`] when `maker` is not a number or a rebate larger than the
    /// taker fee.
    pub fn with_maker(mut self, maker: T) -> Result<Self, PowerFeeError> {
        let floor = T::ZERO
            .checked_sub(self.taker)
            .ok_or(PowerFeeError::ArithmeticOverflow)?;
        if !(maker >= floor) {
            return Err(PowerFeeError::NegativeRate);
        }
        self.maker = maker;
        Ok(self)
    }

    /// The share of each taker fee paid back later, in `[0, 1]`.
    ///
    /// # Errors
    ///
    /// [`PowerFeeError::NegativeRate`] outside `[0, 1]`.
    pub fn with_taker_rebate(mut self, rebate: T) -> Result<Self, PowerFeeError> {
        if !(T::ZERO <= rebate && rebate <= T::ONE) {
            return Err(PowerFeeError::NegativeRate);
        }
        self.taker_rebate = rebate;
        Ok(self)
    }

    /// The taker coefficient Θ.
    pub const fn rate(&self) -> T {
        self.taker
    }

    /// The maker coefficient.
    pub const fn maker(&self) -> T {
        self.maker
    }

    /// The share of a taker fee paid back later.
    pub const fn taker_rebate(&self) -> T {
        self.taker_rebate
    }

    /// The curve's exponent.
    pub const fn exponent(&self) -> u32 {
        self.exponent
    }

    /// The venue's rounding.
    pub const fn rounding(&self) -> FeeRounding {
        S::ROUNDING
    }

    /// Whether a taker fill pays nothing at any price.
    pub fn is_free(&self) -> bool {
        self.taker == T::ZERO
    }

    const fn coefficient(&self, liquidity: Liquidity) -> T {
        match liquidity {
            Liquidity::Taker => self.taker,
            Liquidity::Maker => self.maker,
        }
    }

    /// The fee on one contract at `price`, unrounded: `Θ × (p × (1 − p))^exponent`, with the
    /// taker or maker Θ. Negative for a maker rebate.
    ///
    /// # Errors
    ///
    /// A `price` outside `[0, 1]`, or arithmetic that overflows.
    pub fn per_share(&self, liquidity: Liquidity, price: T) -> Result<T, PowerFeeError> {
        if !(T::ZERO <= price && price <= T::ONE) {
            return Err(PowerFeeError::PriceOutOfBounds);
        }
        let theta = self.coefficient(liquidity);
        if theta == T::ZERO {
            return Ok(T::ZERO);
        }
        let base = T::ONE
            .checked_sub(price)
            .and_then(|complement| price.checked_mul(complement))
            .ok_or(PowerFeeError::ArithmeticOverflow)?;
        pow(base, self.exponent)
            .and_then(|curve| theta.checked_mul(curve))
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }

    fn exact(&self, liquidity: Liquidity, shares: T, price: T) -> Result<T, PowerFeeError> {
        if !(shares > T::ZERO) {
            return Err(PowerFeeError::NonPositiveShares);
        }
        self.per_share(liquidity, price)?
            .checked_mul(shares)
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }

    /// What one fill of `shares` at `price` charges, rounded as the venue rounds it. Negative
    /// for a maker rebate.
    ///
    /// # Errors
    ///
    /// Non-positive `shares`, a `price` outside `[0, 1]`, or arithmetic that overflows.
    pub fn fee(&self, liquidity: Liquidity, shares: T, price: T) -> Result<T, PowerFeeError> {
        Ok(self.exact(liquidity, shares, price)?.round_fee(S::ROUNDING))
    }

    /// What one order's taker fills charge, fill by fill: each fill's rounded fee, cut from the
    /// last fill back so the total never exceeds the rounding of the order's exact fee. Maker
    /// fills are charged each on its own.
    ///
    /// # Errors
    ///
    /// As [`Fees::fee`], for any fill.
    pub fn order_fees(
        &self,
        liquidity: Liquidity,
        fills: &[(T, T)],
    ) -> Result<Vec<T>, PowerFeeError> {
        let mut charged = Vec::with_capacity(fills.len());
        let mut exact_total = T::ZERO;
        let mut charged_total = T::ZERO;
        for &(shares, price) in fills {
            let exact = self.exact(liquidity, shares, price)?;
            let fee = exact.round_fee(S::ROUNDING);
            exact_total = exact_total
                .checked_add(exact)
                .ok_or(PowerFeeError::ArithmeticOverflow)?;
            charged_total = charged_total
                .checked_add(fee)
                .ok_or(PowerFeeError::ArithmeticOverflow)?;
            charged.push(fee);
        }
        if liquidity == Liquidity::Maker {
            return Ok(charged);
        }
        let mut excess = charged_total
            .checked_sub(exact_total.round_fee(S::ROUNDING))
            .ok_or(PowerFeeError::ArithmeticOverflow)?;
        for fee in charged.iter_mut().rev() {
            if !(excess > T::ZERO) {
                break;
            }
            let cut = if excess < *fee { excess } else { *fee };
            *fee = fee
                .checked_sub(cut)
                .ok_or(PowerFeeError::ArithmeticOverflow)?;
            excess = excess
                .checked_sub(cut)
                .ok_or(PowerFeeError::ArithmeticOverflow)?;
        }
        Ok(charged)
    }

    /// A fill's fee less the taker rebate paid back later: the fee's economic cost once the
    /// rebate arrives. Unrounded, since the venue does not state how a rebate rounds.
    ///
    /// # Errors
    ///
    /// As [`Fees::fee`].
    pub fn fee_after_rebate(
        &self,
        liquidity: Liquidity,
        shares: T,
        price: T,
    ) -> Result<T, PowerFeeError> {
        let fee = self.fee(liquidity, shares, price)?;
        if liquidity == Liquidity::Maker {
            return Ok(fee);
        }
        T::ONE
            .checked_sub(self.taker_rebate)
            .and_then(|kept| fee.checked_mul(kept))
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }

    /// What a buy of `shares` at `price` takes at the fill: notional plus fee.
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

    /// What a sale of `shares` at `price` pays at the fill: notional less fee.
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
    /// shares = (bid − tick) / (p + Θ × (p × (1 − p))^exponent)
    /// ```
    ///
    /// One rounding tick is held back because the rounded fee can exceed the exact fee by half a
    /// tick. A fill at a lower price costs less per share (`d/dp [p + Θ·p(1−p)] = 1 + Θ(1 − 2p) >
    /// 0` for `Θ < 1`), and a venue grid only floors shares, so neither pushes the spend past the
    /// bid. A leg that pays nothing (a free schedule, or a maker without a fee) buys exactly
    /// `bid / price`; a maker rebate is not spent ahead of its credit.
    ///
    /// # Errors
    ///
    /// A `bid` no larger than the tick, a `price` outside `(0, 1]`, or arithmetic that overflows.
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
        let (spendable, per_share) = if per_share > T::ZERO {
            let spendable = bid
                .checked_sub(T::tick(S::ROUNDING))
                .ok_or(PowerFeeError::ArithmeticOverflow)?;
            (spendable, per_share)
        } else {
            (bid, T::ZERO)
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

impl<S: FeeSchedule> Fees<Decimal, S> {
    /// The venue's standard schedule, from its [`FeeSchedule`] constants.
    pub const fn standard() -> Self {
        Self {
            taker: S::TAKER,
            exponent: S::EXPONENT,
            maker: S::MAKER,
            taker_rebate: S::TAKER_REBATE,
            schedule: PhantomData,
        }
    }

    /// Alias of [`Fees::new`] under the metadata names.
    ///
    /// # Errors
    ///
    /// As [`Fees::new`].
    pub fn from_market_metadata(rate: Decimal, exponent: u32) -> Result<Self, PowerFeeError> {
        Self::new(rate, exponent)
    }

    /// The same schedule over `f64`, for model arithmetic.
    ///
    /// # Errors
    ///
    /// [`PowerFeeError::FloatConversion`] when a coefficient has no finite `f64`.
    pub fn to_f64(&self) -> Result<Fees<f64, S>, PowerFeeError> {
        let float =
            |value: Decimal| f64::try_from(value).map_err(|_| PowerFeeError::FloatConversion);
        Ok(Fees {
            taker: float(self.taker)?,
            exponent: self.exponent,
            maker: float(self.maker)?,
            taker_rebate: float(self.taker_rebate)?,
            schedule: PhantomData,
        })
    }

    /// The taker coefficient under its V2 metadata name, `fd.r`.
    pub const fn fee_rate(&self) -> Decimal {
        self.taker
    }

    /// The exponent under its V2 metadata name, `fd.e`.
    pub const fn fee_exponent(&self) -> u32 {
        self.exponent
    }

    /// The taker fee on `shares` at `price`, rounded as the venue rounds it.
    ///
    /// # Errors
    ///
    /// As [`Fees::fee`].
    pub fn taker_fee(&self, shares: Decimal, price: Decimal) -> Result<Decimal, PowerFeeError> {
        self.fee(Liquidity::Taker, shares, price)
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

impl Fees<Decimal, Clob> {
    /// The international CLOB's current Crypto schedule: rate `0.07`, exponent `1`. Metadata
    /// stays authoritative for live markets.
    pub const fn crypto() -> Self {
        Self::standard()
    }
}

/// The fee schedule an account trades under over time: a promotional program until it ends, then
/// the account's volume tier. Polymarket US fixes an order's coefficients when the order is
/// inserted, so look the schedule up at the insert time.
///
/// `At` is whatever instant the caller keeps (`chrono::DateTime<Utc>`, Unix seconds); the program
/// covers instants strictly before its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(bound(
        serialize = "T: serde::Serialize, At: serde::Serialize",
        deserialize = "T: serde::Deserialize<'de> + FeeScalar, At: serde::Deserialize<'de>"
    ))
)]
pub struct FeePlan<T, At, S: FeeSchedule = Pmus> {
    /// The account's schedule outside any program.
    pub tier: Fees<T, S>,
    /// A program and the instant it ends: the US API program (Θ 0.0695, 70% taker rebate) runs
    /// through 2026-12-31.
    pub program: Option<FeeProgram<T, At, S>>,
}

/// A dated fee program.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(bound(
        serialize = "T: serde::Serialize, At: serde::Serialize",
        deserialize = "T: serde::Deserialize<'de> + FeeScalar, At: serde::Deserialize<'de>"
    ))
)]
pub struct FeeProgram<T, At, S: FeeSchedule = Pmus> {
    pub fees: Fees<T, S>,
    /// The first instant the program no longer covers.
    pub ends: At,
}

impl<T: FeeScalar, At: PartialOrd, S: FeeSchedule> FeePlan<T, At, S> {
    /// The schedule an order inserted at `at` pays.
    pub fn at(&self, at: &At) -> &Fees<T, S> {
        match &self.program {
            Some(program) if *at < program.ends => &program.fees,
            _ => &self.tier,
        }
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
    fn a_metadata_schedule_charges_makers_nothing() {
        let calculator = FeeCalculator::from_market_metadata(Decimal::new(7, 2), 2)
            .expect("fd.r=0.07 and fd.e=2 are valid");

        assert_eq!(
            calculator.fee(Liquidity::Maker, Decimal::ONE_HUNDRED, Decimal::new(5, 1)),
            Ok(Decimal::ZERO)
        );
    }

    fn us_api() -> PmusFees {
        Pmus::API.with_maker(Pmus::MAKER_TIERS[3]).unwrap()
    }

    #[test]
    fn published_constants_match_their_builders() {
        let built = PmusFees::with_rate(Decimal::new(695, 4))
            .unwrap()
            .with_taker_rebate(Decimal::new(70, 2))
            .unwrap();
        assert_eq!(Pmus::API, built);
        assert_eq!(Pmus::API.rounding(), FeeRounding::Cents);
        assert_eq!(PmusFees::standard().rate(), Decimal::new(11, 2));
        assert_eq!(Pmus::TAKER_TIERS[3], Pmus::API.rate());
        assert_eq!(Pmus::MAKER_TIERS[2], Decimal::new(-70, 4));
        assert_eq!(Pmus::TAKER_REBATE_TIERS[5], Decimal::new(30, 2));
        assert_eq!(
            FeeCalculator::crypto(),
            FeeCalculator::new(Decimal::new(7, 2), 1).unwrap()
        );
        assert_eq!(
            FeeCalculator::crypto().rounding(),
            FeeRounding::FiveDecimals
        );
    }

    /// The worked examples on <https://docs.polymarket.us/fees>: Θ 0.0695 on 100 contracts.
    #[test]
    fn us_schedule_matches_the_published_examples() {
        let fees = us_api();
        let fee = |price: i64| {
            fees.fee(
                Liquidity::Taker,
                Decimal::ONE_HUNDRED,
                Decimal::new(price, 2),
            )
        };
        assert_eq!(fee(10), Ok(Decimal::new(63, 2))); // 0.6255
        assert_eq!(fee(30), Ok(Decimal::new(146, 2))); // 1.4595
        assert_eq!(fee(65), Ok(Decimal::new(158, 2))); // 1.581125
        assert_eq!(fee(50), Ok(Decimal::new(174, 2))); // 1.7375
        // Tier 4 maker rebate at 0.10: −0.0104 × 100 × 0.09 = −0.0936 → −0.09, credited at the fill.
        assert_eq!(
            fees.fee(Liquidity::Maker, Decimal::ONE_HUNDRED, Decimal::new(10, 2)),
            Ok(Decimal::new(-9, 2))
        );
        // The 70% API rebate is paid back on the taker fee; makers keep their rebate whole.
        assert_eq!(
            fees.fee_after_rebate(Liquidity::Taker, Decimal::ONE_HUNDRED, Decimal::new(50, 2)),
            Ok(Decimal::new(5220, 4))
        );
        assert_eq!(
            fees.fee_after_rebate(Liquidity::Maker, Decimal::ONE_HUNDRED, Decimal::new(10, 2)),
            Ok(Decimal::new(-9, 2))
        );
    }

    #[test]
    fn cents_round_half_to_even() {
        let half_even = |x: Decimal| x.round_fee(FeeRounding::Cents);
        assert_eq!(half_even(Decimal::new(25, 3)), Decimal::new(2, 2));
        assert_eq!(half_even(Decimal::new(35, 3)), Decimal::new(4, 2));
        assert_eq!(0.025_f64.round_fee(FeeRounding::Cents), 0.02);
        assert_eq!(0.035_f64.round_fee(FeeRounding::Cents), 0.04);
        assert_eq!(
            Decimal::new(25, 3).round_fee(FeeRounding::FiveDecimals),
            Decimal::new(25, 3)
        );
    }

    /// Ten fills of 1 contract at 0.50 each round to 0.02 (0.017375); the order's exact fee is
    /// 0.17375 → 0.17, so the last fill gives back 0.03 of the 0.20 charged.
    #[test]
    fn an_order_never_pays_more_than_its_rounded_exact_fee() {
        let fees = us_api();
        let fills = [(Decimal::ONE, Decimal::new(50, 2)); 10];
        let charged = fees.order_fees(Liquidity::Taker, &fills).unwrap();
        assert_eq!(
            charged.iter().copied().sum::<Decimal>(),
            Decimal::new(17, 2)
        );
        assert_eq!(charged[9], Decimal::ZERO);
        assert_eq!(charged[8], Decimal::new(1, 2));
        assert!(charged[..8].iter().all(|fee| *fee == Decimal::new(2, 2)));
        // A single fill is its own order.
        assert_eq!(
            fees.order_fees(
                Liquidity::Taker,
                &[(Decimal::ONE_HUNDRED, Decimal::new(50, 2))]
            ),
            Ok(vec![Decimal::new(174, 2)])
        );
    }

    #[test]
    fn a_us_bid_pays_the_shares_and_their_cent_rounded_fee() {
        let model = us_api().to_f64().unwrap();
        for bid in [1.0_f64, 4.0, 25.0, 100.0] {
            for cents in 5..=95 {
                let ask = f64::from(cents) / 100.0;
                let shares = model.shares_for_bid(Liquidity::Taker, bid, ask).unwrap();
                let spend = model.cost(Liquidity::Taker, shares, ask).unwrap();
                assert!(spend <= bid + 1e-12, "bid {bid} ask {ask}: spend {spend}");
            }
        }
    }

    #[test]
    fn a_rebate_above_the_taker_fee_or_outside_one_is_refused() {
        let fees = PmusFees::with_rate(Decimal::new(695, 4)).unwrap();
        assert_eq!(
            fees.with_maker(Decimal::new(-1, 1)),
            Err(PowerFeeError::NegativeRate)
        );
        assert_eq!(
            fees.with_taker_rebate(Decimal::new(11, 1)),
            Err(PowerFeeError::NegativeRate)
        );
        assert_eq!(
            fees.with_taker_rebate(Decimal::NEGATIVE_ONE),
            Err(PowerFeeError::NegativeRate)
        );
    }

    /// The API program covers orders inserted before its end, then the volume tier applies.
    #[test]
    fn a_program_gives_way_to_the_tier_when_it_ends() {
        let tier = PmusFees::standard();
        let plan = FeePlan {
            tier,
            program: Some(FeeProgram {
                fees: us_api(),
                ends: 1_798_761_600_i64,
            }), // 2027-01-01Z
        };
        assert_eq!(plan.at(&1_798_761_599).rate(), Decimal::new(695, 4));
        assert_eq!(plan.at(&1_798_761_599).taker_rebate(), Decimal::new(70, 2));
        assert_eq!(plan.at(&1_798_761_600), &tier);
        assert_eq!(
            FeePlan::<Decimal, i64> {
                tier,
                program: None
            }
            .at(&0),
            &tier
        );
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
        assert_eq!(
            Fees::<f64, Clob>::new(f64::NAN, 1),
            Err(PowerFeeError::NegativeRate)
        );
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
