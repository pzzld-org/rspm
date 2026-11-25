/*
    Appellation: rebates <module>
    Contrib: @FL03
*/
//! Money a venue pays back: kept apart from [`crate::fees`] so the fee stays the pure published
//! fee and a program can be deleted (this module, or one const) without touching [`Fees`].
//!
//! Polymarket US (<https://docs.polymarket.us/fees>) pays two kinds, and they never mix:
//!
//! - [`TakerCashback`]: a share of each taker fee charged, paid back later (weekly). The API
//!   program pays 70% through 2026-12-31 ([`Pmus::API_CASHBACK`]); otherwise the taker rebate
//!   tier ([`Pmus::TAKER_CASHBACK_TIERS`]).
//! - [`MakerRebate`]: a coefficient on the fee curve, `rate × C × p × (1 − p)`, credited at the
//!   fill by maker rebate tier ([`Pmus::MAKER_REBATE_TIERS`]), computed on the gross fee.
//!
//! Neither lowers what a fill charges: sizing and loss caps use [`Fees`] alone.

use core::marker::PhantomData;

use rust_decimal::Decimal;

use crate::fees::{FeeScalar, FeeSchedule, Fees, Liquidity, Pmus, PowerFeeError, pow};

/// When money paid back arrives.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(rename_all = "snake_case")
)]
pub enum Payout {
    /// Credited with the fill.
    AtFill,
    /// Paid in a weekly batch.
    #[default]
    Weekly,
}

/// A share of each taker fee paid back later, over a dated window `[starts, ends)` in Unix
/// seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct TakerCashback<T = Decimal> {
    share: T,
    payout: Payout,
    starts: i64,
    ends: i64,
}

// The negated comparisons are deliberate: a NaN `f64` fails every check, so it is refused.
#[allow(clippy::neg_cmp_op_on_partial_ord)]
impl<T: FeeScalar> TakerCashback<T> {
    /// # Errors
    ///
    /// [`PowerFeeError::NegativeRate`] for a `share` outside `[0, 1]` or a window that ends
    /// before it starts.
    pub fn new(share: T, payout: Payout, starts: i64, ends: i64) -> Result<Self, PowerFeeError> {
        if !(T::ZERO <= share && share <= T::ONE) || ends < starts {
            return Err(PowerFeeError::NegativeRate);
        }
        Ok(Self {
            share,
            payout,
            starts,
            ends,
        })
    }

    pub const fn share(&self) -> T {
        self.share
    }

    pub const fn payout(&self) -> Payout {
        self.payout
    }

    /// The first Unix second the cash back covers.
    pub const fn starts(&self) -> i64 {
        self.starts
    }

    /// The first Unix second the cash back no longer covers.
    pub const fn ends(&self) -> i64 {
        self.ends
    }

    /// Whether a fill at Unix second `at` earns it.
    pub const fn active(&self, at: i64) -> bool {
        self.starts <= at && at < self.ends
    }

    /// The share of `at`'s fills paid back: [`Self::share`] inside the window, else zero.
    pub fn share_at(&self, at: i64) -> T {
        if self.active(at) { self.share } else { T::ZERO }
    }

    /// What comes back on a taker fee charged: `share × fee`, unrounded, since the venue does not
    /// state how a payout rounds.
    ///
    /// # Errors
    ///
    /// Arithmetic that overflows.
    pub fn on_fee(&self, fee: T) -> Result<T, PowerFeeError> {
        self.share
            .checked_mul(fee)
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }

    /// What comes back per contract on a taker fill at `price` under `fees`, unrounded: for model
    /// arithmetic beside [`Fees::per_share`].
    ///
    /// # Errors
    ///
    /// As [`Fees::per_share`].
    pub fn per_share<S: FeeSchedule>(
        &self,
        fees: &Fees<T, S>,
        price: T,
    ) -> Result<T, PowerFeeError> {
        self.on_fee(fees.per_share(Liquidity::Taker, price)?)
    }
}

impl TakerCashback<Decimal> {
    /// The same cash back over `f64`, for model arithmetic.
    ///
    /// # Errors
    ///
    /// [`PowerFeeError::FloatConversion`] when the share has no finite `f64`.
    pub fn to_f64(&self) -> Result<TakerCashback<f64>, PowerFeeError> {
        Ok(TakerCashback {
            share: f64::try_from(self.share).map_err(|_| PowerFeeError::FloatConversion)?,
            payout: self.payout,
            starts: self.starts,
            ends: self.ends,
        })
    }
}

/// A maker rebate at venue `S`: `rate × C × (p × (1 − p))^exponent`, credited at the fill and
/// rounded as the venue rounds its fees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MakerRebate<T = Decimal, S: FeeSchedule = Pmus> {
    rate: T,
    schedule: PhantomData<S>,
}

#[allow(clippy::neg_cmp_op_on_partial_ord)]
impl<T: FeeScalar, S: FeeSchedule> MakerRebate<T, S> {
    /// # Errors
    ///
    /// [`PowerFeeError::NegativeRate`] when `rate` is negative or not a number.
    pub fn new(rate: T) -> Result<Self, PowerFeeError> {
        if !(rate >= T::ZERO) {
            return Err(PowerFeeError::NegativeRate);
        }
        Ok(Self {
            rate,
            schedule: PhantomData,
        })
    }

    pub const fn rate(&self) -> T {
        self.rate
    }

    /// What a maker fill of `shares` at `price` is credited, rounded as the venue rounds.
    ///
    /// # Errors
    ///
    /// Non-positive `shares`, a `price` outside `[0, 1]`, or arithmetic that overflows.
    pub fn credit(&self, shares: T, price: T) -> Result<T, PowerFeeError> {
        if !(shares > T::ZERO) {
            return Err(PowerFeeError::NonPositiveShares);
        }
        if !(T::ZERO <= price && price <= T::ONE) {
            return Err(PowerFeeError::PriceOutOfBounds);
        }
        T::ONE
            .checked_sub(price)
            .and_then(|complement| price.checked_mul(complement))
            .and_then(|base| pow(base, S::EXPONENT))
            .and_then(|curve| self.rate.checked_mul(curve))
            .and_then(|per_share| per_share.checked_mul(shares))
            .map(|credit| credit.round_fee(S::ROUNDING))
            .ok_or(PowerFeeError::ArithmeticOverflow)
    }
}

impl Pmus {
    /// The API program's cash back: 70% of every API taker fee, paid weekly, over the program's
    /// window ([`Pmus::API_STARTS`] to [`Pmus::API_ENDS`]), straight trades only.
    pub const API_CASHBACK: TakerCashback = TakerCashback {
        share: Decimal::from_parts(70, 0, 0, false, 2),
        payout: Payout::Weekly,
        starts: Pmus::API_STARTS,
        ends: Pmus::API_ENDS,
    };

    /// Share of taker fees paid back weekly by taker rebate tier 1 to 6 (qualifying taker
    /// volume below $250k, to $1M, to $10M, to $25M, to $50M, above), set independently of the
    /// taker fee tier.
    pub const TAKER_CASHBACK_TIERS: [Decimal; 6] = [
        Decimal::ZERO,
        Decimal::from_parts(3, 0, 0, false, 2),
        Decimal::from_parts(5, 0, 0, false, 2),
        Decimal::from_parts(10, 0, 0, false, 2),
        Decimal::from_parts(15, 0, 0, false, 2),
        Decimal::from_parts(30, 0, 0, false, 2),
    ];

    /// Maker rebate rate by maker rebate tier 1 to 4 (qualifying maker volume below $10M, to
    /// $50M, to $200M, above), credited at the fill.
    pub const MAKER_REBATE_TIERS: [Decimal; 4] = [
        Decimal::ZERO,
        Decimal::from_parts(35, 0, 0, false, 4),
        Decimal::from_parts(70, 0, 0, false, 4),
        Decimal::from_parts(104, 0, 0, false, 4),
    ];
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fees::PmusFees;

    #[test]
    fn the_api_cash_back_matches_its_builder_and_its_window() {
        let built = TakerCashback::new(
            Decimal::new(70, 2),
            Payout::Weekly,
            1_795_237_260,
            1_798_779_600,
        );
        assert_eq!(built, Ok(Pmus::API_CASHBACK));
        let cash = Pmus::API_CASHBACK;
        assert!(!cash.active(1_795_237_259));
        assert!(cash.active(1_795_237_260));
        assert!(cash.active(1_798_779_599));
        assert_eq!(cash.share_at(1_798_779_600), Decimal::ZERO);
    }

    /// docs.polymarket.us/fees: 100 contracts at 0.50 pay 1.74; 70% of it comes back.
    #[test]
    fn cash_back_is_a_share_of_the_fee_charged_and_leaves_the_fee_alone() {
        let fee = Pmus::API
            .fee(Liquidity::Taker, Decimal::ONE_HUNDRED, Decimal::new(50, 2))
            .unwrap();
        assert_eq!(fee, Decimal::new(174, 2));
        assert_eq!(Pmus::API_CASHBACK.on_fee(fee), Ok(Decimal::new(1218, 3)));
        let model = Pmus::API.to_f64().unwrap();
        let cash = Pmus::API_CASHBACK.to_f64().unwrap();
        let back = cash.per_share(&model, 0.5).unwrap();
        assert!((back - 0.7 * 0.0695 * 0.25).abs() < 1e-15);
    }

    #[test]
    fn a_share_outside_one_or_a_backwards_window_is_refused() {
        assert!(TakerCashback::new(Decimal::new(11, 1), Payout::Weekly, 0, 1).is_err());
        assert!(TakerCashback::new(Decimal::NEGATIVE_ONE, Payout::Weekly, 0, 1).is_err());
        assert!(TakerCashback::new(Decimal::ONE, Payout::Weekly, 2, 1).is_err());
    }

    /// Tier 4 at 0.10: 0.0104 × 100 × 0.09 = 0.0936 → 0.09 credited, while the fee is zero.
    #[test]
    fn a_maker_rebate_is_credited_on_the_curve_apart_from_the_fee() {
        let rebate = MakerRebate::<Decimal, Pmus>::new(Pmus::MAKER_REBATE_TIERS[3]).unwrap();
        assert_eq!(
            rebate.credit(Decimal::ONE_HUNDRED, Decimal::new(10, 2)),
            Ok(Decimal::new(9, 2))
        );
        assert_eq!(
            PmusFees::standard().fee(Liquidity::Maker, Decimal::ONE_HUNDRED, Decimal::new(10, 2)),
            Ok(Decimal::ZERO)
        );
        assert!(MakerRebate::<Decimal, Pmus>::new(Decimal::new(-1, 4)).is_err());
    }
}
