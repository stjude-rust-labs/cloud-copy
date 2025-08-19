//! Utility for random alphanumeric string generation.

use std::fmt;

use rand::Rng as _;

/// An alphanumeric string generator.
///
/// Every display of the generator will create a new random alphanumeric string.
///
/// By default, the displayed string will use all alphanumeric characters.
///
/// Use the alternate format (i.e. `{:#}`) to use only lowercase alphanumeric
/// characters.
#[derive(Clone, Copy)]
pub struct Alphanumeric {
    /// The length of the alphanumeric string.
    length: usize,
}

impl Alphanumeric {
    /// Creates a new alphanumeric string generator with the given string
    /// length.
    pub fn new(length: usize) -> Self {
        Self { length }
    }
}

impl Default for Alphanumeric {
    fn default() -> Self {
        Self { length: 12 }
    }
}

impl fmt::Display for Alphanumeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lowercase = f.alternate();
        for c in rand::rng()
            .sample_iter(&rand::distr::Alphanumeric)
            .take(self.length)
            .map(|c| if lowercase { c.to_ascii_lowercase() } else { c })
        {
            write!(f, "{c}", c = c as char)?;
        }

        Ok(())
    }
}
