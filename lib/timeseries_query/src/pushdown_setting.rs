use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub fn all_pushdowns() -> HashSet<PushdownSetting> {
    [PushdownSetting::GroupBy, PushdownSetting::ValueConditions].into()
}

#[derive(Hash, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum PushdownSetting {
    ValueConditions,
    GroupBy,
}
