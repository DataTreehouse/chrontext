// Based on https://github.com/GreptimeTeam/greptimedb/blob/05751084e7bbfc5e646df7f51bb7c3e5cbf16d58/src/session/src/session_config.rs
// and https://github.com/GreptimeTeam/greptimedb/blob/05751084e7bbfc5e646df7f51bb7c3e5cbf16d58/src/servers/src/postgres/types/datetime.rs
//
// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;

// Refers to: https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DATESTYLE
#[derive(Default, PartialEq, Eq, Clone, Copy, Debug)]
pub enum PGDateOrder {
    #[default]
    MDY,
    DMY,
    YMD,
}

impl Display for PGDateOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PGDateOrder::MDY => write!(f, "MDY"),
            PGDateOrder::DMY => write!(f, "DMY"),
            PGDateOrder::YMD => write!(f, "YMD"),
        }
    }
}

#[derive(Default, PartialEq, Eq, Clone, Copy, Debug)]
pub enum PGDateTimeStyle {
    #[default]
    ISO,
    SQL,
    Postgres,
    German,
}

impl Display for PGDateTimeStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PGDateTimeStyle::ISO => write!(f, "ISO"),
            PGDateTimeStyle::SQL => write!(f, "SQL"),
            PGDateTimeStyle::Postgres => write!(f, "Postgres"),
            PGDateTimeStyle::German => write!(f, "German"),
        }
    }
}
