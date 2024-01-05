use opcua::server::prelude::*;
use opcua::sync::RwLock;
use polars::export::chrono::{DateTime as PolarsDateTime, Utc as PolarsUtc};
use polars::export::chrono::{NaiveDateTime, Utc};
use polars::prelude::{col, lit, DataType as PolarsDataType, IntoLazy};
use polars_core::datatypes::AnyValue;
use polars_core::frame::DataFrame;
use polars_core::prelude::TimeUnit;
use std::collections::HashMap;
use std::ops::{Div, Mul};
use std::sync::Arc;

const OPCUA_AGG_FUNC_AVERAGE: u32 = 2342;
#[allow(dead_code)]
const OPCUA_AGG_FUNC_COUNT: u32 = 2352;
#[allow(dead_code)]
const OPCUA_AGG_FUNC_MINIMUM: u32 = 2346;
#[allow(dead_code)]
const OPCUA_AGG_FUNC_MAXIMUM: u32 = 2347;
const OPCUA_AGG_FUNC_TOTAL: u32 = 2344;

pub struct OPCUADataProvider {
    pub frames: HashMap<String, DataFrame>,
}

impl OPCUADataProvider {
    #[allow(dead_code)]
    pub fn new(frames: HashMap<String, DataFrame>) -> OPCUADataProvider {
        OPCUADataProvider { frames }
    }

    fn read(
        &self,
        nodes_to_read: &[HistoryReadValueId],
        aggregation_types: Option<Vec<NodeId>>,
        start_time: &DateTime,
        end_time: &DateTime,
        interval: Option<f64>,
    ) -> Result<Vec<HistoryReadResult>, StatusCode> {
        let mut results = vec![];
        for (i, n) in nodes_to_read.iter().enumerate() {
            let NodeId {
                namespace: _,
                identifier,
            } = &n.node_id;
            let idstring = if let Identifier::String(uas) = identifier {
                uas.to_string()
            } else {
                panic!("")
            };
            let mut df = self.frames.get(&idstring).unwrap().clone();
            let mut lf = df.lazy();
            if !start_time.is_null() {
                let start = start_time
                    .as_chrono()
                    .to_string()
                    .parse::<PolarsDateTime<PolarsUtc>>()
                    .unwrap()
                    .naive_utc();
                lf = lf.filter(col("timestamp").gt_eq(lit(start)));
            }
            if !end_time.is_null() {
                let stop = end_time
                    .as_chrono()
                    .to_string()
                    .parse::<PolarsDateTime<PolarsUtc>>()
                    .unwrap()
                    .naive_utc();
                lf = lf.filter(col("timestamp").lt_eq(lit(stop)));
            }
            if let Some(aggregation_types) = &aggregation_types {
                assert!(interval.unwrap() > 0.0);
                lf = lf.with_column(
                    col("timestamp")
                        .cast(PolarsDataType::Datetime(TimeUnit::Nanoseconds, None))
                        .cast(PolarsDataType::UInt64)
                        .alias("timestamp")
                        .div(lit(interval.unwrap() * 1_000_000.0))
                        .floor()
                        .mul(lit(interval.unwrap() * 1_000_000.0))
                        .cast(PolarsDataType::UInt64)
                        .cast(PolarsDataType::Datetime(TimeUnit::Nanoseconds, None)),
                );
                let lfgr = lf.group_by(["timestamp"]);
                let agg_func = aggregation_types.get(i).unwrap();
                assert_eq!(agg_func.namespace, 0);
                let mut agg_col = None;
                if let Identifier::Numeric(agg_func_i) = &agg_func.identifier {
                    agg_col = Some(match agg_func_i {
                        &OPCUA_AGG_FUNC_AVERAGE => col("value").mean(),
                        &OPCUA_AGG_FUNC_TOTAL => col("value").sum(),
                        _ => {
                            unimplemented!(
                                "We do not support this aggregation function: {}",
                                agg_func
                            )
                        }
                    });
                }
                lf = lfgr.agg([agg_col.unwrap().alias("value")]);
            }
            df = lf.collect().unwrap();
            let mut ts_iter = df.column("timestamp").unwrap().iter();
            let mut v_iter = df.column("value").unwrap().iter();
            let mut data_values = vec![];
            for _ in 0..df.height() {
                let value_variant = match v_iter.next().unwrap() {
                    AnyValue::Float64(f) => Variant::Double(f),
                    AnyValue::Int64(i) => Variant::Int64(i),
                    _ => {
                        todo!("Very rudimentary value type support!")
                    }
                };

                let naive_date_time = match ts_iter.next().unwrap() {
                    AnyValue::Datetime(number, timeunit, _) => match timeunit {
                        TimeUnit::Nanoseconds => NaiveDateTime::from_timestamp(
                            number / 1_000_000_000,
                            (number % 1_000_000_000) as u32,
                        ),
                        TimeUnit::Microseconds => NaiveDateTime::from_timestamp(
                            number / 1_000_000,
                            (number % 1_000_000) as u32,
                        ),
                        TimeUnit::Milliseconds => {
                            NaiveDateTime::from_timestamp(number / 1_000, (number % 1_000) as u32)
                        }
                    },
                    v => {
                        panic!("Something is not right! {}", v)
                    }
                };

                let timestamp = DateTime::from(DateTimeUtc::from_utc(naive_date_time, Utc));

                let dv = DataValue {
                    value: Some(value_variant),
                    status: None,
                    source_timestamp: Some(timestamp),
                    source_picoseconds: None,
                    server_timestamp: None,
                    server_picoseconds: None,
                };
                data_values.push(dv);
            }
            let h = HistoryData {
                data_values: Some(data_values),
            };
            let r = HistoryReadResult {
                status_code: StatusCode::Good,
                continuation_point: Default::default(),
                history_data: ExtensionObject::from_encodable(h.object_id(), &h),
            };
            results.push(r);
        }
        Ok(results)
    }
}

impl HistoricalDataProvider for OPCUADataProvider {
    fn read_raw_modified_details(
        &self,
        _address_space: Arc<RwLock<AddressSpace>>,
        request: ReadRawModifiedDetails,
        _timestamps_to_return: TimestampsToReturn,
        _release_continuation_points: bool,
        nodes_to_read: &[HistoryReadValueId],
    ) -> Result<Vec<HistoryReadResult>, StatusCode> {
        self.read(
            nodes_to_read,
            None,
            &request.start_time,
            &request.end_time,
            None,
        )
    }

    fn read_processed_details(
        &self,
        _address_space: Arc<RwLock<AddressSpace>>,
        request: ReadProcessedDetails,
        _timestamps_to_return: TimestampsToReturn,
        _release_continuation_points: bool,
        nodes_to_read: &[HistoryReadValueId],
    ) -> Result<Vec<HistoryReadResult>, StatusCode> {
        self.read(
            nodes_to_read,
            Some(request.aggregate_type.unwrap()),
            &request.start_time,
            &request.end_time,
            Some(request.processing_interval),
        )
    }
}
