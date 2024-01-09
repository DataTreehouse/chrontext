// From: https://github.com/pola-rs/polars/blob/master/py-polars/src/arrow_interop/to_py.rs
// Edited to remove dependencies on py-polars
// Original licence:
//
// Copyright (c) 2020 Ritchie Vink
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::collections::HashMap;
use polars_core::frame::ArrowChunk;
use polars_core::prelude::{ArrayRef, ArrowField};
use polars_core::utils::arrow::ffi;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyList;

const SEMANTIC_DATAFRAME_CODE:&str = r#"
from typing import Dict
from polars import DataFrame
from polars.datatypes import N_INFER_DEFAULT
from polars.type_aliases import SchemaDefinition, FrameInitTypes, SchemaDict, Orientation

class SemanticDataFrame(DataFrame):
    """
    A Polars DataFrame but with an extra field rdf_datatypes containing the RDF data types of the columns.
    """
    def __init__(
            self,
            data: FrameInitTypes | None = None,
            schema: SchemaDefinition | None = None,
            *,
            schema_overrides: SchemaDict | None = None,
            orient: Orientation | None = None,
            infer_schema_length: int | None = N_INFER_DEFAULT,
            nan_to_null: bool = False,
            rdf_datatypes: Dict[str, str]
    ):
        """
        The signature of this method is from Polars, license can be found in the file ../../../LICENSING/POLARS_LICENSE
        SemanticDataFrames should be instantiated using the SemanticDataFrame.from_df()-method.
        This method mainly exists as a placeholder to make autocomplete work.
        """
        super().__init__(data, schema, schema_overrides=schema_overrides, orient=orient,
                         infer_schema_length=infer_schema_length, nan_to_null=nan_to_null)
        self.rdf_datatypes = rdf_datatypes

    @staticmethod
    def from_df(df: DataFrame, rdf_datatypes: Dict[str, str]) -> "SemanticDataFrame":
        """

        :param rdf_datatypes:
        :return:
        """
        df.__class__ = SemanticDataFrame
        df.init_rdf_datatypes(rdf_datatypes)
        return df

    def init_rdf_datatypes(self, map: Dict[str, str]):
        self.rdf_datatypes = map
"#;

/// Arrow array to Python.
pub(crate) fn to_py_array(array: ArrayRef, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let schema = Box::new(ffi::export_field_to_c(&ArrowField::new(
        "",
        array.data_type().clone(),
        true,
    )));
    let array = Box::new(ffi::export_array_to_c(array));

    let schema_ptr: *const ffi::ArrowSchema = &*schema;
    let array_ptr: *const ffi::ArrowArray = &*array;

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

/// RecordBatch to Python.
pub(crate) fn to_py_rb(
    rb: &ArrowChunk,
    names: &[&str],
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    let mut arrays = Vec::with_capacity(rb.len());

    for array in rb.columns() {
        let array_object = to_py_array(array.clone(), py, pyarrow)?;
        arrays.push(array_object);
    }

    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (arrays, names.to_vec()))?;

    Ok(record.to_object(py))
}

pub fn to_py_df(
    rb: &ArrowChunk,
    names: &[&str],
    py: Python,
    pyarrow: &PyModule,
    polars: &PyModule,
    types: HashMap<String, String>
) -> PyResult<PyObject> {
    let py_rb = to_py_rb(rb, names, py, pyarrow)?;
    let py_rb_list = PyList::empty(py);
    py_rb_list.append(py_rb)?;
    let py_table = pyarrow
        .getattr("Table")?
        .call_method1("from_batches", (py_rb_list,))?;
    let py_table = py_table.to_object(py);
    let df = polars.call_method1("from_arrow", (py_table,))?;
    let semantic_dataframe = PyModule::from_code(py, SEMANTIC_DATAFRAME_CODE, "semantic_dataframe.py", "semantic_dataframe")?;
    let df = semantic_dataframe.getattr("SemanticDataFrame")?.getattr("from_df")?.call1((df, types))?;
    Ok(df.to_object(py))
}
