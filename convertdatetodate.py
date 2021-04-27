import datetime
import time
from typing import Callable, Dict, Literal

import pyarrow as pa
import pyarrow.compute
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.i18n import trans
from cjwmodule.types import RenderError

Unit = Literal["day", "week", "month", "quarter", "year"]


StructTimeConverters: Dict[Unit, Callable[[time.struct_time], datetime.date]] = {
    "day": lambda st: datetime.date(st.tm_year, st.tm_mon, st.tm_mday),
    "week": lambda st: datetime.date.fromordinal(
        datetime.date(st.tm_year, st.tm_mon, st.tm_mday).toordinal() - st.tm_wday
    ),
    "month": lambda st: datetime.date(st.tm_year, st.tm_mon, 1),
    "quarter": lambda st: datetime.date(
        st.tm_year, [0, 1, 1, 1, 4, 4, 4, 7, 7, 7, 10, 10, 10][st.tm_mon], 1
    ),
    "year": lambda st: datetime.date(st.tm_year, 1, 1),
}


def convert_array(array: pa.TimestampArray, unit: Unit) -> pa.Date32Array:
    # ns => s
    unix_timestamps = pa.compute.multiply(
        array.view(pa.int32()).cast(pa.int64()), 86400
    )
    unix_timestamp_list = unix_timestamps.to_pylist()

    struct_time_to_date = StructTimeConverters[unit]

    # s => datetime.date
    date_list = [None] * len(unix_timestamp_list)
    for i, unix_timestamp in enumerate(unix_timestamp_list):
        if unix_timestamp is not None:
            struct_time = time.gmtime(unix_timestamp)
            date_list[i] = struct_time_to_date(struct_time)

    # datetime.date => pa.date32
    return pa.array(date_list, pa.date32())


def convert_chunked_array(
    chunked_array: pa.ChunkedArray, unit: Unit
) -> pa.ChunkedArray:
    chunks = (convert_array(chunk, unit) for chunk in chunked_array.chunks)
    return pa.chunked_array(chunks, pa.date32())


def render_arrow_v1(table: pa.Table, params, **kwargs):
    to_unit: Unit = params["unit"]

    warnings = []
    for colname in params["colnames"]:
        i = table.column_names.index(colname)

        from_unit = table.schema.field(i).metadata[b"unit"].decode("ascii")
        if from_unit == to_unit:
            continue
        elif (
            (to_unit == "day" and from_unit in {"week", "month", "quarter", "year"})
            or (to_unit == "month" and from_unit in {"quarter", "year"})
            or (to_unit == "quarter" and from_unit == "year")
        ):
            warnings.append(
                RenderError(
                    trans(
                        "warning.choseStartOfUnit",
                        "Column “{column}” does not specify a {toUnit}; we picked the first {toUnit} of the {fromUnit}",
                        dict(column=colname, fromUnit=from_unit, toUnit=to_unit),
                    )
                )
            )
            table = table.set_column(
                i,
                pa.field(colname, pa.date32(), metadata={"unit": to_unit}),
                table.columns[i],
            )
        elif (from_unit == "week" and to_unit != "day") or (
            to_unit == "week" and from_unit != "day"
        ):
            return ArrowRenderResult(
                pa.table({}),
                [
                    RenderError(
                        trans(
                            "error.weekUnitIncompatible",
                            "Column “{column}” cannot be converted to {toUnit}: weeks are not compatible with other units",
                            dict(column=colname, fromUnit=from_unit, toUnit=to_unit),
                        )
                    )
                ],
            )
        else:
            # The conversion isn't lossy
            table = table.set_column(
                i,
                pa.field(colname, pa.date32(), metadata={"unit": to_unit}),
                convert_chunked_array(table.columns[i], to_unit),
            )

    return ArrowRenderResult(table, warnings)
