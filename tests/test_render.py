from datetime import date
from pathlib import Path

from cjwmodule.arrow.testing import assert_result_equals, make_column, make_table
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.spec.testing import param_factory
from cjwmodule.testing.i18n import i18n_message
from cjwmodule.types import RenderError

from convertdatetodate import render_arrow_v1 as render

P = param_factory(Path(__name__).parent.parent / "convertdatetodate.yaml")


def test_no_columns_no_op():
    table = make_table(make_column("A", [1]))
    result = render(table, P(colnames=[]))
    assert_result_equals(result, ArrowRenderResult(table))


def test_day_to_day():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2000, 1, 1)], unit="day")),
            P(colnames=["A"], unit="day"),
        ),
        ArrowRenderResult(make_table(make_column("A", [date(2000, 1, 1)], unit="day"))),
    )


def test_day_to_week():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [date(2021, 4, 26), date(2021, 5, 2)], unit="day")
            ),
            P(colnames=["A"], unit="week"),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [date(2021, 4, 26), date(2021, 4, 26)], unit="week")
            )
        ),
    )


def test_day_to_month():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [date(2021, 4, 1), date(2021, 4, 30)], unit="day")
            ),
            P(colnames=["A"], unit="month"),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [date(2021, 4, 1), date(2021, 4, 1)], unit="month")
            )
        ),
    )


def test_day_to_quarter():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [date(2021, 1, 1), date(2021, 12, 31)], unit="day")
            ),
            P(colnames=["A"], unit="quarter"),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [date(2021, 1, 1), date(2021, 10, 1)], unit="quarter")
            )
        ),
    )


def test_day_to_year():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [date(2021, 1, 1), date(2021, 12, 31)], unit="day")
            ),
            P(colnames=["A"], unit="year"),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [date(2021, 1, 1), date(2021, 1, 1)], unit="year")
            )
        ),
    )


def test_week_to_day():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 26)], unit="week")),
            P(colnames=["A"], unit="day"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 4, 26)], unit="day")),
            [
                RenderError(
                    i18n_message(
                        "warning.choseStartOfUnit",
                        dict(column="A", fromUnit="week", toUnit="day"),
                    )
                )
            ],
        ),
    )


def test_week_to_week():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 26)], unit="week")),
            P(colnames=["A"], unit="week"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 4, 26)], unit="week")),
        ),
    )


def test_week_to_month():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 26)], unit="week")),
            P(colnames=["A"], unit="month"),
        ),
        ArrowRenderResult(
            make_table(),
            [
                RenderError(
                    i18n_message(
                        "error.weekUnitIncompatible",
                        dict(column="A", fromUnit="week", toUnit="month"),
                    )
                )
            ],
        ),
    )


def test_week_to_quarter():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 26)], unit="week")),
            P(colnames=["A"], unit="quarter"),
        ),
        ArrowRenderResult(
            make_table(),
            [
                RenderError(
                    i18n_message(
                        "error.weekUnitIncompatible",
                        dict(column="A", fromUnit="week", toUnit="quarter"),
                    )
                )
            ],
        ),
    )


def test_week_to_year():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 26)], unit="week")),
            P(colnames=["A"], unit="year"),
        ),
        ArrowRenderResult(
            make_table(),
            [
                RenderError(
                    i18n_message(
                        "error.weekUnitIncompatible",
                        dict(column="A", fromUnit="week", toUnit="year"),
                    )
                )
            ],
        ),
    )


def test_month_to_day():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="month")),
            P(colnames=["A"], unit="day"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 4, 1)], unit="day")),
            [
                RenderError(
                    i18n_message(
                        "warning.choseStartOfUnit",
                        dict(column="A", fromUnit="month", toUnit="day"),
                    )
                )
            ],
        ),
    )


def test_month_to_week():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="month")),
            P(colnames=["A"], unit="week"),
        ),
        ArrowRenderResult(
            make_table(),
            [
                RenderError(
                    i18n_message(
                        "error.weekUnitIncompatible",
                        dict(column="A", fromUnit="month", toUnit="week"),
                    )
                )
            ],
        ),
    )


def test_month_to_month():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="month")),
            P(colnames=["A"], unit="month"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 4, 1)], unit="month")),
        ),
    )


def test_month_to_quarter():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="month")),
            P(colnames=["A"], unit="quarter"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 4, 1)], unit="quarter")),
        ),
    )


def test_month_to_year():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="month")),
            P(colnames=["A"], unit="year"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 1, 1)], unit="year")),
        ),
    )


def test_quarter_to_day():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="quarter")),
            P(colnames=["A"], unit="day"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 4, 1)], unit="day")),
            [
                RenderError(
                    i18n_message(
                        "warning.choseStartOfUnit",
                        dict(column="A", fromUnit="quarter", toUnit="day"),
                    )
                )
            ],
        ),
    )


def test_quarter_to_week():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="quarter")),
            P(colnames=["A"], unit="week"),
        ),
        ArrowRenderResult(
            make_table(),
            [
                RenderError(
                    i18n_message(
                        "error.weekUnitIncompatible",
                        dict(column="A", fromUnit="quarter", toUnit="week"),
                    )
                )
            ],
        ),
    )


def test_quarter_to_month():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="quarter")),
            P(colnames=["A"], unit="month"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 4, 1)], unit="month")),
            [
                RenderError(
                    i18n_message(
                        "warning.choseStartOfUnit",
                        dict(column="A", fromUnit="quarter", toUnit="month"),
                    )
                )
            ],
        ),
    )


def test_quarter_to_quarter():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="quarter")),
            P(colnames=["A"], unit="quarter"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 4, 1)], unit="quarter")),
        ),
    )


def test_quarter_to_year():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 4, 1)], unit="quarter")),
            P(colnames=["A"], unit="year"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 1, 1)], unit="year")),
        ),
    )


def test_year_to_day():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 1, 1)], unit="year")),
            P(colnames=["A"], unit="day"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 1, 1)], unit="day")),
            [
                RenderError(
                    i18n_message(
                        "warning.choseStartOfUnit",
                        dict(column="A", fromUnit="year", toUnit="day"),
                    )
                )
            ],
        ),
    )


def test_year_to_week():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 1, 1)], unit="year")),
            P(colnames=["A"], unit="week"),
        ),
        ArrowRenderResult(
            make_table(),
            [
                RenderError(
                    i18n_message(
                        "error.weekUnitIncompatible",
                        dict(column="A", fromUnit="year", toUnit="week"),
                    )
                )
            ],
        ),
    )


def test_year_to_month():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 1, 1)], unit="year")),
            P(colnames=["A"], unit="month"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 1, 1)], unit="month")),
            [
                RenderError(
                    i18n_message(
                        "warning.choseStartOfUnit",
                        dict(column="A", fromUnit="year", toUnit="month"),
                    )
                )
            ],
        ),
    )


def test_year_to_quarter():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 1, 1)], unit="year")),
            P(colnames=["A"], unit="quarter"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 1, 1)], unit="quarter")),
            [
                RenderError(
                    i18n_message(
                        "warning.choseStartOfUnit",
                        dict(column="A", fromUnit="year", toUnit="quarter"),
                    )
                )
            ],
        ),
    )


def test_year_to_year():
    assert_result_equals(
        render(
            make_table(make_column("A", [date(2021, 1, 1)], unit="year")),
            P(colnames=["A"], unit="year"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [date(2021, 1, 1)], unit="year")),
        ),
    )
