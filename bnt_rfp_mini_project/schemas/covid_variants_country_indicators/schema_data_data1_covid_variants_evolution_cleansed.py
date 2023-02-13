import pandera as pa
import pandas as pd


schema = pa.DataFrameSchema(
    {
        "location": pa.Column(
            str,
            checks=[
                pa.Check(
                    lambda x: len(str(x)) > 1,
                    raise_warning=True,
                    element_wise=True,
                    name="No comma in value",
                )
            ],
        ),
        "date": pa.Column(
            pa.DateTime,
            pa.Check(
                lambda x: pd.Timestamp("2025-01-01") > x > pd.Timestamp("2010-01-01"),
                raise_warning=True,
                element_wise=True,
                name="DateTime in reasonable timeframe 2025-01-01 > x > 2010-01-01",
            ),
        ),
        "variant": pa.Column(str),
        "num_sequences": pa.Column(
            int,
            pa.Check(
                lambda x: x >= 0,
                raise_warning=True,
                element_wise=True,
                name="Positive integer",
            ),
        ),
        "perc_sequences": pa.Column(
            float,
            pa.Check(
                lambda x: 100.0 >= x >= 0.0,
                raise_warning=False,
                element_wise=True,
                name="Percentage values only 0.0-100.0",
            ),
        ),
        "num_sequences_total": pa.Column(
            int,
            pa.Check(
                lambda x: x >= 0,
                raise_warning=True,
                element_wise=True,
                name="Positive integer",
            ),
        ),
    }
)
