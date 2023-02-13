import pandera as pa
import pandas as pd
from typing import Union


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
    },
    strict=False,  # Check only some columns
)
