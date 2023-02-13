import pandera as pa
from pathlib import Path
from bnt_rfp_mini_project.transformation.utils.raw2cleansed import Raw2Cleansed
from bnt_rfp_mini_project.schemas.covid_variants_country_indicators.schema_data_data2_large_set_of_country_indicators_cleansed import schema


def raw2cleansed_country_indicators(filepath: Path, schema: pa.DataFrameSchema = None):
    transformation = Raw2Cleansed(filepath=filepath)
    # Define schema for automatic validation of data consistency
    transformation.schema = schema
    
    transformation.transform()
    return transformation.output_filepath


if __name__ == "__main__":
    raw2cleansed_country_indicators(
        filepath=Path(
            "covid_variants_country_indicators",
            "data_Data2_Large set of country indicators.xlsx",
        ),
        schema=schema
    )
