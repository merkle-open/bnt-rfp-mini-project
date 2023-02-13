import pandas as pd
import numpy as np
import pandera as pa
from pathlib import Path
from bnt_rfp_mini_project.transformation.utils.create_logger import create_logger
from bnt_rfp_mini_project.transformation.utils.raw2cleansed import Raw2Cleansed
from bnt_rfp_mini_project.schemas.covid_variants_country_indicators.schema_data_data1_covid_variants_evolution_cleansed import schema


logger = create_logger(name=Path(__file__).name)


def raw2cleansed_covid_variants(filepath: Path = None, schema: pa.DataFrameSchema = None):
    class Raw2CleansedCovidVariants(Raw2Cleansed):
        def cleanse_data(self):
            logger.info("Cleansing corrupted data")
            filepath = self.input_abs_filepath

            def remove_wrapper_quotes(string_with_quotes):
                string_without_quotes = string_with_quotes.removeprefix(
                    '"'
                ).removesuffix('"')
                return string_without_quotes

            self.cleansed_df = pd.read_csv(filepath, quoting=3, delimiter=",")
            self.cleansed_df = self.cleansed_df.apply(
                np.vectorize(remove_wrapper_quotes)
            )
            self.cleansed_df.rename(
                columns=lambda x: remove_wrapper_quotes(x), inplace=True
            )
            self.cleansed_df["num_sequences"] = pd.to_numeric(
                self.cleansed_df["num_sequences"], errors="raise"
            )
            self.cleansed_df["perc_sequences"] = pd.to_numeric(
                self.cleansed_df["perc_sequences"], errors="raise"
            )
            self.cleansed_df["num_sequences_total"] = pd.to_numeric(
                self.cleansed_df["num_sequences_total"], errors="raise"
            )
            self.cleansed_df["date"] = pd.to_datetime(
                self.cleansed_df["date"], errors="raise", dayfirst=True
            )
            return self.cleansed_df

    transformation = Raw2CleansedCovidVariants(filepath=filepath)
    
    # Define schema for automatic validation of data consistency
    transformation.schema = schema
    
    transformation.transform()
    return transformation.output_filepath


if __name__ == "__main__":
    raw2cleansed_covid_variants(
        filepath=Path(
            "covid_variants_country_indicators",
            "data_Data1_Covid Variants evolution.csv",
        ),
        schema=schema
    )
