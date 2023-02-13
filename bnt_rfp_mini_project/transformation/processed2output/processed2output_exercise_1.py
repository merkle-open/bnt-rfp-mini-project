import pandas as pd
from pathlib import Path
from bnt_rfp_mini_project.transformation.utils.create_logger import create_logger
from bnt_rfp_mini_project.conventions import data_processed_path, data_output_path

logger = create_logger(name=Path(__file__).name)
folder_name = "analysis_of_covid_variants_country_indicators"


def exercise_1(folder_name: str = folder_name):
    # Read intermediate tables
    logger.info("Read input DataFrames")
    intermediate_result_df = pd.read_csv(
        Path(
            data_output_path, folder_name, "covid_first_hit_per_country_variant.csv"
        )
    )
    continents_countries_df = pd.read_csv(
        Path(data_processed_path, folder_name, "continent_countries_mapping.csv")
    )

    # The median of first occurances of variants of a country tells us ...
    result_df = (
        intermediate_result_df.groupby(["location"])
        .aggregate({"days": "median"})
        .reset_index()
        .merge(right=continents_countries_df, how="left", on="location")
    )

    result_top5_per_continent = (
        result_df.sort_values(by=["continent", "days"])
        .groupby("continent")
        .head(5)[["continent", "location", "days"]]
    )

    # Write down results we have for next steps
    Path(data_output_path, folder_name).mkdir(parents=True, exist_ok=True)
    result_top5_per_continent_output_filepath = Path(
        data_output_path, folder_name, "covid_first_hit_countries_by_continent.csv"
    )
    logger.info(f"Writing output DataFrame as: {result_top5_per_continent_output_filepath}")
    result_top5_per_continent[["continent", "location", "days"]].to_csv(
        result_top5_per_continent_output_filepath,
        index=False,
    )


if __name__ == "__main__":
    exercise_1()
