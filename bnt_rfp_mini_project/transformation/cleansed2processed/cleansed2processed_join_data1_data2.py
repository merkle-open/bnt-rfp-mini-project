import pandas as pd
from pathlib import Path
from bnt_rfp_mini_project.transformation.utils.create_logger import create_logger
from bnt_rfp_mini_project.conventions import data_processed_path, data_output_path

logger = create_logger(name=Path(__file__).name)
folder_name = "analysis_of_covid_variants_country_indicators"


def merge_continent_to_countries(folder_name: str = folder_name):
    # Read input dataframes
    logger.info("Read input DataFrames")
    evolution_df = pd.read_csv(
        Path(
            data_processed_path,
            "covid_variants_country_indicators",
            "data_data1_covid_variants_evolution_processed.csv",
        )
    )

    continents_df = pd.read_csv(
        Path(
            data_processed_path,
            "covid_variants_country_indicators",
            "data_data2_large_set_of_country_indicators_processed.csv",
        )
    )

    # Filter only data with available sequences
    evolution_df = evolution_df[evolution_df.num_sequences > 0]

    # Disregards unknown variants
    evolution_df = evolution_df[evolution_df.variant != "non_who"]
    evolution_df = evolution_df[evolution_df.variant != "others"]

    evolution_df["date"] = pd.to_datetime(evolution_df["date"], format="%Y-%m-%d")
    evolution_df["variant"] = pd.Categorical(
        evolution_df["variant"], categories=evolution_df["variant"].unique()
    )
    
    # Filter only continent and location information for joining later
    continents_countries_df = continents_df[["location", "continent"]].drop_duplicates()

    # Merge continent information to countries
    evolution_df = evolution_df.merge(
        right=continents_countries_df, how="left", on="location"
    )

    # Sort by date
    evolution_df.sort_values(by=["date"], inplace=True)

    # The first day in the dataset is day "0". All other dates are represented by the number of days
    # between the first day and the actual date. The result will be saved in the column "days".
    variant_first_occurance_df = (
        evolution_df.groupby(["variant"])[["date"]]
        .min()
        .reset_index()
        .rename(columns={"date": "date_first_occurance"})
    )
    evolution_df = evolution_df.merge(variant_first_occurance_df, how="left")

    evolution_df["days"] = (
        evolution_df["date"] - evolution_df["date_first_occurance"]
    ).dt.days

    # Grouping the dataset by location and variant, allows us to find the first occurance of variant in a country.
    intermediate_result_df = (
        evolution_df.groupby(["location", "variant"], as_index=False)[["days"]]
        .min()
        .merge(right=continents_countries_df, how="left", on="location")
    )

    # Write down results we have for next steps
    Path(data_processed_path, folder_name).mkdir(parents=True, exist_ok=True)
    Path(data_output_path, folder_name).mkdir(parents=True, exist_ok=True)
    
    intermediate_result_df_output_filename = Path(
        data_output_path, folder_name, "covid_first_hit_per_country_variant.csv"
    )
    
    logger.info(f"Writing output DataFrame as: {intermediate_result_df_output_filename}")
    
    intermediate_result_df.to_csv(
        intermediate_result_df_output_filename,
        index=False,
    )
    
    continents_countries_df_output_filename = Path(
        data_processed_path, folder_name, "continent_countries_mapping.csv"
    )
    
    logger.info(f"Writing output DataFrame as: {continents_countries_df_output_filename}")
    
    continents_countries_df.to_csv(
        continents_countries_df_output_filename,
        index=False,
    )


if __name__ == "__main__":
    merge_continent_to_countries()
