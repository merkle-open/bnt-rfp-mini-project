import pandas as pd
from pathlib import Path
from scipy.spatial.distance import squareform
from scipy.spatial.distance import pdist
from sklearn.impute import KNNImputer
from bnt_rfp_mini_project.transformation.utils.create_logger import create_logger
from bnt_rfp_mini_project.conventions import data_processed_path, data_output_path


logger = create_logger(name=Path(__file__).name)
folder_name = "analysis_of_covid_variants_country_indicators"


def exercise_2(folder_name: str = folder_name):
    # Read intermediate tables
    logger.info("Read input DataFrames")
    first_hit_df = pd.read_csv(
        Path(data_output_path, folder_name, "covid_first_hit_per_country_variant.csv")
    )

    continents_df = pd.read_csv(
        Path(data_processed_path, folder_name, "continent_countries_mapping.csv")
    )

    top5_first_hit_df = pd.read_csv(
        Path(
            data_output_path, folder_name, "covid_first_hit_countries_by_continent.csv"
        )
    )

    countries_df = first_hit_df.pivot(
        index="location", columns="variant", values="days"
    )

    imputer = KNNImputer(n_neighbors=5, metric="nan_euclidean")

    df_filled = imputer.fit_transform(countries_df)

    pairwise_df = pd.DataFrame(
        squareform(pdist(df_filled)),
        columns=countries_df.index,
        index=countries_df.index,
    ).rename_axis(["location_from"])

    dist_df = pd.DataFrame(pdist(df_filled))
    dist_df = (
        pd.DataFrame(pairwise_df.stack())
        .reset_index()
        .rename(columns={"location": "location_to"})
    )
    dist_df = dist_df.rename(
        columns={"location": "location_to", dist_df.columns[2]: "distance"}
    )
    dist_df = dist_df[dist_df["distance"] > 0]

    # # %%
    # plt.figure(figsize=(10,10))
    # sns.heatmap(
    #     pairwise_df,
    #     cmap='OrRd',
    #     linewidth=1)

    dist_df = (
        dist_df.merge(
            right=continents_df,
            how="left",
            left_on="location_from",
            right_on="location",
        )
        .drop(["location"], axis=1)
        .rename(columns={"continent": "continent_from"})
        .merge(
            right=continents_df, how="left", left_on="location_to", right_on="location"
        )
        .drop(["location"], axis=1)
        .rename(columns={"continent": "continent_to"})
    )
    dist_df.sort_values(["distance"], inplace=True)

    dist_same_continent_df = dist_df[
        dist_df["continent_from"] == dist_df["continent_to"]
    ][["location_from", "location_to", "distance"]]
    dist_same_continent_df["continent"] = "same"
    dist_different_continent_df = dist_df[
        dist_df["continent_from"] != dist_df["continent_to"]
    ][["location_from", "location_to", "distance"]]
    dist_different_continent_df["continent"] = "different"

    intermediate_result_df = pd.concat(
        [
            dist_same_continent_df.groupby("location_from").head(5),
            dist_different_continent_df.groupby("location_from").head(5),
        ]
    )

    result_df = intermediate_result_df[
        intermediate_result_df["location_from"].isin(
            top5_first_hit_df["location"].unique()
        )
    ].sort_values(["location_from", "continent", "distance"])

    # Write down results we have for next steps
    Path(data_output_path, folder_name).mkdir(parents=True, exist_ok=True)
    pairwise_df_output_filename = Path(
        data_output_path, folder_name, "covid_countries_similarity.csv"
    )

    logger.info(f"Writing output DataFrame as: {pairwise_df_output_filename}")

    pairwise_df.to_csv(
        pairwise_df_output_filename,
    )

    result_df_output_filename = Path(
        data_output_path, folder_name, "top_5_similar_countries.csv"
    )

    logger.info(f"Writing output DataFrame as: {result_df_output_filename}")
    result_df.to_csv(
        result_df_output_filename,
        index=False,
    )


if __name__ == "__main__":
    exercise_2()
