from pathlib import Path
from bnt_rfp_mini_project.transformation.utils.cleansed2processed import Cleansed2Processed


def cleansed2processed_country_indicators(filepath: Path):
    transformation = Cleansed2Processed(filepath=filepath)
    transformation.transform()
    return transformation.output_filepath


if __name__ == "__main__":
    cleansed2processed_country_indicators(
        filepath=Path(
            "covid_variants_country_indicators",
            "data_data2_large_set_of_country_indicators_cleansed.csv",
        )
    )
