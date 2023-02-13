from pathlib import Path
from bnt_rfp_mini_project.transformation.raw2cleansed.raw2cleansed_data1_covid_variants import (
    raw2cleansed_covid_variants,
)
from bnt_rfp_mini_project.schemas.covid_variants_country_indicators.schema_data_data1_covid_variants_evolution_cleansed import (
    schema as schema_data1,
)
from bnt_rfp_mini_project.transformation.raw2cleansed.raw2cleansed_data2_country_indicators import (
    raw2cleansed_country_indicators,
)
from bnt_rfp_mini_project.schemas.covid_variants_country_indicators.schema_data_data2_large_set_of_country_indicators_cleansed import (
    schema as schema_data2,
)

from bnt_rfp_mini_project.transformation.cleansed2processed.cleansed2processed_data1_covid_variants import (
    cleansed2processed_covid_variants,
)
from bnt_rfp_mini_project.transformation.cleansed2processed.cleansed2processed_data2_country_indicators import (
    cleansed2processed_country_indicators,
)

from bnt_rfp_mini_project.transformation.cleansed2processed.cleansed2processed_join_data1_data2 import (
    merge_continent_to_countries,
)
from bnt_rfp_mini_project.transformation.processed2output.processed2output_exercise_1 import (
    exercise_1,
)
from bnt_rfp_mini_project.transformation.processed2output.processed2output_exercise_2 import (
    exercise_2,
)


def main():
    print("raw2cleansed + cleansed2processed transformation")
    # Generic transformations
    cleansed_covid_variants_path = raw2cleansed_covid_variants(
        filepath=Path(
            "covid_variants_country_indicators",
            "data_Data1_Covid Variants evolution.csv",
        ),
        schema=schema_data1,
    )
    processed_covid_variants_path = cleansed2processed_covid_variants(
        filepath=cleansed_covid_variants_path
    )

    cleansed_country_indicators_path = raw2cleansed_country_indicators(
        filepath=Path(
            "covid_variants_country_indicators",
            "data_Data2_Large set of country indicators.xlsx",
        ),
        schema=schema_data2,
    )
    processed_country_indicators_path = cleansed2processed_country_indicators(
        cleansed_country_indicators_path
    )

    # Custom transformations
    merge_continent_to_countries()
    exercise_1()
    exercise_2()


if __name__ == "__main__":
    main()
