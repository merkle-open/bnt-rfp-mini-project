import pandas as pd
from pandera.schemas import DataFrameSchema
from pandera.errors import SchemaError
from pathlib import Path
from bnt_rfp_mini_project.conventions import (
    data_raw_path,
    data_cleansed_path,
    data_rejected_path,
)
from .layer2layer import Layer2Layer
from .raw2rejected import Raw2Rejected


class Raw2Cleansed(Layer2Layer):
    """Implements transformation between "raw" and "cleansed" layers

    Inherits from paret Layer2Layer class
    """

    output_file_suffix = "cleansed"
    output_file_format = "csv"

    schema: DataFrameSchema = None

    def __init__(self, filepath: Path = None) -> None:
        super().__init__(
            filepath=filepath,
            input_layer_path=data_raw_path,
            output_layer_path=data_cleansed_path,
        )

    def cleanse_data(self) -> pd.DataFrame:
        """Override this function if custom cleansing logic is necessary

        By default, it performs no cleansing, it just stores input DataFrame as output DataFrame

        Returns:
            pd.DataFrame: cleansed DataFrame
        """
        self.logger.info("Starting cleansing of data")
        cleansed_df = self.input_df
        return cleansed_df

    def transform(self) -> pd.DataFrame:
        """Main transformation function for layer logic applying cleansing on the input data

        Returns:
            pd.DataFrame: cleansed and transformed input DataFrame
        """
        self.logger.info("Starting transformation from raw to cleansed layer")
        # Read data
        self.read_input_file()

        # Do custom tranformation
        self.transformed_df = self.cleanse_data()
        
        # Validate schema if is provided
        if self.schema is not None:
            try:
                self.transformed_df = self.schema(self.transformed_df)
            except SchemaError as schema_error:
                self.logger.warning(
                    f"Schema validation found inconsistencies in the data."
                )
                self.logger.warning(schema_error)
                self.logger.warning(f"DataFrame will be stored in 'rejected' folder.")
                transformation_rejected = Raw2Rejected(filepath=self.input_filepath)
                transformation_rejected.transform()
                with open(
                    Path(
                        data_rejected_path,
                        transformation_rejected.output_filepath.with_suffix(
                            ".schemaerror"
                        ),
                    ),
                    "w",
                ) as schema_log:
                    schema_log.write(str(schema_error))

        # Write output data
        self.write_data_to_layer()
        return self.transformed_df
