import pandas as pd
from pandera.schemas import DataFrameSchema
from pandera.errors import SchemaError
from pathlib import Path
from bnt_rfp_mini_project.conventions import (
    data_raw_path,
    data_rejected_path,
)
from .layer2layer import Layer2Layer


class Raw2Rejected(Layer2Layer):
    """Implements transformation between "raw" and "rejected" layers

    Inherits from paret Layer2Layer class
    """

    output_file_suffix = "rejected"
    output_file_format = "csv"
    
    schema: DataFrameSchema = None

    def __init__(self, filepath: Path = None) -> None:
        super().__init__(
            filepath=filepath,
            input_layer_path=data_raw_path,
            output_layer_path=data_rejected_path,
        )

    def transform(self) -> pd.DataFrame:
        """Main transformation function for layer logic applying cleansing on the input data

        Returns:
            pd.DataFrame: cleansed and transformed input DataFrame
        """
        self.logger.info("Starting transformation from raw to cleansed layer")
        # Read data
        self.read_input_file()

        # Do custom tranformation
        self.transformed_df = self.input_df

        # Write output data
        self.write_data_to_layer()
        return self.transformed_df
