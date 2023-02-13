import pandas as pd
from pathlib import Path
from bnt_rfp_mini_project.conventions import (
    data_cleansed_path,
    data_processed_path,
)
from .layer2layer import Layer2Layer


class Cleansed2Processed(Layer2Layer):
    """Implements transformation between "cleansed" and "processed" layers

    Inherits from paret Layer2Layer class
    """

    output_file_suffix = "processed"
    output_file_format = "csv"

    def __init__(self, filepath: Path = None) -> None:
        super().__init__(
            filepath=filepath,
            input_layer_path=data_cleansed_path,
            output_layer_path=data_processed_path,
        )

    def transform(self):
        """Main transformation function for layer logic applying tranformation on the input data

        Returns:
            pd.DataFrame: transformed input DataFrame
        """
        self.logger.info("Starting transformation from cleansed to output layer")
        # Read data
        self.read_input_file()

        # Do custom tranformation
        self.transformed_df = self.input_df

        # Write output data
        self.write_data_to_layer()
        return self.transformed_df
