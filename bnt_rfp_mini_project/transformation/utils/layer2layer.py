from caseconverter import snakecase
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from bnt_rfp_mini_project.conventions import (
    data_raw_path,
    data_cleansed_path,
    data_processed_path,
    data_output_path,
)
from bnt_rfp_mini_project.transformation.utils.create_logger import create_logger
from abc import abstractmethod
from typing import NoReturn


class Layer2Layer():
    """Parent class providing universal layer to layer tranformation patterns

    All children subclasses needs to implement their own transform function depending
    on the type of transformation.
    """

    input_layer_path: Path = None
    output_layer_path: Path = None

    input_filename: str = None
    input_filepath: Path = None
    input_abs_filepath: Path = None

    output_file_suffix: str = None
    output_file_format: str = "csv"
    output_filepath: Path = None
    output_abs_filepath: Path = None

    input_df: pd.DataFrame = None
    transformed_df: pd.DataFrame = None
    
    logger: logging.Logger = None

    def __init__(
        self,
        input_layer_path: Path,
        output_layer_path: Path,
        filepath: Path,
    ) -> None:
        """Init function

        Args:
            filepath (Path): path to input file in data layer (relative)
            input_layer_path (Path): absolute path to data layer of input file
            output_layer_path (Path): absolute path to data layer for output file
        """
        self.logger = create_logger(name=self.__class__.__name__)
        self.logger.debug(f"Start transforming {filepath}")
        self.input_layer_path = input_layer_path
        self.output_layer_path = output_layer_path
        self.input_filepath = filepath
        self.input_filename = self.input_filepath.name
        self.input_abs_filepath = Path(self.input_layer_path, self.input_filepath)

        output_filename = self._build_new_file_name(
            self.input_filepath.name, self.output_file_suffix
        )
        self.output_filepath = Path(
            self.input_filepath.parent,
            output_filename,
        )
        self.output_abs_filepath = Path(
            self.output_layer_path,
            self.input_filepath.parent,
            output_filename,
        )

    @staticmethod
    def _build_new_file_name(original_filename: str, new_suffix: str) -> str:
        """Build unified name based on selected implementation pattern of storage layer

        Applies snakecase conversion and adds layer suffix to file name.

        Args:
            original_filename (str): original name to modify
            new_suffix (str): layer suffix to add to name

        Returns:
            str: new file name compliant to conventions
        """
        filename_no_extension = original_filename.rsplit(".", 1)[0]
        filename_stripped_layer_suffix = (
            filename_no_extension.rsplit("_", 1)[0]
            if filename_no_extension.rsplit("_", 1)[1]
            in ["cleansed", "processed", "output"]
            else filename_no_extension
        )
        output_filename = "_".join(
            [snakecase(filename_stripped_layer_suffix), new_suffix]
        )
        return output_filename

    def read_input_file(self, filepath: Path = None) -> pd.DataFrame:
        """Read input file from specific location

        Returns:
            pd.DataFrame: DataFrame with loaded data
        """
        if filepath:
            read_filepath = filepath
        else:
            read_filepath = self.input_abs_filepath

        if read_filepath.suffix == ".csv":
            self.logger.debug(f"Reading `.csv` file {read_filepath}")
            self.input_df = pd.read_csv(read_filepath)
        elif read_filepath.suffix == ".xlsx":
            self.logger.debug(f"Reading `.xlsx` file {read_filepath}")
            self.input_df = pd.read_excel(read_filepath)
        return self.input_df

    def write_data_to_layer(self):
        """Write transformed DataFrame to target layer

        Raises:
            NotImplemented: When trying to save unsupported file format
        """
        if self.transformed_df is None:
            return

        # Create directories if they don't exist
        self.output_abs_filepath.parent.mkdir(parents=True, exist_ok=True)
        # Write fixed dataset to cleansed layer
        if self.output_file_format == "csv":
            self.output_filepath = self.output_filepath.with_suffix(f".csv")
            self.output_abs_filepath = self.output_abs_filepath.with_suffix(f".csv")
            self.logger.info(f"Writing output DataFrame as: {self.output_abs_filepath}")
            self.transformed_df.to_csv(self.output_abs_filepath, index=False)
        else:
            error_message = "Other file formats than 'csv' are not implemented yet"
            self.logger.error(error_message)
            raise NotImplemented(error_message)
        self.logger.info(f"Finished writing output DataFrame successfully.")

    @abstractmethod
    def transform(self) -> pd.DataFrame:
        """Abstract method which all childrens needs to implement

        Each layer has its own transformation logic, therefore each transformation
        implements its own transform function when inheriting from this class.

        Returns:
            pd.DataFrame: transformed data to write to layer
        """
        pass
