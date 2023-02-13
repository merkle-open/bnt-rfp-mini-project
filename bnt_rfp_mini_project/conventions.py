from pathlib import Path
from configparser import ConfigParser


# Boiler plate for parsing conventions.ini file
_base_path_to_project = Path(__file__).parent
_base_path_to_repository = _base_path_to_project.parent
_config = ConfigParser()

# Path to config files
path_to_configs = Path(_base_path_to_project, "config")

# Parse convention values which can be reused later in Python code
_config.read(Path(_base_path_to_project, "config", "conventions.ini"))

data_raw_path = Path(
    _base_path_to_repository, _config["data"]["data"], _config["layers"]["raw"]
)
data_cleansed_path = Path(
    _base_path_to_repository, _config["data"]["data"], _config["layers"]["cleansed"]
)
data_processed_path = Path(
    _base_path_to_repository, _config["data"]["data"], _config["layers"]["processed"]
)
data_output_path = Path(
    _base_path_to_repository, _config["data"]["data"], _config["layers"]["output"]
)
data_rejected_path = Path(
    _base_path_to_repository, _config["data"]["data"], _config["layers"]["rejected"]
)

if __name__ == "__main__":
    print(
        [
            data_raw_path,
            data_cleansed_path,
            data_processed_path,
            data_output_path,
            path_to_configs,
            Path(path_to_configs, "new.ini"),
        ]
    )
