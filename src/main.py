"""Main script"""
import os
from pathlib import Path
from src.pipeline import Pipeline
from src import utils


def main():
    """Main function"""
    utils.initialize_coloredlog()
    utils.initialize_rich()
    utils.initialize_logging()
    # Project path
    project_dir = str(Path(__file__).resolve().parents[1])
    # Load enviromental variables
    env_var = utils.load_env_variables(project_dir)
    # Load paramenters
    params = utils.load_json(os.path.join(project_dir, "parameters", "parameters.json"))
    params["global"]["root_path"] = env_var["root_path"]
    # Load switchers
    switchers = utils.load_json(
        os.path.join(project_dir, "parameters", "switchers.json")
    )
    # Creates and run the census processing pipeline
    pipeline_locations = Pipeline("census", params, switchers["census"])
    pipeline_locations.run()


if __name__ == "__main__":
    main()
