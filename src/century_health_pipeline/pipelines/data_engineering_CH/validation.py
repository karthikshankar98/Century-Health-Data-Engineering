from great_expectations.dataset import PandasDataset
from great_expectations.data_context import DataContext
import json
from pathlib import Path

def validate_master_data(data, output_dir="data/04_validation" ,output_file="master_data_validation.json"):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = Path(output_dir) / output_file

    context = DataContext()

    expectation_suite = context.get_expectation_suite("master_data_expectations")

    dataset = PandasDataset(data)

    validation_results = dataset.validate(expectation_suite=expectation_suite)
    validation_results_dict = validation_results.to_json_dict()

    with open(output_path, "w") as f:
        json.dump(validation_results_dict, f, indent=2)

    return validation_results_dict


