from kedro.pipeline import Pipeline, node
from .nodes import clean_patients, clean_symptoms, clean_encounters, clean_conditions, clean_medications, merge_data
from .validation import validate_master_data

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=clean_patients,
                inputs=["raw_patients", "raw_patient_gender"],
                outputs="patients_table",
                name="clean_patients_node",
            ),

            node(
                func=clean_symptoms,
                inputs="raw_symptoms",
                outputs="symptoms_table",
                name="clean_symptoms_node",
            ),

            node(
                func=clean_encounters,
                inputs="raw_encounters",
                outputs="encounters_table",
                name="clean_encounters_node",
            ),

            node(
                func=clean_conditions,
                inputs="raw_conditions",
                outputs="conditions_table",
                name="clean_conditions_node",
            ),

            node(
                func=clean_medications,
                inputs="raw_medications",
                outputs="medications_table",
                name="clean_medications_node",
            ),

            node(
                func=merge_data,
                inputs=["conditions_table","encounters_table","patients_table","medications_table","symptoms_table"],
                outputs="master_data",
                name="merge_data_node",
            ),

            node(
                func=validate_master_data,
                inputs= "master_data",
                outputs=None,
                name="validate_master_node",
            ),


        ]
    )
