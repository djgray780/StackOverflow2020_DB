import pandas as pd
from helper_functions import create_lookup_table, dict_vals_to_list, read_yaml

# Load the Ready To Process Data into a DataFrame.
PATH_TO_YAML = r"C:\Users\den\code\stackoverflow2020\data\inputs.yaml"
path = read_yaml(PATH_TO_YAML)["Path_R2P_Data"]
R2P_df = pd.read_json(path)

# Place the values of the MultiResponse key into a list to subset the dataframe on multiple responses.
multi_responses = read_yaml(PATH_TO_YAML)["MultiResponse"]
multi_response_columns = dict_vals_to_list(multi_responses)
multi_response_df = R2P_df[multi_response_columns]

# Generate the look up table using the subsetted dataframe and the .
multi_response_lookup_table = create_lookup_table(multi_response_df, multi_responses)

# Export the DataFrame object into a seriealized JSON file for further processing in proces_survey_data.py.
to_path = r"C:\Users\den\code\stackoverflow2020\data\multi_response_lookup_table2.json"
multi_response_lookup_table.to_json(to_path)
