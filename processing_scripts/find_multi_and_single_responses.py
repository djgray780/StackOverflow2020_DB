import helper_functions as h
import pandas as pd

# Read in the path from the inputs.yaml file and load into a dataframe
PATH_TO_YAML = r"C:\Users\den\code\stackoverflow2020\data\inputs.yaml"
PATH_TO_RAW_DATA = h.read_yaml(PATH_TO_YAML)["Original_Data_Path"]
survey_df = pd.read_excel(PATH_TO_RAW_DATA)

# Figure out which columns have single and multiple responses
multi_cols = h.identify_multi_response_columns(survey_df, delimiter=";")
single_cols = h.identify_single_response_columns(survey_df, delimiter=";")

# Raise AssertionError if a column label appears in both multi_cols and single_cols
assert len(set(multi_cols).intersection(set(single_cols))) == 0, "At least one field appears in both multi and single response columns. "

# Print results to terminal and use this information to organize new data to be added into inputs.yaml
print("")
print(f"These are the multiple response columns: {multi_cols}")
print("")
print(f"These are the single response columns: {single_cols}")
print("")
