import pandas as pd
from helper_functions import dict_vals_to_list, read_yaml, split_strings

if __name__ == "__main__":
    PATH_TO_YAML = r"C:\Users\den\code\stackoverflow2020\data\inputs.yaml"
    PATH_TO_RAW_DATA = read_yaml(PATH_TO_YAML)["Original_Data_Path"]
    survey_df = pd.read_excel(PATH_TO_RAW_DATA)

    # Define special/new characters to remove/replace:
    # This requires a bit of iteration to check the data set for any weird characters that pop up
    # TODO: This might be solved more efficiently using decoding/encoding of characters using utf-8. Start by identifying what needs to be decoded, and then when exporting to csv, use encoding
    old_chars = ["â€™", "Im"]
    new_chars = ["'", "I am"]
    survey_df.replace(old_chars, new_chars, regex=True, inplace=True)

    # old_chars should not be in df, raise AssertionError if they are
    for char in old_chars:
        assert char not in survey_df, f"The character {char} was not removed from the data."

    # Begin the process of separating the strings by the chosen delimeter
    multi_responses = read_yaml(PATH_TO_YAML)["MultiResponse"]
    multi_responses_list = dict_vals_to_list(multi_responses)

    # Split the strings, raise AssertionError if splitting the strings fails
    DELIMITER = ";"
    survey_df = split_strings(survey_df, multi_responses_list, delimeter=DELIMITER)
    print(survey_df.head())
    assert DELIMITER not in survey_df, f"The delimeter {DELIMITER} was not split on properly."

    # Add a new ID just in case the old ones have errors (They did in the original data).
    survey_df["RespondentNew"] = survey_df.index

    # Export the data into serialized json to be input into create_reference_table.py
    to_path = r"C:\Users\den\code\stackoverflow2020\data\survey_results_public_2020_ready2process2.json"
    survey_df.to_json(to_path)
