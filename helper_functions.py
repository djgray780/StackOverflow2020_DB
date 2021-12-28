from copy import deepcopy

import numpy as np
import pandas as pd
import yaml


def read_yaml(file_path):
    """Read and load a yaml file."""
    stream = open(file_path, "r")
    return yaml.load(stream, Loader=yaml.FullLoader)


def identify_multi_response_columns(dataframe, delimiter):
    """Identify all of the columns where multiple responses are permitted."""
    survey_data = dataframe.copy()
    columns = survey_data.columns

    multiple_response_columns = []
    for column in columns:
        for response in survey_data[column]:
            # If the response is dtype string and contains the delimiter then there are at least two responses.
            if isinstance(response, str) and f"{delimiter}" in response:
                multiple_response_columns.append(column)
                break
    return multiple_response_columns


def identify_single_response_columns(dataframe, delimiter):
    """Identify all of the columns where single responses are permitted."""
    columns = dataframe.columns
    multi_response_columns = identify_multi_response_columns(dataframe, delimiter)
    return [single_response for single_response in columns if single_response not in multi_response_columns]


def dict_vals_to_list(dictionary):
    """Return all of the values from a dictionary to a list."""
    return [val for key in dictionary.values() for val in key]


def split_strings(dataframe, multi_response_columns, delimeter):
    """Split all multi response entries that are separated by the delimeter (e.g., [a;b;c])"""
    df = dataframe.copy()
    df[multi_response_columns] = df[multi_response_columns].apply(lambda col: col.str.split(str(delimeter)))
    return df


def create_lookup_table(dataframe, multi_response_dict):
    # TODO: Refactor the code so that the dataframe is not being built through concatenation; build up a single dictionary.
    """Identifies all of the tools listed from the survey and combines them in a single dataframe

    Args:
        dataframe (pandas object): Dataframe from applying the split_strings() method
        multi_response_dict (dict): Key-value pairs for the multiple response columns

    Returns:
        pandas dataframe: A dataframe containing all of the unique tools associated with the keys from multi_response_cols_dict
    """

    # Empty dataframe that will be populated
    unique_responses = pd.DataFrame()

    # Subset the dataframe for referencing in the loop below
    df = dataframe.copy()

    for key in multi_response_dict:
        container = []
        columns = multi_response_dict[key]

        # Loop through related multiple entries (e.g., LanguageWorkedWith & LanguageDesireNextYear).
        for column in columns:
            for responses in df[column]:
                # Check if the survey response is null, continue if True
                if pd.isnull(responses) is True:
                    continue
                # Add all of the unique tools encountered to the toolbox
                for response in responses:
                    if response not in container:
                        container.append(response)

        container = pd.Series(container, name=key)
        unique_responses = pd.concat([unique_responses, container], axis=1)

    return unique_responses


def create_join_table_for_tools(dataframe, multi_responses, lookup_table, lookup_key, new_respondent_id):
    """Creates the join tables for the many-to-many relationships with name toolInterests

    Args:
        dataframe (pandas object): pandas dataframe
        multi_response_dict (dict): Key-value pairs for the multiple response columns
        lookup_table (pandas object): Reference table of different tools
        lookup_key (str): Lookup key to use in the lookup table for fast access
        new_respondent_id (str): The column name of the new repondent

    Returns:
        pandas dataframe: A dataframe containing all of the unique tools associated with the keys from multi_response_cols_dict
    """
    # TODO: There is a lot going on in this function, consider breaking it up.
    # Dictionary that will be populated below
    table_data = {"RespID": [], f"{lookup_key}ID": [], "Knows": [], "WantsToLearn": []}

    # Pull the series to be referenced from the lookup table
    reference = lookup_table[lookup_key]

    # Subset the dataframe on the desired lookup columns
    sub_columns = deepcopy(multi_responses[lookup_key])
    sub_columns.insert(0, new_respondent_id)
    df = dataframe[sub_columns].copy()

    # Loop through each respondent from the survey
    for resp_id in df[f"{new_respondent_id}"]:
        # Create empty containers to store information for each respondent
        id_container = np.array([])
        lookup_key_container = np.array([])
        knows = np.array([])
        wants_to_learn = np.array([])
        tracker = np.array([])
        indexes = np.array([])
        # Loop through the respondents current tool knowledge and learning desires for next year
        for val in multi_responses[lookup_key]:
            # If the response is empty, move on; the respondent has knowledge or wants to learn a tool -> store the indexes
            if df[val][resp_id] is None or df[val][resp_id] is np.nan:
                continue
            else:
                indexes = reference.isin(df[val][resp_id])
                indexes = indexes[indexes].index
                # If the list of tools is non-empty, begin sorting the indexes into the table_data keys
                if len(indexes) != 0:
                    # Populate the id container
                    id_container = resp_id * np.ones(reference.shape)

                    if lookup_key + "WorkedWith" in val:
                        knows = np.in1d(np.array(reference.index), indexes)
                    elif lookup_key + "DesireNextYear" in val:
                        wants_to_learn = np.in1d(np.array(reference.index), indexes)
                    # Append indexes to the tracker; duplicates will be dealt with momentarily
                    for index in np.array(indexes):
                        tracker = np.append(tracker, index)
                # Create the array to be appended to the f{look_upkey} ID
                lookup_key_container = np.zeros(reference.shape)
                for index in np.unique(tracker).astype(int):
                    lookup_key_container[index] = index

        # Populate the table_data dictionary from above
        table_data["Knows"].append(knows)
        table_data["RespID"].append(id_container)
        table_data["WantsToLearn"].append(wants_to_learn)
        table_data[f"{lookup_key}ID"].append(lookup_key_container)

    # Flatten the arrays in table_data to make uploading to pandas dataframe easier
    for key in table_data:
        table_data[key] = np.array(table_data[key], dtype=object).flatten()
        flattened_values = [val for ragged_list in table_data[key] for val in ragged_list]
        table_data[key] = pd.Series(flattened_values)

    # Finally, add to the dataframe and clean it for upload
    join_table = pd.DataFrame(table_data)
    join_table = join_table.drop(join_table[~join_table.Knows & ~join_table.WantsToLearn].index)
    join_table = join_table.reset_index(drop=True)
    join_table[["RespID", f"{lookup_key}ID"]] = join_table[["RespID", f"{lookup_key}ID"]].astype("int32")
    return join_table


def create_join_table_for_multi(dataframe, lookup_table, lookup_key, new_respondent_id):
    """Creates the join tables for the many-to-many relationships that are not tools

    Args:
        dataframe (pandas object): pandas dataframe
        lookup_table (pandas object): Reference table of different tools
        lookup_key (str): Lookup key to use in the lookup table for fast access
        new_respondent_id (str): The column name of the new repondent

    Returns:
        pandas dataframe: A dataframe containing all of the unique tools associated with the keys from multi_response_cols_dict
    """
    # Dictionary that will be populated below
    table_data = {"RespID": [], f"{lookup_key}ID": [], "Bool": []}
    # Pull the series to be referenced from the lookup table
    reference = lookup_table[lookup_key].dropna()

    # Subset the dataframe on the desired lookup columns
    df = dataframe[[new_respondent_id, lookup_key]].copy()

    for resp_id in df[f"{new_respondent_id}"]:
        # Create empty containers to store information for each respondent
        id_container = np.array([])
        lookup_key_container = np.array([])
        tracker = np.array([])
        indexes = np.array([])
        bool_container = np.array([])

        # If respondent has listed a sexuality, find the indexes
        if df[lookup_key][resp_id] is None or df[lookup_key][resp_id] is np.nan:
            # TODO: This is tedious to maintain, needs to be improved.
            continue
        else:
            indexes = reference.isin(df[lookup_key][resp_id])
            indexes = indexes[indexes].index

            # If the list of tools is non-empty, begin sorting the indexes into the table_data keys
            if len(indexes) != 0:
                # Populate the id container
                id_container = resp_id * np.ones(reference.shape)
                bool_container = np.in1d(np.array(reference.index), indexes)

                # Append indexes to the tracker; duplicates will be dealt with momentarily
                for index in np.array(indexes):
                    tracker = np.append(tracker, index)

            # Create the array to be appended to the f{look_upkey} ID
            lookup_key_container = np.zeros(reference.shape)
            for index in np.unique(tracker).astype(int):
                lookup_key_container[index] = index

        # Populate the table_data dictionary from above
        table_data["RespID"].append(id_container)
        table_data[f"{lookup_key}ID"].append(lookup_key_container)
        table_data["Bool"].append(bool_container)

    # Flatten the arrays in table_data to make uploading to pandas dataframe easier
    for key in table_data:
        table_data[key] = np.array(table_data[key], dtype=object).flatten()
        flattened_values = [val for val in table_data[key]]
        table_data[key] = pd.Series(flattened_values)

    # Insert data into dataframe, remove all False matches and drop the bool.
    data = pd.DataFrame(table_data)
    data = data.drop(data[~data["Bool"]].index).reset_index(drop=True)
    return data.drop("Bool", axis=1)


def create_satellite_join_entity(lookup_table, lookup_key):
    """ Creates satellite joins for non-tool tables """
    entity = pd.DataFrame(lookup_table[lookup_key]).dropna()
    entity.reset_index(inplace=True)
    entity.rename(columns={"index": f"{lookup_key}" + "ID"}, inplace=True)
    return entity


def drop_column_vals_on_conditions(dataframe, column, conditions, drop_column=False, drop_nan=False):
    """ Helper function to drop columns based on a list of conditions """
    df = dataframe.copy()

    if drop_nan:
        df = df.drop(df[pd.isna(df[column])].index)

    # Drop based on the conditions
    for condition in conditions:
        df = df.drop(df[df[column] == condition].index)

    # Drop the column?
    if drop_column:
        df = df.drop(columns=column)

    return df


def add_workid_to_respondent(dataframe, other, id_list):
    """ Adds the WorkID to Respondent Table"""
    resp_id = id_list[0]
    other_id = id_list[1]

    # Create temp df to be merged onto dataframe
    temp_df = other[[other_id, resp_id]].set_index(resp_id)
    # Reset indicies to make concatenation easier
    dataframe.set_index(resp_id, inplace=True)
    dataframe = pd.concat([dataframe, temp_df], axis=1)
    dataframe = dataframe.reset_index()
    return dataframe


def add_countryid_to_respondent(dataframe, other, id_list):
    """ Adds the CountryID to respondent table """
    rows, _ = dataframe.shape
    df = dataframe.copy()

    for i in range(5):
        respondents_country = df.loc[i, "Country"]
        tmp_country_row = other.loc[other["Country"] == respondents_country]
        tmp_country_id = tmp_country_row.iloc[0][0]
        df.loc[i, "Country"] = tmp_country_id

    df.rename(columns={"Country": "CountryID"}, inplace=True)
    return df


def add_overflowid_to_respondent(dataframe, other, id_list):
    pass

# def create_SOFeedback(dataframe, columns):
#     df = dataframe[columns].copy().reset_index()
#     df.rename(columns={"index": "SOFeedbackID"}, inplace=True)
#     return df
