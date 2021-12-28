import helper_functions as h
import pandas as pd

if __name__ == "__main__":
    # Read in the necessary paths
    PATH_TO_YAML = r"C:\Users\den\code\stackoverflow2020\data\inputs.yaml"
    path = h.read_yaml(PATH_TO_YAML)["Path_R2P_Data"]
    path_to_lookup_table = h.read_yaml(PATH_TO_YAML)["Path_Lookup_Table"]

    # Read in the core serialized objects
    df = pd.read_json(path)

    # Will need to drop Employment, Country.
    Respondent = df[
        [
            "MainBranch",
            "Hobbyist",
            "EdLevel",
            "Employment",
            "Trans",
            "Age",
            "UndergradMajor",
            "Age1stCode",
            "YearsCode",
            "YearsCodePro",
            "Country",
        ]
    ]
    Respondent = Respondent.reset_index()
    Respondent.rename(columns={"index": "RespID"}, inplace=True)

    ### Create country table
    Country = df["Country"].drop_duplicates().dropna().sort_values().reset_index(drop=True)
    Country = Country.reset_index()
    Country.rename(columns={"index": "CountryID"}, inplace=True)

    ### Create WorkEnvironment Table
    WorkEnvironment = df[
        [
            "RespondentNew",
            "CompFreq",
            "ConvertedComp",
            "JobSat",
            "JobSeek",
            "NEWOvertime",
            "WorkWeekHrs",
            "OpSys",
            "NEWDevOps",
            "NEWOnboardGood",
            "OrgSize",
            "PurchaseWhat",
            "Employment",  # 'Employment' gets deleted right away with drop_column_vals_on_conditions()
        ]
    ].reset_index(drop=True)

    WorkEnvironment = h.drop_column_vals_on_conditions(
        WorkEnvironment,
        "Employment",
        ["Not employed, and not looking for work", "Not employed, but looking for work", "Retired", "Student"],
        drop_column=True,
        drop_nan=True,
    )
    WorkEnvironment = WorkEnvironment.reset_index(drop=True)
    WorkEnvironment = WorkEnvironment.reset_index()
    WorkEnvironment.rename(columns={"RespondentNew": "RespID", "index": "WorkID"}, inplace=True)


    # Add the WorkID to Respondent Table
    Respondent = h.add_workid_to_respondent(Respondent, WorkEnvironment, ["RespID", "WorkID"])
    Respondent = h.add_countryid_to_respondent(Respondent, Country, ["RespID", "CountryID"])

    print(Respondent.columns)

# TODO: Add the SOFeedback Table
    # SOFeedback = h.create_SOFeedback(
    #     df,
    #     [
    #         "SOAccount",
    #         "SOComm",
    #         "SOPartFreq",
    #         "SOVisitFreq",
    #         "WelcomeChange",
    #         "NEWOffTopic",
    #         "SurveyEase",
    #         "SurveyLength",
    #     ],
    # )

    ### Export all objects as json for import into entity_factory.py
    root = r"C:\Users\den\code\stackoverflow2020\data\json_to_postgress"
    entities_str = ['Respondent', 'Country', 'WorkEnvironment']
    entities = [Respondent, Country, WorkEnvironment]
    for entity, name in zip(entities, entities_str):
        print(name)
        path = root + f"\{name}.json"
        entity.to_json(path)
