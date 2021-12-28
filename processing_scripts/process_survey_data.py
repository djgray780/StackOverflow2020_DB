import helper_functions as h
import pandas as pd

if __name__ == "__main__":
    # Read in the necessary paths
    PATH_TO_YAML = r"C:\Users\den\code\stackoverflow2020\data\inputs.yaml"
    path = h.read_yaml(PATH_TO_YAML)["Path_R2P_Data"]
    path_to_lookup = h.read_yaml(PATH_TO_YAML)["Path_Lookup_Table"]

    # Separate the multiple and single response columns
    single_responses = h.read_yaml(PATH_TO_YAML)["SingleResponse"]  # dict
    multi_responses = h.read_yaml(PATH_TO_YAML)["MultiResponse"]  # dict
    multi_responses_list = h.dict_vals_to_list(multi_responses)

    # Load json object into dataframe
    path_to_lookup = h.read_yaml(PATH_TO_YAML)["Path_Lookup_Table"]

    df = pd.read_json(path)
    multi_response_lookup_table = pd.read_json(path_to_lookup)
    # Split the strings of the dataframe for all of the multiple response columns

    # Create the lookup table for all of the multiple response fields
    # multi_response_lookup_table = h.create_lookup_table(df[multi_responses_list], multi_responses)
    print(multi_response_lookup_table)

    # # Create join tables from DB design
    # CollabToolsInterests = h.create_join_table_for_tools(
    #     df, multi_responses, multi_response_lookup_table, "CollabTools", "RespondentNew"
    # )
    # PlatformInterests = h.create_join_table_for_tools(
    #     df, multi_responses, multi_response_lookup_table, "Platform", "RespondentNew"
    # )
    # MiscTechInterests = h.create_join_table_for_tools(
    #     df, multi_responses, multi_response_lookup_table, "MiscTech", "RespondentNew"
    # )
    # DatabaseInterests = h.create_join_table_for_tools(
    #     df, multi_responses, multi_response_lookup_table, "Database", "RespondentNew"
    # )
    # WebFrameworkInterests = h.create_join_table_for_tools(
    #     df, multi_responses, multi_response_lookup_table, "WebFramework", "RespondentNew"
    # )
    # LanguageInterests = h.create_join_table_for_tools(
    #     df, multi_responses, multi_response_lookup_table, "Language", "RespondentNew"
    # )

    # # Create individual tool tables
    # CollabTools = h.create_satellite_join_entity(multi_response_lookup_table, "CollabTools")
    # Platform = h.create_satellite_join_entity(multi_response_lookup_table, "Platform")
    # MiscTech = h.create_satellite_join_entity(multi_response_lookup_table, "MiscTech")
    # Database = h.create_satellite_join_entity(multi_response_lookup_table, "Database")
    # Webframework = h.create_satellite_join_entity(multi_response_lookup_table, "Webframework")
    # Language = h.create_satellite_join_entity(multi_response_lookup_table, "Language")

    # # Create Gender, Ethnicity, and Sexuality individual tables.
    # Gender = h.create_satellite_join_entity(multi_response_lookup_table, "Gender")
    # Ethnicity = h.create_satellite_join_entity(multi_response_lookup_table, "Ethnicity")
    # Sexuality = h.create_satellite_join_entity(multi_response_lookup_table, 'Sexuality')

    # # Create join tables for 'Section 2'
    # GenderIdentifications = h.create_join_table_for_multi(df, multi_responses, multi_response_lookup_table, "Gender", "RespondentNew")
    # EthnicBackgrounds = h.create_join_table_for_multi(df, multi_responses, multi_response_lookup_table, "Ethnicity", "RespondentNew")
    # SexualOrientations = h.create_join_table_for_multi(df, multi_responses, multi_response_lookup_table, "Sexuality", "RespondentNew")

    # Create single responses for country table
    # Country = df["Country"].drop_duplicates().dropna().sort_values().reset_index(drop=True)
    # Country = Country.reset_index()
    # Country.rename(columns={"index": "CountryID"}, inplace=True)

    # Create WorkEnvironment Table

    # WorkEnvironment = df[
    #     [
    #         "RespondentNew",
    #         "CompFreq",
    #         "ConvertedComp",
    #         "JobSat",
    #         "JobSeek",
    #         "NEWOvertime",
    #         "WorkWeekHrs",
    #         "OpSys",
    #         "NEWDevOps",
    #         "NEWOnboardGood",
    #         "OrgSize",
    #         "PurchaseWhat",
    #         "Employment",  # This gets deleted right away
    #     ]
    # ].reset_index(drop=True)

    # WorkEnvironment = h.drop_column_vals_on_conditions(
    #     WorkEnvironment,
    #     "Employment",
    #     ["Not employed, and not looking for work", "Not employed, but looking for work", "Retired", "Student"],
    #     drop_column=True,
    #     drop_nan=True,
    # )

    # Create DevType
    # DevType = h.create_satellite_join_entity(multi_response_lookup_table, "DevType")
    # Create JobDescription
    # JobDescription = h.create_join_table_for_multi(df, multi_response_lookup_table, "DevType", "RespondentNew")

    # Create JobHuntQuality Table
    # JobHuntQuality = h.create_satellite_join_entity(multi_response_lookup_table, 'NEWJobHunt')
    # NEWJobHunt = h.create_join_table_for_multi(df, multi_response_lookup_table, "NEWJobHunt", "RespondentNew")

    # Create ResearchResource
    # ResearchResource = h.create_satellite_join_entity(multi_response_lookup_table, 'NEWJobHuntResearch')
    # NEWJobHuntResearch = h.create_join_table_for_multi(
    #     df, multi_response_lookup_table, "NEWJobHuntResearch", "RespondentNew"
    # )

    # # Create SatisfactionFactor
    # SatisfactionFactor = h.create_satellite_join_entity(multi_response_lookup_table, 'JobFactors')
    # JobFactors = h.create_join_table_for_multi(df, multi_response_lookup_table, "JobFactors", "RespondentNew")

    # # Create SOSites
    # SOSites = h.create_satellite_join_entity(multi_response_lookup_table, 'NEWSOSites')
    # NEWSOSitesVisited = h.create_join_table_for_multi(df, multi_response_lookup_table, "NEWSOSites", "RespondentNew")

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
