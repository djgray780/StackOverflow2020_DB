import helper_functions as h
import pandas as pd

if __name__ == "__main__":
    # Read in the necessary paths
    PATH_TO_YAML = r"C:\Users\den\code\stackoverflow2020\data\inputs.yaml"
    path = h.read_yaml(PATH_TO_YAML)["Path_R2P_Data"]
    path_to_lookup_table = h.read_yaml(PATH_TO_YAML)["Path_Lookup_Table"]

    # Separate out the multiple response columns
    multi_responses = h.read_yaml(PATH_TO_YAML)["MultiResponse"]  # dict
    multi_responses_list = h.dict_vals_to_list(multi_responses)

    # Read in the core serialized objects
    R2P_df = pd.read_json(path)
    multi_response_lookup_table = pd.read_json(path_to_lookup_table)
    # print(multi_response_lookup_table)

    # Create join tables from DB design
    CollabToolsInterests = h.create_join_table_for_tools(
        R2P_df,
        multi_responses,
        multi_response_lookup_table,
        "CollabTools",
        "RespondentNew",
    )
    PlatformInterests = h.create_join_table_for_tools(
        R2P_df,
        multi_responses,
        multi_response_lookup_table,
        "Platform",
        "RespondentNew",
    )
    MiscTechInterests = h.create_join_table_for_tools(
        R2P_df,
        multi_responses,
        multi_response_lookup_table,
        "MiscTech",
        "RespondentNew",
    )
    DatabaseInterests = h.create_join_table_for_tools(
        R2P_df,
        multi_responses,
        multi_response_lookup_table,
        "Database",
        "RespondentNew",
    )
    WebFrameworkInterests = h.create_join_table_for_tools(
        R2P_df,
        multi_responses,
        multi_response_lookup_table,
        "Webframe",
        "RespondentNew",
    )
    LanguageInterests = h.create_join_table_for_tools(
        R2P_df,
        multi_responses,
        multi_response_lookup_table,
        "Language",
        "RespondentNew",
    )

    # Create satellite tool tables
    CollabTools = h.create_satellite_join_entity(multi_response_lookup_table, "CollabTools")
    Platform = h.create_satellite_join_entity(multi_response_lookup_table, "Platform")
    MiscTech = h.create_satellite_join_entity(multi_response_lookup_table, "MiscTech")
    Database = h.create_satellite_join_entity(multi_response_lookup_table, "Database")
    Webframework = h.create_satellite_join_entity(multi_response_lookup_table, "Webframe")
    Language = h.create_satellite_join_entity(multi_response_lookup_table, "Language")

    # Create join tables for 'Section 2'
    GenderIdentifications = h.create_join_table_for_multi(R2P_df, multi_response_lookup_table, "Gender", "RespondentNew")
    EthnicBackgrounds = h.create_join_table_for_multi(R2P_df, multi_response_lookup_table, "Ethnicity", "RespondentNew")
    SexualOrientations = h.create_join_table_for_multi(R2P_df, multi_response_lookup_table, "Sexuality", "RespondentNew")

    # Create Gender, Ethnicity, and Sexuality satellite tables.
    Gender = h.create_satellite_join_entity(multi_response_lookup_table, "Gender")
    Ethnicity = h.create_satellite_join_entity(multi_response_lookup_table, "Ethnicity")
    Sexuality = h.create_satellite_join_entity(multi_response_lookup_table, "Sexuality")

    # Create DevType
    DevIdentifications = h.create_join_table_for_multi(R2P_df, multi_response_lookup_table, "DevType", "RespondentNew")
    DevType = h.create_satellite_join_entity(multi_response_lookup_table, "DevType")

    # Create JobHuntQuality Table
    NEWJobHunt = h.create_join_table_for_multi(R2P_df, multi_response_lookup_table, "NEWJobHunt", "RespondentNew")
    JobHuntQuality = h.create_satellite_join_entity(multi_response_lookup_table, "NEWJobHunt")

    # Create ResearchResource
    NEWJobHuntResearch = h.create_join_table_for_multi(R2P_df, multi_response_lookup_table, "NEWJobHuntResearch", "RespondentNew")
    ResearchResource = h.create_satellite_join_entity(multi_response_lookup_table, "NEWJobHuntResearch")

    # Create SatisfactionFactor
    JobFactors = h.create_join_table_for_multi(R2P_df, multi_response_lookup_table, "JobFactors", "RespondentNew")
    SatisfactionFactor = h.create_satellite_join_entity(multi_response_lookup_table, "JobFactors")

    # Create SOSites
    NEWSOSitesVisited = h.create_join_table_for_multi(R2P_df, multi_response_lookup_table, "NEWSOSites", "RespondentNew")
    SOSites = h.create_satellite_join_entity(multi_response_lookup_table, "NEWSOSites")

    ### Export all objects as json for import into entity_factory.py
    root = r"C:\Users\den\code\stackoverflow2020\data\json_to_postgress"
    entities = [
        CollabToolsInterests,
        PlatformInterests,
        MiscTechInterests,
        DatabaseInterests,
        WebFrameworkInterests,
        LanguageInterests,
        CollabTools,
        Platform,
        MiscTech,
        Database,
        Webframework,
        Language,
        GenderIdentifications,
        EthnicBackgrounds,
        SexualOrientations,
        GenderIdentifications,
        EthnicBackgrounds,
        SexualOrientations,
        Gender,
        Ethnicity,
        Sexuality,
        DevIdentifications,
        DevType,
        NEWJobHunt,
        JobHuntQuality,
        NEWJobHuntResearch,
        ResearchResource,
        JobFactors,
        SatisfactionFactor,
        NEWSOSitesVisited,
        SOSites,
    ]

    entities_str = [
        "CollabToolsInterests",
        "PlatformInterests",
        "MiscTechInterests",
        "DatabaseInterests",
        "WebFrameworkInterests",
        "LanguageInterests",
        "CollabTools",
        "Platform",
        "MiscTech",
        "Database",
        "Webframework",
        "Language",
        "GenderIdentifications",
        "EthnicBackgrounds",
        "SexualOrientations",
        "GenderIdentifications",
        "EthnicBackgrounds",
        "SexualOrientations",
        "Gender",
        "Ethnicity",
        "Sexuality",
        "DevIdentifications",
        "DevType",
        "NEWJobHunt",
        "JobHuntQuality",
        "NEWJobHuntResearch",
        "ResearchResource",
        "JobFactors",
        "SatisfactionFactor",
        "NEWSOSitesVisited",
        "SOSites",
    ]

    for entity, name in zip(entities, entities_str):
        path = root + f"\{name}.json"
        entity.to_json(path)
