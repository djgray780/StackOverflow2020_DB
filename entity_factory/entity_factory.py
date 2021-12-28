import pandas as pd
from sqlalchemy import create_engine

root = r"C:\Users\den\code\stackoverflow2020\data\json_to_postgress"
entities_str = [
    "Respondent"
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
    "WorkEnvironment"
]

# Establish a connection to postgres 
engine = create_engine("postgresql://postgres:postgres@localhost:5432/so2020")
conn = engine.connect()

# Export all json files to sql tables in postgres.
for entity in entities_str:
    import_path = root + f"\{entity}.json"
    df = pd.read_json(import_path)
    df.to_sql(name=f"{entity.lower()}", con=engine, index=False, if_exists="replace")
