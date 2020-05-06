import pandas as pd
from fginvestigation.extraction import export_sqlserver, get_data_sql


def get_offset() -> int:
    query = (
        "SELECT MAX(TAG_ID) as MAX"
        " from TAGS"
    )
    df = get_data_sql(query)

    return int(df.MAX.values[0])

print(get_offset())

EVENTS = pd.read_csv("EVENTS.csv", dtype={"TAG_ID":int})
export_sqlserver(EVENTS[["EVENT_TIME","EVENT_VALUE", "TAG_ID"]], "EVENTS", if_exists='append')
TAGS = pd.read_csv("TAGS.csv", dtype={"TAG_ID":int, "OBJECT_NO":int})
export_sqlserver(TAGS, "TAGS", if_exists='append')
OBJECTS = pd.read_csv("OBJECTS.csv", dtype={"OBJECT_NO":int})
export_sqlserver(OBJECTS, "OBJECTS", if_exists='fail')