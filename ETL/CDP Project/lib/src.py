import pandas as pd
from lib.utils import connect_pymongo


def get_label_by_Clabel(cdb_id):
    client, db=connect_pymongo(cdb_id)
    cursor = db["label"].find({}, {"label_id", "name"})
    out = pd.DataFrame(list(cursor)).drop(["_id"], axis=1)
    out.columns = ['labelId', 'label']
    client.close()
    return out    


