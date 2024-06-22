from octoflow.utils.sqlitedict import SQLiteDict

db = SQLiteDict("examples/example.db")

db.clear()

print(db)

db["foo_str"] = "bar"
db["foo_int"] = 42
db["foo_float"] = 3.14
db["foo_list"] = [1, 2, 3]
db["foo_dict"] = {"a": 1, "b": 2}

for key in db:
    print(key, db[key])


print(db)
