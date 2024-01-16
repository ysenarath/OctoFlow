import pandas as pd

from octoflow.core.artifact import handler

handler_types = handler.list_handler_types()

print("Available handler types:")
for handler_type in handler_types:
    print(f"\t{handler_type}")
print()

temp_df = pd.DataFrame({
    "a": [1, 2, 3],
    "b": [4, 5, 6],
})

handler_class = handler.get_handler_type_by_object(temp_df)

f = handler_class(path="logs/logged-artifact-pandas-dataframe")

if not f.exists():
    print("File does not exist")
    print("Creating file...", end="\t")
    f.save(temp_df)
    print("Done")
else:
    print("File exists")

try:
    print("Loading file...", end="\t")
    df = f.load()
    print("Done")
except FileNotFoundError:
    print("Failed")

print("Unlinking file...", end="\t")
f.unlink()
print("Done")
