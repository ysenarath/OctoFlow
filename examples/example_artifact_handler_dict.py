from octoflow.tracking.artifact import handler

handler_types = handler.list_handler_types()

print("Available handler types:" + " None" if not handler_types else "")
for handler_type in handler_types:
    print(f"\t{handler_type}")
print()

handler_class = handler.get_handler_type("json")

f = handler_class(path="logs/logged-artifact-dict-json")

data = {
    "a": [1, 2, 3],
    "b": [4, 5, 6],
}

if not f.exists():
    print("File does not exist")
    print("Creating file...", end="\t")
    f.save(data)
    print("Done")
else:
    print("File exists")

try:
    print("Loading file...", end="\t")
    df = f.load()
    print("Done")
except FileNotFoundError:
    print("File does not exist")

print("Unlinking file...", end="\t")
f.unlink()
print("Done")
