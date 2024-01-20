from octoflow.tracking.artifact import handler

handler_types = handler.list_handler_types()

print("Available handler types:" + " None" if not handler_types else "")
for handler_type in handler_types:
    print(f"\t{handler_type}")
print()
