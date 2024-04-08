from octoflow.data.document import Document, DocumentBatch

# Create a document
doc = Document(
    {
        "name": "John",
        "age": 30,
        "city": "New York",
    },
    id=1,
)


# Access the document - just like a dictionary
print(doc["name"])  # John

# Access the document id
print(doc.id)  # 1

# Access the document schema
print(doc.schema)
"""
name: string
age: uint8
city: string
"""

docs = DocumentBatch([doc])
print(docs[0]["name"])  # John

# to batch
print(docs.to_batch())
