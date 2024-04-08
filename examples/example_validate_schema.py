import cProfile

import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int32()),
    pa.field("name", pa.string()),
])
doc = {
    "id": 1,
    "name": "Alice",
}
invalid_doc = {
    "id": "2",
}


def method_1():
    try:
        pa.RecordBatch.from_pylist([doc], schema)
    except pa.lib.ArrowInvalid as e:
        raise ValidationError(str(e).lower()) from e


def method_2():
    try:
        pa.RecordBatch.from_pylist([invalid_doc], schema)
    except pa.lib.ArrowInvalid as e:
        raise ValidationError(str(e).lower()) from e


def main():
    print("------------------")
    cProfile.run("print(method_1())")
    print("------------------")
    cProfile.run("print(method_1())")
    print("------------------")
    cProfile.run("print(method_2())")


if __name__ == "__main__":
    main()
