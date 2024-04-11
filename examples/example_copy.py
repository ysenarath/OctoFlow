from octoflow.utils.rsync import RSyncError, rsync

if __name__ == "__main__":
    try:
        for stdout in rsync(
            "./examples/data.jsonl", "./examples/data_copy.jsonl"
        ):
            print(stdout)
    except RSyncError as e:
        print(e)
