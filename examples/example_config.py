from octoflow import Config

config = Config()


@config.wraps
def sample_fn_1(a, b, c, d):
    print(a, b, c, d)


@config.wraps
def sample_fn_2(d: int = 5):
    for i in range(d):
        sample_fn_1(c=i, d=d)


if __name__ == "__main__":
    config["sample_fn_1"] = {
        "a": 1,
        "b": 2,
    }
    config.update({
        "sample_fn_2": {
            "d": 10,
        },
    })
    sample_fn_2()
