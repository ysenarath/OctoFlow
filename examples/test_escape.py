from octoflow.utils.escape import escape, unescape


def main():
    text = "_some_function"
    escaped = "_" + escape(text)
    print(f"Escaped: {escaped}")
    unescaped = unescape(escaped)
    print(f"Unescaped: {unescaped}")


if __name__ == "__main__":
    main()
