def remove_spaces(text):
    if len(text.strip()) == 0:
        return text

    n = 0
    while text[n] == "\n":
        n += 1

    left_n = n

    n = 0
    while text[-(n + 1)] == "\n":
        n += 1

    right_n = n
    return "\n" * left_n + text.strip() + "\n" * right_n
