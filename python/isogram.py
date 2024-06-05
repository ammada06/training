

def is_isogram(word):
    seen_letters = set()

    for l in word:
        if l in seen_letters:
            return False
        seen_letters.add(l)

    return True
