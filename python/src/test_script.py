words = ["test", "cat", "dog", "rabbit"]

for word in words:
    print(word, len(word))

reversed_words = list()
for word in words[::-1]:
    print(word)
    reversed_words.append(word)

print(reversed_words)
