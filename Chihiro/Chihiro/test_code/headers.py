for i in range(1, 10):
    print('i', i)
    a = (x for x in range(1, 10))
    for j in a:
        print('j', j)
# a.next()

s1 = set([1, 2, 3])
print(s1)
