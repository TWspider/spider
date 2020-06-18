"""排序正则规则
"""


def sort_regular():
    with open('config/regular.txt', 'r') as fp:
        regular = fp.read().split('\n')
    
    regular = sorted(regular)

    with open('config/regular.txt', 'w') as fp:
        fp.write('\n'.join(regular))


if __name__ == "__main__":
    sort_regular()
