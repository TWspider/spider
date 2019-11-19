class ItemHandle(object):
    def __init__(self):
        pass

    def get_item_xpath_item(self, response, str_xpath):
        item_xpath = response.xpath("{}".format(str_xpath)).extract_first()
        return item_xpath

    def get_item_json(self):
        pass

    def get_item_no_space(self):
        pass


if __name__ == '__main__':
    pass
