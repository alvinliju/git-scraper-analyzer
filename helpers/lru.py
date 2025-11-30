from collections import OrderedDict


class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key: int) -> int:
        if key not in self.cache:
            return -1
        # Move the accessed key to the end to mark it as most recently used
        value = self.cache.pop(key)
        self.cache[key] = value
        return value

    def put(self, key: int, value: int) -> None:
        if key in self.cache:
            # If key exists, update its value and move to the end
            self.cache.pop(key)
        elif len(self.cache) >= self.capacity:
            # If cache is full, remove the least recently used item (first item)
            self.cache.popitem(last=False)
        self.cache[key] = value
