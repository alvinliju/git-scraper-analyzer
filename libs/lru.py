from collections import OrderedDict
from typing import Any

class LRUCache:
    def __init__(self, capacity:int):
        #initlize cahce with predetermined capacity
        self.cache = OrderedDict()
        self.capacity = capacity
    
    def get(self, key:str) -> Any:
        #retrive items from provided key. Return -1 if not exist
        #find the key
        if key not in self.cache:
            return -1
        #move the key to the end of the cache
        self.cache.move_to_end(key)
        #return the value
        return self.cache[key]

    def put(self, key:str, value:Any):
        #add items to cache. If cache is at capacity, remove least recently used item
        self.cache[key] = value
        #if cahce is at capacity remove the cache that was stroed in the begning
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
        return
    
    def print(self):
        # Print the cache content in order from most recently used to least recently used
        for key, value in self.cache.items():
            print(f"{key}: {value}")  
