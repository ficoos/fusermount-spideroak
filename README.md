# fusermount-spideroak

FUSE implementation to allow accessing a SpiderOak public share without having to synchornize is locally.

# Usage
```
Usage of fusermount-spideroak: -share-id <SHAREID> -room-key <ROOMKEY> [option]... <MOUNTPOINT>
  -cache-dir string
    	cache directory (default ".cache")
  -cache-size value
    	size of the cache (default 1G)
  -h	display this help and exit
  -help
    	display this help and exit
  -room-key string
    	share's room key
  -share-id string
    	name of the share
```

# FAQ

* **Q:** My cache is is effectively bigger than the requested cache size
  **A:** Cache size is a recommendation more than a hard limit.
  Files are not actually cleared until they are closed.
  As a rule of thumb the actual cache size should not exceed cache-size + biggest file accessed + all open files.
* **Q:** I update things on the share but I don't see an update until I remount
  **A:** The current cache implementation assumes the share never changes. We do intend to fix that.
