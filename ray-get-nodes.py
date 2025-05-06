#!/usr/bin/env python3

import json

import ray

ray.init(address="ray://localhost:10001")
print(json.dumps(ray.nodes(), indent=2))
ray.shutdown()
