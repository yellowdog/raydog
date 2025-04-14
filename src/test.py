#!/usr/bin/env python3

from raydogclient import *
from raydoghead import *

def main():
    raydog = RayDogClient()
    try:
        raydog.start_head_node()

        headnode = RayDogHead()
        headnode.add_workers(2)

        input("Hit enter to shut down cluster ")

    finally:
        raydog.shutdown()
        
# call the main function
if __name__ == "__main__":
    main()