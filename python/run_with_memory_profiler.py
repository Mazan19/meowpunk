"""
Run main with memory profiler
Usage: python -m memory_profiler run_with_memory_profiler.py
"""

import main
from memory_profiler import profile


@profile
def run_with_profiler():
    main.main()


if __name__ == "__main__":
    run_with_profiler()
