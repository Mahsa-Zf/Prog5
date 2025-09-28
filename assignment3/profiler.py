"""Profiler for the sieve function in assignment3 module."""
import cProfile
import pstats
from assignment3 import sieve

profiler = cProfile.Profile()
profiler.enable()

primes = sieve(10000)

profiler.disable()

with open('sieve_profile_output.txt', 'w', encoding='utf-8') as f:
    ps = pstats.Stats(profiler, stream=f).sort_stats('cumulative')
    ps.print_stats(10)  # write top 10 to file
