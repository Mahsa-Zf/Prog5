"""Profiler for the sieve function in assignment3 module."""
import cProfile
import pstats
import io
from assignment3 import sieve

profiler = cProfile.Profile()
profiler.enable()

# Call the function to profile
primes = sieve(10000)

profiler.disable()

# Save the profiling results to a text file
with open('sieve_profile_output.txt', 'w', encoding='utf-8') as f:
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats(10)  # Print top 10 lines
    f.write(s.getvalue())
