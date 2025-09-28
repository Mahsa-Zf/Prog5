## 1.1 Can you see where the most calculation time is spent? What line number/function is it?
it's my sieve function, followed by the list comprehension I created when it returnes the final results.

## 1.2 What is the distribution of execution times in your file, is it homegeneous (uniform) or is there a power distribution? (Make a graph if it helps).
The execution time distribution is heavily skewed to this single function, with very little time spent elsewhere.
This aligns with a power distribution where most run time is concentrated in one location; it's not uniform or homogeneous.