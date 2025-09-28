"""
algorithm Sieve of Eratosthenes is
    input: an integer n > 1.
    output: all prime numbers from 2 through n.

    let A be an array of Boolean values, indexed by integers 2 to n,
    initially all set to true.
    
    for i = 2, 3, 4, ..., not exceeding √n do
        if A[i] is true
            for j = i2, i2+i, i2+2i, i2+3i, ..., not exceeding n do
                set A[j] := false

    return all i such that A[i] is true.

"""
import numpy as np

def sieve(n):
    """
    input: an integer n > 1.
    output: all prime numbers from 2 through n.
    """
    arr = np.ones(n-1, dtype=bool)

    for i in np.arange(2,round(np.sqrt(n))+1):
        indx = i - 2
        if arr[indx]:
            a = 0
            while i**2 + a*i <= n:
                non_prime_indx = i**2 + a*i - 2
                arr[non_prime_indx] = False
                a += 1
    return [i + 2 for i in range(len(arr)) if arr[i]]

