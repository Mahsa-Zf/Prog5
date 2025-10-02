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
We are going to use python profiler to see how much time each part of the code takes
"""
import sys
import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

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

def main():
    """Main function to execute the parallel sieve of Eratosthenes."""
    if len(sys.argv) != 2:
        print("Usage: python assignment3.py n")
        sys.exit(1)

    n = int(sys.argv[1])
    sqrt_n = int(np.sqrt(n))

    # Divide the range after sqrt_n among processes
    chunk_size = (n - sqrt_n) // size if size > 1 else n - sqrt_n
    start = sqrt_n + 1 + rank * chunk_size
    if rank == size - 1:
        end = n + 1
    else:
        end = start + chunk_size
    local_arr = np.ones(end - start, dtype=bool)
    numbers = np.arange(start, end)

    # Broadcast small primes from root to all ranks synchronously
    if rank == 0:
        small_primes = sieve(sqrt_n)
    else:
        small_primes = None

    # bcast is a blocking collective operation, The call will not return and
    # the code will not proceed beyond this point until all ranks
    # in the communicator, including rank 0 itself, have entered
    # the broadcast call and completed it.
    small_primes = comm.bcast(small_primes, root=0)

    # Mark non-primes in each chunk
    for prime in small_primes:

        # the first index in local_arr that is a multiple of prime
        first = (prime - (start % prime)) % prime
        # we are deleting multiples of small prime numbers, 
        # starting from the first multiple in the range
        non_prime_indices = np.arange(first, end - start, prime)
        local_arr[non_prime_indices] = False

    local_primes = numbers[local_arr]
    print(f"Rank {rank} found primes in range [{start}, {end}):", local_primes)

    if size == 1:
        # If only one process, just print the result
        all_primes = local_primes
        print(f"All primes up to {n}:", all_primes)
    else:
        # Asynchronously send local primes back to root
        if rank != 0:
            req = comm.isend(local_primes, dest=0)
            # .wait is necessary to ensure the message sending is complete
            req.wait()
        else:
            # We took care of non-root ranks, now
            # We should include small primes and calculations from root
            results = [small_primes, local_primes]
            # irecv must be matched with correct senders and receivers (it's always in rank == 0)
            recv_reqs = [comm.irecv(source=i) for i in range(1, size)]

            # while the loop is running, the root is continuously
            # checking and progressively receiving data as it arrives.
            while recv_reqs:
                for req in recv_reqs:
                    flag, data = req.test()
                    if flag:
                        results.append(data)
                        recv_reqs.remove(req)

            all_primes = np.concatenate(results)
            print(f"All primes up to {n}:", all_primes)


if __name__ == "__main__":
    main()
