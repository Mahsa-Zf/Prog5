"""Assignment 2: Numerical Integration (Trapezoidal Rule) Using SLURM"""
__author__ = "Mahsa Zamanifard"
__date__ = "2025-09-18"

import sys
import math
from sympy import symbols, integrate, cos
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
# print("Rank: ", rank, " Size: ", size)

def trapezoid(func, a, b, n=256):
    """Numerically integrate func from a to b with n trapezoids."""
    step_size = (b - a) / n
    sums = func(a) + func(b)
    for i in range(1, n):
        sums += 2 * func(a + i * step_size)
    return sums * step_size / 2

def main():
    """Main function to execute the trapezoidal integration and compute error."""
    if len(sys.argv) != 4:
        print("Usage: python trapezoid_integral.py a b n")
        sys.exit(1)

    a = float(sys.argv[1])
    b = float(sys.argv[2])
    n = int(sys.argv[3])

    # Define the function to integrate
    fcos = math.cos
    quotient = n // size
    remainder = n % size
    step_size = (b - a) / n

    # Assign workload including remainder to rank 0
    # Performing numerical integral using trapezoid method
    # First we scatter the workload
    if rank == 0:
        local_n = quotient + remainder
        local_a = a
    else:
        local_n = quotient
        local_a = a + remainder * step_size + rank * quotient * step_size

    local_b = local_a + local_n * step_size

    local_result = trapezoid(fcos, local_a, local_b, local_n)

    # Now we gather the results
    results = comm.gather(local_result, root=0)

    if rank == 0:
        numerical = sum(results)
         # Symbolic exact integral of cos(x) from a to b
        var = symbols('x')
        exact = integrate(cos(var), (var, a, b)).evalf()

        # Error (difference)
        error = abs(exact - numerical)

        # Output n and error separated by a comma
        print(f"n: {n}, error: {error:f}")



if __name__ == "__main__":
    main()
