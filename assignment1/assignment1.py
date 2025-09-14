"""Assignment 1: Numerical Integration (Trapezoidal Rule) Using SLURM"""
__author__ = "Mahsa Zamanifard"
__date__ = "2025-09-14"

import sys
import math
from sympy import symbols, integrate, cos

def trapezoid(func, a, b, n=256):
    """Numerically integrate func from a to b with n trapezoids."""
    h = (b - a) / n
    s = func(a) + func(b)
    for i in range(1, n):
        s += 2 * func(a + i * h)
    return s * h / 2

def main():
    """Main function to execute the trapezoidal integration and compute error."""
    if len(sys.argv) != 4:
        print("Usage: python trapezoid_integral.py a b n")
        sys.exit(1)

    a = float(sys.argv[1])
    b = float(sys.argv[2])
    n = int(sys.argv[3])

    # Define the function to integrate
    f = math.cos

    # Numerical integral using trapezoid method
    numerical = trapezoid(f, a, b, n)

    # Symbolic exact integral of cos(x) from a to b
    x = symbols('x')
    exact = integrate(cos(x), (x, a, b)).evalf()

    # Error (difference)
    error = abs(exact - numerical)

    # Output n and error separated by a comma
    print(f"{n},{error:f}")

if __name__ == "__main__":
    main()
