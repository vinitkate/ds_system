#ifndef PRIMALITY_TEST_H
#define PRIMALITY_TEST_H

#include <cstdint>

// GMP-based Miller-Rabin primality test
// Highly optimized, industry-standard implementation
bool is_prime(uint64_t n);

#endif
