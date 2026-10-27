#include "primality_test.h"
#include <gmp.h>

// GMP-based primality test using Miller-Rabin algorithm
// Returns true if the number is prime, false otherwise
bool is_prime(uint64_t n){
    if(n <= 1) return false;
    if(n == 2) return true;
    if(n % 2 == 0) return false;
    
    mpz_t num;
    mpz_init(num);
    mpz_set_ui(num, n);
    
    // 25 is the number of Miller-Rabin tests, open to configuration
    int result = mpz_probab_prime_p(num, 25);
    
    mpz_clear(num);
    
    return result > 0;
}

