#include "pch.h"
#include <immintrin.h>

int main() {

#if defined(__AVX512F__)
    std::cout << "AVX-512 Foundation instructions are enabled" << std::endl;
#else
    std::cout << "AVX-512F not enabled. Check compiler flags." << std::endl;
#endif
    std::cin.get();
    return 0;
}