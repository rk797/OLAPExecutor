#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include <random>

#include <immintrin.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#ifdef min
#undef min
#endif

#ifdef max
#undef max
#endif