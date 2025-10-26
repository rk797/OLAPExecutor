cmake_minimum_required(VERSION 3.16)
project(OlapEngine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

message(STATUS "Checking for compiler")
if(MSVC)
    # Enable AVX2
    add_compile_options(/arch:AVX2)
    
    add_compile_options(/MP) 
    
    message(STATUS "MSVC compiler detected. Enabling /arch:AVX2 and /MP (multi-core build)")
else()
    # Enable AVX2
    add_compile_options(-mavx2)
    
    message(STATUS "GCC/Clang compiler detected. Enabling -mavx2")
    message(STATUS "For parallel builds on Linux, run 'make -j8' or 'ninja'")
endif()


find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

message(STATUS "Found Apache Arrow version: ${Arrow_VERSION}")

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(engine ${SOURCES})

# pch
target_precompile_headers(engine 
    PRIVATE 
        "src/pch.h"
)


target_link_libraries(engine 
    PUBLIC 
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
)

target_include_directories(engine PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")
