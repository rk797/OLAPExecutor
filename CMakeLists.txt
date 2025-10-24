cmake_minimum_required(VERSION 3.16)
project(OlapEngine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

message(STATUS "Checking for compiler")
if(MSVC)
    # Enable AVX-512
    add_compile_options(/arch:AVX512)
    
    add_compile_options(/MP) 
    
    message(STATUS "MSVC compiler detected. Enabling /arch:AVX512 and /MP (multi-core build)")
else()
    add_compile_options(-mavx512f)
    
    message(STATUS "GCC/Clang compiler detected. Enabling -mavx512f")
    message(STATUS "For parallel builds on Linux, run 'make -j8' or 'ninja'")
endif()

find_package(Arrow REQUIRED)
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
        Arrow::arrow_shared
)

target_include_directories(engine PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")