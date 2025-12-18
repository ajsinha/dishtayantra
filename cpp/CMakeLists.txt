cmake_minimum_required(VERSION 3.14)
project(dishtayantra_cpp VERSION 1.1.0 LANGUAGES CXX)

# C++17 required
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find Python and pybind11
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
find_package(pybind11 CONFIG REQUIRED)

# Source files
set(SOURCES
    src/dishtayantra_cpp.cpp
)

# Create the Python extension module
pybind11_add_module(dishtayantra_cpp ${SOURCES})

# Include directories
target_include_directories(dishtayantra_cpp PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Compiler-specific optimization flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dishtayantra_cpp PRIVATE
        $<$<CONFIG:Release>:-O3 -march=native -ffast-math>
        -Wall -Wextra -Wpedantic
    )
elseif(MSVC)
    target_compile_options(dishtayantra_cpp PRIVATE
        $<$<CONFIG:Release>:/O2 /fp:fast>
        /W4
    )
endif()

# Link-time optimization for release builds
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
endif()

# Installation
install(TARGETS dishtayantra_cpp
    LIBRARY DESTINATION .
    RUNTIME DESTINATION .
)

# Print configuration summary
message(STATUS "")
message(STATUS "DishtaYantra C++ Calculators Configuration:")
message(STATUS "  Version: ${PROJECT_VERSION}")
message(STATUS "  Python: ${Python3_VERSION}")
message(STATUS "  pybind11: ${pybind11_VERSION}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "")
