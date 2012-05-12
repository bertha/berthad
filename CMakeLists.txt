project(berthad)

# XXX We really need 2.6?
cmake_minimum_required(VERSION 2.6)

set (CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

include(CheckFunctionExists)
include(CheckCCompilerFlag)

MACRO(ADD_C_COMPILER_FLAG_IF_AVAILABLE FLAG RESULT)
        check_c_compiler_flag("${FLAG}" ${RESULT})
        if(${RESULT})
                add_definitions("${FLAG}")
        endif(${RESULT})
ENDMACRO(ADD_C_COMPILER_FLAG_IF_AVAILABLE)

add_c_compiler_flag_if_available(-std=c99 HAVE_CFLAG_STDC99)
add_c_compiler_flag_if_available(-Wall HAVE_CFLAG_WALL)
add_c_compiler_flag_if_available(-pedantic HAVE_CFLAG_PEDANTIC)

check_function_exists(splice HAVE_SPLICE)
check_function_exists(sendfile HAVE_SENDFILE)
check_function_exists(fallocate HAVE_FALLOCATE)
check_function_exists(posix_fadvise HAVE_POSIX_FADVISE)

configure_file(${CMAKE_SOURCE_DIR}/config.h.in ${CMAKE_SOURCE_DIR}/config.h)

find_package(Glib)
find_package(Gthread)

add_executable(berthad-vfs berthad-vfs.c)

include_directories(${Glib_INCLUDE_DIRS})
target_link_libraries(berthad-vfs ${Glib_LIBRARIES} ${Gthread_LIBRARIES})

install (TARGETS berthad-vfs DESTINATION bin)
