/*
 * Profiling points
 *
 * We borrowed profiling system from LegoOS.
 * LegoOS: https://github.com/WukLab/LegoOS
 * 
 * We added profiling outside of kernel itself.
 * For example, kernel module which is not compiled with 
 * the kernel can use profiling points defined in the kernel
 * and use PROFILE_LEAVE_PTR instead of PROFILE_LEAVE.
 * Those profiling points used outside of the kernel
 * should be exported in this file by using 
 * PROTO_PROFILE_WITH_EXPORT()
 */

#pragma once

#include <string>

#define CONFIG_PROFILE_POINTS
#define MAX_PROFILE_POINTS 32
#define MAX_PROFILE_THREADS 16
#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

enum {
    PP_MUTEX = 0,
	PP_WRITE_WAIT,
	NUM_PP
};

struct profile_point {
    unsigned long nr;
    double time_us;
};

struct alignas(PAGE_SIZE) profile_point_arr {
	struct profile_point arr[MAX_PROFILE_POINTS];
};

void print_profile_points(void);
void profile_add(int tid, int pp, double time_us);

/*
#define PROFILE_POINT_TIME \
	unsigned long t_start __maybe_unused;

#define PROFILE_START             \
	do                                  \
	{                                   \
		auto t_start = std::chrono::high_resolution_clock::now(); \
	} while (0)

#define PROFILE_LEAVE(tid, pp)                                         \
	do                                                              \
	{                                                               \
		auto t_end = std::chrono::high_resolution_clock::now();                                \
		std::chrono::duration<double, std::micro> t_double = t_end - t_start;                              \
		profile_add(tid, pp, t_double.count())										\
	} while (0)
*/

#define PROFILE_START             \
	auto t_start = std::chrono::high_resolution_clock::now();

#define PROFILE_LEAVE(tid, pp)		\
		auto t_end = std::chrono::high_resolution_clock::now();                                \
		std::chrono::duration<double, std::micro> t_double = t_end - t_start;                              \
		profile_add(tid, pp, t_double.count());

