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
#include <atomic>

#define CONFIG_PROFILE_POINTS
#define MAX_PROFILE_POINTS 32
#define MAX_PROFILE_THREADS 16
#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif
#ifndef CACHE_SIZE
#define CACHE_SIZE (PAGE_SIZE * 16)
#endif

enum {
    //PP_MUTEX = 0,
	//PP_RWMUTEX_RLOCK,
	//PP_RWMUTEX_WLOCK,
	PP_DB_RLOCK,
	PP_DB_WLOCK,
	PP_MEMTABLE_RLOCK,
	PP_MEMTABLE_WLOCK,
	PP_WRITE_WAIT,
	PP_WRITE_BLOCKWAIT,
	PP_WRITE_CS,
	NUM_PP
};

struct profile_point {
    unsigned long nr;
    double time_us;
};

struct atomic_profile_point {
	std::atomic_ulong nr;
	std::atomic<double> time_us;
};

struct alignas(PAGE_SIZE) profile_point_arr {
	struct profile_point arr[MAX_PROFILE_POINTS];
};

struct alignas(PAGE_SIZE) atomic_profile_point_arr {
	struct atomic_profile_point arr[MAX_PROFILE_POINTS];
};

void print_profile_points(void);
void clear_profile_points(void);
void report_profile_points(void);
void profile_add(int pp, double time_us);

#define _PP_TIME(pp, var)	__##pp##var

#define PROFILE_START(pp)             \
	auto _PP_TIME(pp, t_start) = std::chrono::high_resolution_clock::now();

#define PROFILE_LEAVE(pp)		\
		auto _PP_TIME(pp, t_end) = std::chrono::high_resolution_clock::now();                                \
		std::chrono::duration<double, std::micro> _PP_TIME(pp, t_double) = _PP_TIME(pp, t_end) - _PP_TIME(pp, t_start);                              \
		profile_add(pp, _PP_TIME(pp, t_double).count());

