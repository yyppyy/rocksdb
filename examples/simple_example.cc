// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sched.h>
#include <sys/time.h>
#include <fcntl.h>
#include <iostream>
#include <atomic>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/slice_transform.h"


/* rocksdb */
#define MEMTABLE_SIZE (1L << 29)
#define NUM_MEMTABLE 1
#define BLK_CACHE_SIZE (64L << 20)
#define BLK_CACHE_SIZE_MB (BLK_CACHE_SIZE >> 20)
#define NUM_BUCKET 10000

/* ycsb */
#define MAX_VALUE_SIZE 1024
#define MAX_LINE_SIZE (MAX_VALUE_SIZE + 256)
//#define MAX_OP_NUM  1000000UL
#define MAX_LOAD_OP_NUM  16000UL
#define MAX_RUN_OP_NUM  32000UL
#define LOWER_DATASET_FACTOR 1
#define MAX_FILE_LEN 64

/* MIND */
#define NUM_CORES_PER_BLADE 10
#define MIND_MAX_THREAD NUM_CORES_PER_BLADE
#define MIND_MAX_BLADE 8
#define MIND_NUM_MAX_THREAD (MIND_MAX_THREAD * MIND_MAX_BLADE)

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::SliceTransform;
using namespace std;

struct hash_test_args {
    int node_id;
    int thread_id;
    int global_thread_id;
    int num_node;
    int num_thread_per_blade;
    int num_thread_tot;
    char load_file[MAX_FILE_LEN];
    char run_file[MAX_FILE_LEN];
    //pthread_barrier_t *barrier_begin;
    //pthread_barrier_t *barrier_end;
    atomic_int *barrier_begin;
    atomic_int *barrier_end;
    int num_op;
    struct hash_test_ycsb_ops *oplist;
    double res_time_in_ms;
};

struct hash_test_ycsb_ops
{
    uint8_t opcode;
    uint64_t key;
    char value[MAX_VALUE_SIZE];
};

enum {
    arg_node_num = 1,
    arg_thread_num,
    arg_load_file,
    arg_run_file,
    arg_db_path,
    argc_num
};

enum{
    YCSB_READ = 1,
    YCSB_UPDATE = 2,
};

atomic_int barrier_begin, barrier_end;

//std::string kDBPath = "/mnt/yanpeng/db";
DB *db;


// The prefix is the first min(length(key),`cap_len`) bytes of the key, and
// all keys are InDomain.
// extern const SliceTransform* NewCappedPrefixTransform(size_t cap_len);

DB* init_rocksdb(const string DBPath) {
    DB* db;
    Options options;

    options.create_if_missing = true;

    /* trust auto opt for now */
    //options.OptimizeLevelStyleCompaction(MEM_TABLE_SIZE);

    /* opt for in-memory db*/
    //options.OptimizeForPointLookup(BLK_CACHE_SIZE_MB);

    /* no bg threads*/
    options.max_background_jobs = 0;

    /* mmap rw tmpfs */
    options.allow_mmap_reads = true;
    options.allow_mmap_writes = true;

    /* memory usage */
    options.write_buffer_size = MEMTABLE_SIZE;
    options.max_write_buffer_number = NUM_MEMTABLE;
    options.min_write_buffer_number_to_merge = NUM_MEMTABLE + 1;

    /* memtable options */
    options.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(8));
    options.memtable_factory.reset(rocksdb::NewHashLinkListRepFactory(NUM_BUCKET));

    /* table options */
    BlockBasedTableOptions table_options;
    // table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
    table_options.no_block_cache = true; /* we won't hit disk */
    // table_options.block_restart_interval = 4;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    /* others */
    options.allow_concurrent_memtable_write = false;
    options.compression = rocksdb::CompressionType::kNoCompression;
    options.max_open_files = -1;

    /* debug log*/
    options.info_log_level = rocksdb::InfoLogLevel::INFO_LEVEL;

    // open DB
    Status s = DB::Open(options, DBPath, &db);
    if (!s.ok()) cerr << s.ToString() << endl;
    assert(s.ok());

    printf("--- RocksDB Configuration ---\n");
    printf("db_path: %s\nlogging level: %d\nmemtable size: %ldMB, num memtable: %d\n",
        DBPath.c_str(), options.info_log_level, MEMTABLE_SIZE >> 20, NUM_MEMTABLE);

    return db;
}

static int pin_to_core(int core_id) {
    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (core_id < 0 || core_id >= num_cores)
        return -1;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

static double timediff_in_ms(struct timeval *begin, struct timeval *end) {
    double diff_sec = end->tv_sec - begin->tv_sec,
            diff_usec = end->tv_usec - begin->tv_usec;
    return diff_sec * 1000 + diff_usec / 1000;
}

static int is_write_rec_for_thread(uint64_t op_idx, uint64_t key, int node_id, int num_nodes, int thread_id, int num_threads) {
    unsigned int num_workers = num_nodes * num_threads;
    int global_th_id = (node_id * num_threads) + thread_id;
    return op_idx % num_workers == global_th_id;
}

static uint64_t parse_load_ycsb(FILE *fp, uint64_t max_ops, int blade_id, int num_nodes, int num_threads) {
    uint64_t op_idx = 0, actual_load = 0;
    if (!fp) {
        return 0;
    }

    char *line = (char *)malloc(MAX_LINE_SIZE);
    unsigned int num_workers = num_nodes * num_threads;
    size_t len = 0;
    ssize_t read = 0;
    int global_th_id_base = (blade_id * num_threads);
    int rc;
    WriteOptions wopts;
    wopts.disableWAL = true;
    /*
    for (int thread_id = 0; thread_id < num_threads; thread_id++) {
        printf("Load data[%d]: write key[%lu - %lu]\n",
                    thread_id, get_start_chunk_idx(sHash, blade_id, num_nodes, thread_id, num_threads),
                    get_end_chunk_idx(sHash, blade_id, num_nodes, thread_id, num_threads));
    }
    */

    while (op_idx < (max_ops / LOWER_DATASET_FACTOR)) {
        read = getline(&line, &len, fp);
        if (read == -1)
            break;  // end of the file
        
        if (line[0] == 'I') {
            char ops[16] = {0};
            char dummy_table[16] = {0};
            uint64_t key_val;
            char dummy_val[MAX_VALUE_SIZE] = {0};   // not dummy, actually
            if (strncmp(line, "INSERT", 6) == 0) {
                sscanf(line, "%s %s user%lu [ field0=%1000c", ops, dummy_table, &key_val, dummy_val);
                for (int thread_id = 0; thread_id < num_threads; thread_id++) {
                    // int global_th_id = global_th_id_base + thread_id;
                    if (is_write_rec_for_thread(op_idx, key_val, blade_id, num_nodes, thread_id, num_threads)) {
                      //TODO
                      Status s = db->Put(wopts, to_string(key_val), string(dummy_val));
                      if (!s.ok()) cerr << s.ToString() << endl;
                      assert(s.ok());
                    }
                }
            }
            op_idx ++;
        }
    }
    if (line)
        free(line);
    printf("Total load operations: %lu\n", actual_load);
    return op_idx;
}

static void *run_rocksdb_ycsb(void *args)
{
    struct hash_test_args *t_args;
    char *load_file, *run_file;
    size_t file_size;
    FILE *fp_ycsb_load;
    int fd_ycsb_run;
    struct stat st;
    struct hash_test_ycsb_ops *oplist;
    uint64_t num_op, op_idx_base;
    int blade_id, thread_id, global_thread_id;
    int num_node, num_thread_per_blade, num_thread_tot;
    uint64_t err;
    uint64_t print_period;
    int64_t read_len;
    double t_tot = 0;
    //pthread_barrier_t *barrier_begin, *barrier_end;
    atomic_int *barrier_begin, *barrier_end;
    volatile char *val = (char *)malloc(MAX_VALUE_SIZE);
    memset((void *)val, MAX_VALUE_SIZE, 0);

    t_args = (struct hash_test_args *)args;
    blade_id = t_args->node_id;
    thread_id = t_args->thread_id;
    num_node = t_args->num_node;
    num_thread_per_blade = t_args->num_thread_per_blade;
    num_thread_tot = t_args->num_thread_tot;
    global_thread_id = blade_id * num_thread_per_blade + thread_id;
    barrier_begin = t_args->barrier_begin;
    barrier_end = t_args->barrier_end;
    load_file = t_args->load_file;
    run_file = t_args->run_file;
    
    // parse load file
    if (thread_id == 0) {

#ifdef DISABLE_CONCURRENT_INSERT
        sleep((blade_id + 1) * 20);
        std::printf("wakeup and load ycsb\n");
#endif
        // parse and do load file
        fp_ycsb_load = fopen(load_file, "r");
        if (fp_ycsb_load) {
            fseek(fp_ycsb_load, 0L, SEEK_END);
            printf("File size: %lu MB\n", ftell(fp_ycsb_load) / 1024 / 1024);
            fseek(fp_ycsb_load, 0L, SEEK_SET);
        } else {
            printf("Cannot open file: %s\n", load_file);    
            exit(-1);
        }
        parse_load_ycsb(fp_ycsb_load, MAX_LOAD_OP_NUM, blade_id, num_node, num_thread_per_blade);
        fclose(fp_ycsb_load);

        // map oplist file
        fd_ycsb_run = open(run_file, O_RDONLY);
        if (fd_ycsb_run < 0) {
            printf("Can not open %s\n", run_file);
            return NULL;
        }
        fstat(fd_ycsb_run, &st);
        file_size = st.st_size;
        oplist = (struct hash_test_ycsb_ops *)mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd_ycsb_run, 0);
        if (oplist == MAP_FAILED) {
            printf("Can not mmap data_buf\n");
            return NULL;
        }
        close(fd_ycsb_run);

        // assign oplist to other threads on same blade
        num_op = t_args->num_op = file_size / sizeof(struct hash_test_ycsb_ops) / num_thread_tot;
        if (num_op > MAX_RUN_OP_NUM / num_thread_tot) num_op = MAX_RUN_OP_NUM / num_thread_tot;
        op_idx_base = num_op * global_thread_id;
        oplist += op_idx_base;
        for (int i = 0; i < num_thread_per_blade; ++i) {
            t_args[i].oplist = oplist + num_op * i;
            t_args[i].num_op = num_op;
            //last few op can be ignored, its OK.
        }
        printf("* YCSB run blade idx base[%ld]\n", num_op * global_thread_id);
    }

    // sync all threads on all blades
    //pthread_barrier_wait(barrier_begin);
    barrier_begin->fetch_add(1, memory_order_release);
    while (barrier_begin->load(memory_order_acquire) != num_thread_tot)
        ;

    // retrieve oplist
    if (thread_id != 0) {
        oplist = t_args->oplist;
        num_op = t_args->num_op;
    }

    //pin_to_core(global_thread_id % MIND_MAX_THREAD);
    //printf("* YCSB run gtid[%d] tid[%d] cpu[%d] num_op[%lu] oplist_off[%ld]\n",
    //    global_thread_id, thread_id, global_thread_id % MIND_MAX_THREAD, num_op, (oplist - (t_args - thread_id)->oplist));

#ifdef DISABLE_CONCURRENT_INSERT
        sleep(blade_id * 30);
#endif

    // run oplist
    err = 0;
    print_period = num_op / 10;
    Status s;
    ReadOptions ropts;
    WriteOptions wopts;
    ropts.verify_checksums = false;
    wopts.disableWAL = true;
    string read_value;
    // auto t_start = std::chrono::high_resolution_clock::now();
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
    for (uint64_t i = 0; i < num_op; i++) {
        if (oplist[i].opcode == YCSB_READ) {
            //TODO
            s = db->Get(ropts, to_string(oplist[i].key), &read_value);
            if (!s.ok() && !s.IsNotFound()) {
                std::printf("%s\n", s.ToString().c_str());
            }
        } else if (oplist[i].opcode == YCSB_UPDATE) {
            //TODO
            s = db->Put(wopts, to_string(oplist[i].key), string(oplist[i].value));
            if (!s.ok() && !s.IsNotFound()) {
                std::printf("%s\n", s.ToString().c_str());
            }
        } else {
            printf("unexpected opcode[%d]\n", oplist[i].opcode);
        }
        if (i % print_period == 0) {
            std:printf("%lu ops done\n", i);
        }
    }
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);
    t_args->res_time_in_ms = t_tot = timediff_in_ms(&tv_begin, &tv_end);

    /*
    auto t_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> t_double = t_end - t_start;
    t_args->res_time_in_ms = t_tot = t_double.count();
    */

    printf("** Finished [thread: %d] %lu | %.3lf MOPS, finished in %.2lf ms\n",
                global_thread_id, num_op, ((double)num_op / t_tot / 1000), t_tot);

    //done oplist
    //pthread_barrier_wait(barrier_end);
    barrier_end->fetch_add(1, memory_order_release);
    while (barrier_end->load(memory_order_acquire) != num_thread_tot)
        ;

    // don't exit
    while (1);

    // clean up memory
    free((char *)val);
    return NULL;
}


static int launch_workers(int num_node, int num_thread, char *load_file, char *run_file) {
    uint64_t i = 0;
    pthread_t threads[MIND_NUM_MAX_THREAD];
    struct hash_test_args thread_args[MIND_NUM_MAX_THREAD] = {0};
    void *status;
    int res;
    uint64_t num_ops = 0, total_num_ops = 0;
    uint64_t num_ops_per_thread = 0;
    int num_thread_tot = num_node * num_thread;
    //pthread_barrier_t barrier_begin, barrier_end;
    //pthread_barrier_init(&barrier_begin, NULL, num_thread_tot);
    //pthread_barrier_init(&barrier_end, NULL, num_thread_tot + 1);
    barrier_begin.store(0, memory_order_release);
    barrier_end.store(0, memory_order_release);

    if (num_thread_tot > MIND_NUM_MAX_THREAD) {
        printf("Cannot create more than %d threads\n", MIND_NUM_MAX_THREAD);
        return -1;
    }

    // launch remote threads
    for (int tid = 0; tid < num_thread; ++tid) {
        for (int nid = 0; nid < num_node; ++nid) {
            int i = nid * num_thread + tid;
            thread_args[i].res_time_in_ms = 0;
            thread_args[i].node_id = i / num_thread;
            thread_args[i].thread_id = i % num_thread;
            thread_args[i].global_thread_id = i;
            thread_args[i].num_node = num_node;
            thread_args[i].num_thread_per_blade = num_thread;
            thread_args[i].num_thread_tot = num_thread_tot;
            memset(thread_args[i].load_file, 0, sizeof(thread_args[i].load_file));
            memcpy(thread_args[i].load_file, load_file, strlen(load_file));
            memset(thread_args[i].run_file, 0, sizeof(thread_args[i].run_file));
            memcpy(thread_args[i].run_file, run_file, strlen(run_file));
            thread_args[i].barrier_begin = &barrier_begin;
            thread_args[i].barrier_end = &barrier_end;

            res = pthread_create(&threads[i], NULL, run_rocksdb_ycsb, &thread_args[i]);
            if (res) {
                printf("Error: unable to create thread: %d\n", res);
                exit(-1);
            }

            sleep(1);
        }
    }

    //wait all worker finish
    //pthread_barrier_wait(&barrier_end);
    while (barrier_end.load(memory_order_acquire) != num_thread_tot)
        ;

    double total_time = 0;
    for (i = 0; i < num_thread_tot; i++) {
        total_num_ops += thread_args[i].num_op;
        if (total_time < thread_args[i].res_time_in_ms)
            total_time = thread_args[i].res_time_in_ms; // maximum time
    }

    if (total_time > 0) {
        printf("* Finished: num_op %lu | %.3lf MOPS\n", total_num_ops, (double)total_num_ops / total_time / 1000);
    } else {
        printf("* No total time!\n");
    }
}

int main(int argc, char *argv[]) {
    int num_nodes = 1;
    int num_threads_per_node = 2;
    int num_threads_tot = 2;
    string db_path;

    if (argc < argc_num) {
        printf("Not enough arguments...terminate\n");
        return -1;
    }

    num_nodes = atoi(argv[arg_node_num]);
    num_threads_per_node = atoi(argv[arg_thread_num]);
    num_threads_tot = num_nodes * num_threads_per_node;
    db_path = string(argv[arg_db_path]);
    printf("--- Test Configuration ---\n");
    printf("num blades: %d, threads per blade: %d, tot threads: %d\n",
        num_nodes, num_threads_per_node, num_threads_tot);

    /* rocksdb init*/
    db = init_rocksdb(db_path);

    /* launch workers on blades*/
    printf("launching workers on remote blades...\n");
    launch_workers(num_nodes, num_threads_per_node, argv[arg_load_file], argv[arg_run_file]);

    /* do not exit */
    while (1);

	return 0;
}