LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd) && export LD_LIBRARY_PATH
cd ./examples
make simple_example && cp simple_example test_mltthrd
./test_mltthrd $1 $2 /test_program_log/ycsb/load_workload$3.dat /test_program_log/ycsb/run_workload$3.oplist
