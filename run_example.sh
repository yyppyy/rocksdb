#LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd) && export LD_LIBRARY_PATH
sudo umount /mnt/yanpeng/db
sudo mount -t tmpfs -o size=8192m tmpfs /mnt/yanpeng/db
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space

cd ./examples
#make simple_example && cp simple_example test_mltthrd
./test_mltthrd $1 $2 /test_program_log/ycsb/load_workload$3.dat /test_program_log/ycsb/run_workload$3.oplist
