#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h> /* mmap() is defined in this header */
#include <fcntl.h>
#include <errno.h>
#include <vector>
#include <sys/wait.h>

using namespace std;

struct commonData {
	vector<int> wordMap;
};
vector<int*> mmapAddr;
int main(){
	int size = 100 * sizeof(int);
	void *addr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	cout << "Mapped at " << addr << endl;
	// cout << addr << endl;
	void *address = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	cout << "Mapped at " << address << endl;
	int* shared = (int*) addr;
	int* shared1 = (int*) address;
	mmapAddr.push_back(shared);
	mmapAddr.push_back(shared1);
	pid_t mychild = fork();
	int status = 0;
	pid_t wpid;
	if(mychild > 0){
		// shared[0] = 1;
		mmapAddr[0][0] = 10;
		mmapAddr[0][1] = 155;
		// cout << shared[0].size() << endl;
		// shared[0].push_back(4);
		cout << mmapAddr[0][0] << endl;
		// wordMap.push_back(5);
		// cout << wordMap[0] << endl;
		// shared[0] = wordMap;
		// cout << shared[0].size() << endl;
		// cout << shared[0][0] << endl;
		// shared[0].push_back(6);
		// cout << "shared: " << shared[0][1] << endl;
		// shared[1] = 20;
		// shared[99] = 5;
		return 0;
	}
	while ((wpid = wait(&status)) > 0);
	// cout << "IN PARENT" << endl;
	// vector<int> newMap = shared[0];
	cout << "in parent " << mmapAddr[0][1] << endl;
	// cout << "parent: " << shared[0][0] << endl;
	// cout << shared->wordMap.size() << endl;
	// cout << shared->wordMap[0] << endl;
	// cout << shared[1] << " " << shared[0] << endl;
	// cout << shared[99] << endl;

	munmap(addr, size);

	return 0;
}

