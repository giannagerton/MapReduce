/* mapred –-app [wordcount, sort] –-impl [procs, threads]
--maps num_maps –-reduces num_reduces --input infile
–-output outfile */
/* mapred --app wordcount --impl procs --maps 10 ---reduces 5 --input word_input.txt
-- output whatever */
/* g++ -o mapred ./mapred2.cpp */
/* ./mapred --app wordcount --impl procs --maps 10 --reduces 5 --input word_input.txt --output outfile.txt */
#include <iostream>
#include <fstream>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <vector>
#include <unistd.h>
#include <sys/shm.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <algorithm>
#include <sys/wait.h>

using namespace std;
vector<int> mapSemVec;
vector<int> reduceSemVec;
// vector<int> mapPID;
// vector<int> reducePID;
// vector<int> mapShmSid;

struct commonData {
	// int index;
	vector <string> wordVector;
	vector<std::pair <std::string,int> > wordMap;
	vector<std::pair <std::string,int> > newMap;
	vector <int> value;
	// std::pair <std::string,int> pairs; 
	// map<string, int> mapOfWords;
struct CommonData* next;
};

vector<std::pair <std::string,int> > vecOfPairs;
vector<commonData*> mapShm;
vector<int> mapShmid;

int map(int numOfProcess){
	cout << "IN map" << endl;
	struct commonData* shm = mapShm[numOfProcess];
	vector<string> wordVec = shm->wordVector;

	std::pair <std::string,int> pairs;
  	for(int i = 0; i < wordVec.size(); i++){
		// cout << "HERE " << i << endl;
		std::transform(wordVec[i].begin(), wordVec[i].end(), wordVec[i].begin(), ::tolower);
  		pairs.first = wordVec[i];
  		pairs.second = 1;
  		shm->wordMap.push_back(pairs);
  	}

  	for(int i = 0; i < shm->wordMap.size(); i++){
  		if(shm->newMap.size() == 0){
  			pairs.first = shm->wordMap[i].first;
  			pairs.second = shm->wordMap[i].second;
  			shm->newMap.push_back(pairs);
  			continue;
  		}
  		for(int j = 0; j < shm->newMap.size(); j++){
  			// cout << shm->newMap[j].first << " vs. " << shm->wordMap[i].first << endl;
  			if(shm->newMap[j].first == shm->wordMap[i].first){
  				shm->newMap[j].second = shm->newMap[j].second + 1;
  				break;
  			}
  			if(shm->newMap[j].first > shm->wordMap[i].first){
  				pairs.first = shm->wordMap[i].first;
  				pairs.second = shm->wordMap[i].second;
  				shm->newMap.insert(shm->newMap.begin()+ j, pairs);
  				break;
  			}
  			if(j+1 == shm->newMap.size()){
				pairs.first = shm->wordMap[i].first;
  				pairs.second = shm->wordMap[i].second;
				shm->newMap.push_back(pairs);
			}
  		}
  	}
  	for(int i = 0; i < shm->newMap.size(); i++){
  		cout << shm->newMap[i].first << endl;
  		cout << shm->newMap[i].second << endl;
	}
	// cout << "value " << shm->value.size() << endl;
  	// for(int i = 0; i < shm->newMap.size(); i++){
  	// 	cout << shm->newMap[i].first << " " << shm->newMap[i].second << endl; 
  	// }

	return 0;
}

vector<string> parseInput(string content){
	// This method takes in two inputs: number of Nodes and string with all text
	// Then extracts words from string using delimiters and add each word to the wordVector
	// return vector of all of the words
	vector<string> wordVector;
	int length = content.length();
  	int i = 0;
  	string buffer;

  	while (i < content.length()){
  		if(content[i] == ' ' || content[i] == ':' || content[i] == '!' || content[i] == '.' || content[i] == ',' 
  			|| content[i] == ';' || content[i] == '-' || content[i] == '\r' || content[i] == '\n'){
  			if(!buffer.empty()){
  				wordVector.push_back(buffer);
  				buffer.clear();
  			}
  			i++;
  		}
  		else{
  			buffer += content[i];
  			i++;
  			if(i == content.length()){
  				wordVector.push_back(buffer);
  			}
  		}
  	}
  	// distributeData(wordVector);
  	return wordVector;
}
// void addToSharedMem(vector<vector <string> > nodeVector){
// 	cout << "size of mapshm: " << mapShm.size() << endl;
// 	for (int i = 0; i < mapShm.size(); i++){
// 		mapShm[i]->wordVector = nodeVector[i];
// 	}
// }

void createSharedMem(vector<vector <string> > nodeVector, int numNodes) {
	char c;
	pid_t pid;
	int shmflg; /* shmflg to be passed to shmget() */ 
	size_t shmSize = 4096; /* size to be passed to shmget() */ 
	void* s;
	// size = sizeof(vector<vector <string> >) + (sizeof(string) * nodeVector.size() * sizeof(nodeVector[0]));
	// size = sizeof(vector<vector <string> >);
	for(int i = 0 ; i < numNodes; i++){
		struct commonData* shm; /* returns a pointer */
		int shmid; /* return value from shmget() */ 
		cout << "here " << endl;
		key_t mem_key;
		mem_key = ftok(".", i);
		shmid = shmget (IPC_PRIVATE, shmSize, IPC_CREAT | 0666);
		mapShmid.push_back(shmid);
		cout << shmid << endl;
		if (shmid == -1) {
			perror("shmget: shmget failed"); 
			exit(1); 
		}

		if ((shm = (struct commonData*)shmat(shmid, NULL, 0)) == (struct commonData*)-1) {
	        perror("shmat");
	        exit(1);
	    }
		mapShm.push_back(shm);
	}
	for (int i = 0; i < mapShm.size(); i++){
		mapShm[i]->wordVector = nodeVector[i];
	}

	// for(int i = 0; i < mapShm[1]->wordVector.size(); i++){
	// 	cout << mapShm[1]->wordVector[i] << endl;
	// }
}

int distributeData(vector<string> wordVector, int numNodes){

	/* takes in word vector which has all of the words from the file in one vector and
	partions these words between the nodes (one vector per node) and saves those vectors in a vector of vectors */
	int wordVecLength = wordVector.size();
  	int wordsPerNode = wordVecLength/numNodes;
  	int wordsLeftOver = 0;
  	vector<vector<string> >nodeVector;

  	if((wordsPerNode * numNodes) != wordVecLength){
  		wordsLeftOver = wordVecLength - (wordsPerNode * numNodes);
  	}
 	for(int j = 0; j < numNodes; j++){
		vector<string> tempVec;
		int limit = wordVector.size() - wordsPerNode;
 		for(int i = wordVector.size()-1; i > limit; i--){
			tempVec.push_back(wordVector[i]);
			wordVector.pop_back();
 		}
 		nodeVector.push_back(tempVec);
 	}
 	if(wordsLeftOver == 0){
 		nodeVector[0].push_back(wordVector[0]);
 	}
 	else if(wordsLeftOver > 0){
 		int var = 0;
 		while(var != wordsLeftOver){
 			nodeVector[var].push_back(wordVector[var]);
 			var++;
 		}
 	}
 	// forkYeah(numNodes);
 	createSharedMem(nodeVector, numNodes);
 	// nodeVector contains n vectors (one for each node as given from user input)
 	return 0;
}

int split(int num_maps, string content){
	distributeData(parseInput(content), num_maps);
	// for(int i = 0; i < wordVector.size(); i++) {
	// 	cout << wordVector[i] << endl;
	// }
	return 0;
}

int shuffle() {
	std::pair <std::string,int> pairs;
	vector<std::pair <std::string,int> > sortedMap;
	// for(int i = 0; i < mapShm.size(); i++){
	cout << "Here in shuffle " << endl;
	vector<std::pair <std::string, int> > tempMap = mapShm[0]->newMap;
	// cout << "Here in shuffle " << endl;
	if(sortedMap.size() == 0){
		for(int i = 0; i < tempMap.size(); i++){
			pairs.first = tempMap[i].first;
			pairs.second = mapShm[0]->newMap[i].second;
			// cout << "new pairs: " << pairs.second << endl;
			sortedMap.push_back(pairs);
		}
	}
	int shmIndex = 1;
	// cout << "size of mapShm[3]: " << mapShm[shmIndex]->newMap.size() << endl;
	// giving me a segmentation fault :(
	// // while(shmIndex != mapShm.size()){
	// // 	// vector<std::pair <std::string, int> > tempMap2;
	// // 	cout << "shmIndex: " << shmIndex << endl;
	// // 	cout << "size of mapShm[shmIndex]: " << mapShm[shmIndex]->newMap[.size()] << endl;
	// // 	// tempMap2 = mapShm[shmIndex]->newMap;
	// // 	cout << "temp map size: " << mapShm[shmIndex]->newMap.size() << endl;
	// // 	for(int i = 0; i < mapShm[shmIndex]->newMap.size(); i++){
	// // 		cout << "HEREE  " << i << endl;
	// // 		cout << "sortedMap size " << sortedMap.size() << endl;
	// // 		for(int j = 0; j < sortedMap.size(); j++){
	// // 			cout << "inside " << j << endl;
	// // 			cout << "test word " << mapShm[shmIndex]->newMap[i].first << endl;
	// // 			cout << "word in sorted " << sortedMap[j].first << endl;
	// // 			if(mapShm[shmIndex]->newMap[i].first <= sortedMap[j].first){
	// // 				cout << "test word " << mapShm[shmIndex]->newMap[i].first << endl;
	// // 				cout << "word in sorted " << sortedMap[j].first << endl;
	// // 				pairs.first = mapShm[shmIndex]->newMap[i].first;
	// // 				pairs.second = mapShm[shmIndex]->newMap[i].second;
 // //  					sortedMap.insert(sortedMap.begin() + j, pairs);
	// // 				break;
	// // 			}
	// // 			if(j+1 == sortedMap.size()){
	// // 				pairs.first = mapShm[shmIndex]->newMap[i].first;
	// // 				pairs.second = mapShm[shmIndex]->newMap[i].second;
	// // 				sortedMap.push_back(pairs);
	// // 			}
	// // 		}
	// // 	}
	// 	shmIndex++;
	// }
	return 0;
}

void deleteSHM(){
	 // destroy the shared memory segment. 
  	for(int i = 0; i < mapShmid.size(); i++){
		if (shmctl(mapShmid[i], IPC_RMID, NULL) == -1) {
			perror("main: shmctl: ");
		}
	}
}


int forkMaps(int num_maps){
	int i = 0;
	int status = 0;
	pid_t wpid;
	for(i = 0; i < num_maps; i++){
		pid_t pid = fork();
		if(pid == 0) // only execute this if child
		{
		    map(i);
		    for(int i = 0; i < mapShm[0]->newMap.size(); i++){
		  		cout << "in child: " <<  mapShm[0]->newMap[i].first << " " << mapShm[0]->newMap[i].second << endl;
			}
		    // cout << "map size: " << mapShm[i]->newMap.size();
			// std::pair <std::string,int> pairs;
		 //    for(int j = 0; j < mapShm[i]->newMap.size(); j++){
		 //    	cout << "in for " << endl;
		 //    	pairs.first = mapShm[i]->newMap[j].first;
		 //    	pairs.second = mapShm[i]->newMap[j].second;
		 //    	cout << "pairs " << pairs.first << " " << pairs.second << endl;
		 //  		// cout << "in child: " << mapShm[i]->newMap[j].second << endl;
			// 	vecOfPairs.push_back(pairs);
			// 	cout << "size: " << vecOfPairs.size() << endl;
			// }
			return 0;
		}
	}

	while ((wpid = wait(&status)) > 0); // only the parent waits

	// vector<std::pair <std::string,int> > pairMap = vecOfPairs[0];
	// cout << "i am here" << endl;
	// cout << mapShmid[0] << endl;
	// key_t mem_key = mapShmid[0];
	// cout << mem_key << endl;
	// int shmid = shmget (mem_key, 4096, IPC_CREAT | 0666);
	// cout << shmid << endl;
	// struct commonData* shm_addr = (struct commonData*) shmat(shmid, NULL, 0);
	// for(int i = 0; i < shm_addr->newMap.size(); i++){
 //  		cout << "in parent: " << shm_addr->newMap[i].second << endl;
	// }
	// cout << shm_addr << endl;
	for(int i = 0; i < mapShm[0]->newMap.size(); i++){
  		cout << "in parent: " <<  mapShm[0]->newMap[i].first << " " << mapShm[0]->newMap[i].second << endl;
	}
	// for(int k = 0; k < mapShmid.size(); k++){
	// 	cout << "shmid: " << mapShmid[k] << endl;
	// }
	deleteSHM();
	// shuffle();
	// deleteSHM();
	return 0;
}

// void forkReduces(int num_reduces){
// 	for(i = 0; i < num_reduces; i++){
// 		pid_t pid = fork();
// 		if(pid == 0) // only execute this if child
// 		{
// 			reducePID.push_back(getpid());
// 		    /* wait on the semaphore, unless it's value is non-negative. */
// 		    sem_op.sem_num = 0;
// 		    sem_op.sem_op = -1;
// 		    sem_op.sem_flg = 0;
// 		    semop(reduceSemVec[i], &sem_op, 1);
// 		    cout << i << " " << getpid() << endl;
// 			return 0;
// 		}
// 	}
// 	// shm->index++;
// 	// while ((wpid = wait(&status)) > 0); // only the parent waits
// 	cout << "here in parent " << getpid() << endl;
// }

int main(int argc, char* argv[]){
	string app, impl, infile, outfile;
	int num_maps, num_reduces;
	int parent_pid = getpid();

	for(int i = 1; i < argc; i++){
		if(strcmp(argv[i], "--app") == 0){
			app = argv[i+1];
		}
		else if(strcmp(argv[i], "--impl") == 0){
			impl = argv[i+1];
		}
		else if(strcmp(argv[i], "--maps") == 0){
			num_maps = atoi(argv[i+1]);
		}
		else if(strcmp(argv[i], "--reduces") == 0){
			num_reduces = atoi(argv[i+1]);
		}
		else if(strcmp(argv[i], "--input") == 0){
			infile = argv[i+1];
		}
		else if(strcmp(argv[i], "--output") == 0){
			outfile = argv[i+1];
		}
	}
	// cout << app << " " << impl << " " << num_maps << " " << num_reduces << " " << infile << " " << outfile << endl;
	// cout << num_maps << endl;
	ifstream ifs(infile.c_str());
  	string content( (istreambuf_iterator<char>(ifs) ),
                       (istreambuf_iterator<char>()    ) );
  	split(num_maps, content);
  	forkMaps(num_maps);
  // 	if(getpid() == parent_pid){
  // 		for(int i = 0; i < mapShm[9]->newMap.size(); i++){
  // 			cout << "in parent: " << mapShm[9]->newMap[i].second << endl;
		// }
  // 	}
  	// for(int i = 0; i < mapShm.size(); i++){
  	// 	cout << mapShm[i] << endl;
  	// }
  	// forkReduces(num_reduces);
  	// createSharedMem(num_maps);
  	// cout << "hello" << mapPID.size() << endl;
  	ifs.close();


  	return 0;
}