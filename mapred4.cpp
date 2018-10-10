/* mapred –-app [wordcount, sort] –-impl [procs, threads]
--maps num_maps –-reduces num_reduces --input infile
–-output outfile */
/* mapred --app wordcount --impl procs --maps 10 ---reduces 5 --input word_input.txt
-- output whatever */
/* g++ -o mapred ./mapred2.cpp */
/* ./mapred4 --app wordcount --impl procs --maps 10 --reduces 5 --input word_input.txt --output outfile.txt */
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
#include <sstream>
#include <sys/mman.h> /* mmap() is defined in this header */
#include <map>

using namespace std;
vector<int> mapSemVec;
vector<int> reduceSemVec;
vector<int*> mmapAddr;
vector<void*> mmapAddress;
vector<int> childPID;
vector<vector<string> >nodeVector;
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
multimap<string,int> mymultimap;
vector<commonData*> mapShm;
vector<int> mapShmid;

// int map(int numOfProcess){
// 	cout << "IN map" << endl;
// 	struct commonData* shm = mapShm[numOfProcess];
// 	vector<string> wordVec = shm->wordVector;

// 	std::pair <std::string,int> pairs;
//   	for(int i = 0; i < wordVec.size(); i++){
// 		// cout << "HERE " << i << endl;
// 		std::transform(wordVec[i].begin(), wordVec[i].end(), wordVec[i].begin(), ::tolower);
//   		pairs.first = wordVec[i];
//   		pairs.second = 1;
//   		shm->wordMap.push_back(pairs);
//   	}

//   	for(int i = 0; i < shm->wordMap.size(); i++){
//   		if(shm->newMap.size() == 0){
//   			pairs.first = shm->wordMap[i].first;
//   			pairs.second = shm->wordMap[i].second;
//   			shm->newMap.push_back(pairs);
//   			continue;
//   		}
//   		for(int j = 0; j < shm->newMap.size(); j++){
//   			// cout << shm->newMap[j].first << " vs. " << shm->wordMap[i].first << endl;
//   			if(shm->newMap[j].first == shm->wordMap[i].first){
//   				shm->newMap[j].second = shm->newMap[j].second + 1;
//   				break;
//   			}
//   			if(shm->newMap[j].first > shm->wordMap[i].first){
//   				pairs.first = shm->wordMap[i].first;
//   				pairs.second = shm->wordMap[i].second;
//   				shm->newMap.insert(shm->newMap.begin()+ j, pairs);
//   				break;
//   			}
//   			if(j+1 == shm->newMap.size()){
// 				pairs.first = shm->wordMap[i].first;
//   				pairs.second = shm->wordMap[i].second;
// 				shm->newMap.push_back(pairs);
// 			}
//   		}
//   	}
//   	for(int i = 0; i < shm->newMap.size(); i++){
//   		cout << shm->newMap[i].first << endl;
//   		cout << shm->newMap[i].second << endl;
// 	}
// 	// cout << "value " << shm->value.size() << endl;
//   	// for(int i = 0; i < shm->newMap.size(); i++){
//   	// 	cout << shm->newMap[i].first << " " << shm->newMap[i].second << endl; 
//   	// }

// 	return 0;
// }

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
  			|| content[i] == ';' || content[i] == '-' || content[i] == '\r' || content[i] == '\n' 
  			|| content[i] == '(' || content[i] == ')'){
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

void createSharedMem(int numNodes) {
	char c;
	pid_t pid;
	int shmflg; /* shmflg to be passed to shmget() */ 
	// size_t shmSize = 4096; /* size to be passed to shmget() */ 
	void* s;
	int size = 10000 * sizeof(int);
	// size = sizeof(vector<vector <string> >) + (sizeof(string) * nodeVector.size() * sizeof(nodeVector[0]));
	// size = sizeof(vector<vector <string> >);
	for(int i = 0 ; i < numNodes; i++){
		void *addr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
		mmapAddress.push_back(addr);
		// cout << "Mapped at " << addr << endl;
		int* shared = (int*) addr;
		mmapAddr.push_back(shared);
	}
}

int distributeData(vector<string> wordVector, int numNodes){

	/* takes in word vector which has all of the words from the file in one vector and
	partions these words between the nodes (one vector per node) and saves those vectors in a vector of vectors */
	int wordVecLength = wordVector.size();
  	int wordsPerNode = wordVecLength/numNodes;
  	int wordsLeftOver = 0;
  	// vector<vector<string> >nodeVector;

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
 	createSharedMem(numNodes);
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

void sort(){
	for(int j = 0; j < nodeVector.size(); j++){
		for(int i = 0; i < nodeVector[j].size(); i++){
			std::transform(nodeVector[j][i].begin(), nodeVector[j][i].end(), nodeVector[j][i].begin(), ::tolower);
		}
		std::sort(nodeVector[j].begin(), nodeVector[j].end());
		// if(j == 0){
		// 	for(int i = 0; i < nodeVector[j].size(); i++){
		// 		cout << nodeVector[j][i] << endl;
		// 	}
		// }
	}
}

void mapString(int numOfProcess){
	int* shared = mmapAddr[numOfProcess];
	// cout << "in map " << numOfProcess << " " << shared << endl;
	// shared[0] = 15;
	vector <string> wordVec = nodeVector[numOfProcess];
	int numWords = nodeVector[numOfProcess].size();
	// cout << "size of nodevector[numOfProcess] " << numWords << endl;
	for(int i = 0; i < numWords; i++){
		shared[i] = 1;
	}
	// if(numOfProcess == 1){
	// 	for(int i = 0; i < numWords; i++){
	// 		cout << nodeVector[numOfProcess][i] << endl;
	// 	}
	// }
	int j = 0;
	int index = 1;
	int i = 0;
	int count = 0;
	while(j < wordVec.size()){
		if(j == wordVec.size()-1){
			break;
		}
		string word = wordVec[j];
		string word2 = wordVec[index];
		if(word == word2){
			shared[i]++;
			count++;
			index++;
		}
		else{
			int k = i+1;
			while (k != (i+count+1)){
				shared[k] = 0;
				k++;
			}
			i = i + count + 1;
			j = index;
			index = index + 1;
			count = 0;
		}
	}
	// if(numOfProcess == 9){
	// 	cout << "OUT " << numOfProcess << endl;
	// 	for(int ind = 0; ind < numWords; ind++){
	// 		cout << wordVec[ind] << " " << shared[ind] << endl;
	// 	}
	// }
}

void mapInt(int numOfProcess){
	int* shared = mmapAddr[numOfProcess];
	// cout << "in map " << numOfProcess << " " << shared << endl;
	// shared[0] = 15;
	vector<int> intVec;
	vector<string> intStrings = nodeVector[numOfProcess];
	cout << nodeVector[numOfProcess].size() << endl;
	// cout << "size of nodevector[numOfProcess] " << numWords << endl;
	std::sort(intStrings.begin(), intStrings.end());
	int num = 0;
	
	for (int x = 0; x < intStrings.size(); x++) {
		stringstream numstream(intStrings[x]);
		numstream >> num;
		if (std::find(intVec.begin(), intVec.end(), num) != intVec.end()) {
			cout << "found duplicate\n";
			continue;
		}
		intVec.push_back(num);
	}
	
	std::sort(intVec.begin(), intVec.end());
	for (int x = 0; x < intVec.size(); x++) {
		shared[x] = intVec[x];
	}
	exit(0);
}

void Map(int numOfProcess, string app) {	
	cout << "app = " << app << endl;
	if (app=="wordcount") {
		cout << "mapstring\n";
		mapString(numOfProcess);
	}
	if (app=="sort") {
		cout << "mapint\n";
		mapInt(numOfProcess);
	}
}

void shuffle(){
	// vector<int*> mmapAddr;
	// vector<vector<string> >nodeVector;


	for(int i = 0; i < nodeVector[9].size(); i++){
		cout << nodeVector[9][i] << endl;		
	}

	cout << endl;
	int ruleNum = 149;
	vector<vector <int> > toKeep(ruleNum);
	// cout << "tokeep: " << toKeep[0].size() << endl;
	for(int j = 0; j < nodeVector.size(); j++){
		vector<int> toErase;
		int* shared = mmapAddr[j];
		for(int i = 0; i < nodeVector[j].size(); i++){
			if(shared[i] == 0){
				toErase.push_back(i);
			}
			else{
				toKeep[j].push_back(shared[i]);
			}
		}

		for(int i = 0; i < toErase.size(); i++){
			nodeVector[j].erase(nodeVector[j].begin() + toErase[i]);
			for(int k = 0; k < toErase.size(); k++){
				toErase[k]--;
			}		
		}
	}


	for(int j = 0; j < nodeVector.size(); j++){
		for(int i = 0; i < nodeVector[j].size(); i++){
			mymultimap.insert ( std::pair<string,int>(nodeVector[j][i], toKeep[j][i]) );
			// cout << nodeVector[9][i] << " " << toKeep[9][i] << endl;		
		}	
	}
	
	std::multimap<string,int>::iterator it;
	for (it=mymultimap.begin(); it!=mymultimap.end(); ++it)
    	std::cout << (*it).first << " => " << (*it).second << '\n';
}

int forkMaps(int num_maps, string app){
	int i = 0;
	sort();
	int status = 0;
	pid_t wpid;
	for(i = 0; i < num_maps; i++){
		pid_t pid = fork();
		// childPID.push_back(getpid());
		if(pid == 0) // only execute this if child
		{
			// childPID.push_back(getpid());s
		    Map(i, app);
		    // cout << "in child " << i << " "<< mmapAddr[i] << endl;
		    // mmapAddr[i][0] = 15;
			return 0;
		}
	}

	while ((wpid = wait(&status)) > 0); // only the parent waits
	// cout << nodeVector[0].size() << endl;
	// for(int i = 0; i < nodeVector[0].size(); i++){
	// 	cout << nodeVector[0][i] << " " << mmapAddr[0][i] << endl;		
	// }
	shuffle();
	// for(int i = 0; i < nodeVector[0].size(); i++){
	// 	cout << nodeVector[0][i] << endl;
	// }
	// cout << "childPID: " << childPID[2] << endl;
	// cout << getpid() << endl;
	// kill(childPID[0], SIGKILL);
	int size = 10000 * sizeof(int);
	for(int i = 0; i < mmapAddress.size(); i++){
		munmap(mmapAddress[i], size);
	}
	return 0;
}

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
  	forkMaps(num_maps, app);
  	// cout << "after fork: " << getpid() << endl;

  	ifs.close();


  	return 0;
}
