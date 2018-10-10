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

// struct commonData {
// 	// int index;
// 	vector <string> wordVector;
// 	vector<std::pair <std::string,int> > wordMap;
// 	vector<std::pair <std::string,int> > newMap;
// 	vector <int> value;
// 	// std::pair <std::string,int> pairs; 
// 	// map<string, int> mapOfWords;
// struct CommonData* next;
// };

vector<std::pair <std::string,int> > vecOfPairs;
multimap<string,int> mymultimap;
// vector<commonData*> mapShm;
vector<int> mapShmid;

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
  	if((wordsPerNode * numNodes) != wordVecLength){
  		cout << "in here!!" << endl;
  		wordsLeftOver = wordVecLength - (wordsPerNode * numNodes);
  		cout << wordsLeftOver << endl;
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
 	int sizee = nodeVector[0].size() + nodeVector[1].size() + nodeVector[2].size() + nodeVector[3].size() + nodeVector[4].size();
 	int index = 0;
 	while(wordVector.size() != 0){
 		nodeVector[index].push_back(wordVector[wordVector.size()-1]);
 		wordVector.pop_back();
 		index++;
 		if(index == numNodes)
 			index = 0;
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
	}
}

void Map(int numOfProcess){
	int* shared = mmapAddr[numOfProcess];
	vector <string> wordVec = nodeVector[numOfProcess];
	int numWords = nodeVector[numOfProcess].size();
	// cout << "size of nodevector[numOfProcess] " << numWords << endl;
	for(int i = 0; i < numWords; i++){
		shared[i] = 1;
	}
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
	
	// std::multimap<string,int>::iterator it;
	// for (it=mymultimap.begin(); it!=mymultimap.end(); ++it)
 //    	std::cout << (*it).first << " => " << (*it).second << '\n';
}

void parseReduce(int num_reduces){
	vector<string> wordVector;
	vector<int> countVector;
	std::multimap<string,int>::iterator it;
	cout << "multimap size: " << mymultimap.size() << endl;
	int wordsPerNode = mymultimap.size()/num_reduces;
	for (it=mymultimap.begin(); it!=mymultimap.end(); ++it){
		wordVector.push_back((*it).first);
		countVector.push_back((*it).second);
    	// std::cout << (*it).first << " => " << (*it).second << '\n';
	}

	mmapAddr.clear();
 	mmapAddress.clear();
	nodeVector.clear();

	distributeData(wordVector, num_reduces);

	cout << "done with parse reduce " << endl;


	// for(int j = 0; j < num_reduces; j++){
	// 	int size = nodeVector[j].size();
	// 	int* shared = mmapAddr[j];
	// 	for(int i = 0; i < size; i++){
	// 		shared[i] = countVector[i];
	// 	}
	// }

	// int* shared = mmapAddr[0];
	// for(int i = 0; i < 204; i++){
	// 	cout << 
	// }
}

int forkMaps(int num_maps){
	int i = 0;
	sort();
	int status = 0;
	pid_t wpid;
	for(i = 0; i < num_maps; i++){
		pid_t pid = fork();
		// childPID.push_back(getpid());
		if(pid == 0) // only execute this if child
		{
			// childPID.push_back(getpid());
		    Map(i);
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

void reduce(int numOfProcess){
	vector <string> wordVec = nodeVector[numOfProcess];
	cout << "IN REDUCE  " << endl;

}

int forkReduces(int num_reduces){
	int i = 0;
	int status = 0;
	pid_t wpid;
	for(i = 0; i < num_reduces; i++){
		pid_t pid = fork();
		// childPID.push_back(getpid());
		if(pid == 0) // only execute this if child
		{
			// childPID.push_back(getpid());
		    reduce(i);
		    // cout << "in child " << i << " "<< mmapAddr[i] << endl;
		    // mmapAddr[i][0] = 15;
			return 0;
		}
	}

	while ((wpid = wait(&status)) > 0); // only the parent waits
	shuffle();
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
  	forkMaps(num_maps);
  	parseReduce(num_reduces);
  	forkReduces(num_reduces);
  	// cout << "after fork: " << getpid() << endl;

  	ifs.close();


  	return 0;
}