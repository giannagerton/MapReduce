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

vector<int*> mmapAddr;
vector<void*> mmapAddress;
vector<int> childPID;
vector<vector<string> >nodeVector;
vector<std::pair <std::string,int> > vecOfPairs;
multimap<string,int> mymultimap;
multimap<string,int> mainMap;

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
  	return wordVector;
}

void createSharedMem(int numNodes) {
	char c;
	pid_t pid;
	int shmflg; /* shmflg to be passed to shmget() */ 
	void* s;
	int size = 10000 * sizeof(int);
	for(int i = 0 ; i < numNodes; i++){
		void *addr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
		mmapAddress.push_back(addr);
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

	// shm->index++;
	while ((wpid = wait(&status)) > 0); // only the parent waits
	cout << "here in parent " << endl;
	// shm->wordMap.clear();
	// cout << "shm: " << shm->index << endl;
	//cout << shm->mapOfWords << endl;
	return 0;

}

void shuffle(){
	int ruleNum = 149;
	vector<vector <int> > toKeep(ruleNum);
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
		}	
	}
}

int distributeReduceData(vector<string> wordVector, int numNodes){
	/* takes in word vector which has all of the words from the file in one vector and
	partions these words between the nodes (one vector per node) and saves those vectors in a vector of vectors */
	int wordVecLength = wordVector.size();
  	int wordsPerNode = wordVecLength/numNodes;
  	int wordsLeftOver = 0;
  	if((wordsPerNode * numNodes) != wordVecLength){
  		wordsLeftOver = wordVecLength - (wordsPerNode * numNodes);
  	}
 	for(int j = 0; j < numNodes; j++){
		vector<string> tempVec;
		int limit = wordVector.size() - wordsPerNode;
 		for(int i = wordVector.size()-1; i > limit; i--){
			tempVec.push_back(wordVector[i]);
			wordVector.pop_back();
			if((i-1) == limit){
				if(wordVector[i-1] == tempVec[tempVec.size()-1]){
					if(limit > 0){
						limit--;
					}
					else{
						break;
					}
				}
			}
 		}
 		nodeVector.push_back(tempVec);
 	}
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

void parseReduce(int num_reduces){
	vector<string> wordVector;
	vector<int> countVector;
	std::multimap<string,int>::iterator it;
	int wordsPerNode = mymultimap.size()/num_reduces;
	for (it=mymultimap.begin(); it!=mymultimap.end(); ++it){
		wordVector.push_back((*it).first);
		countVector.push_back((*it).second);
	}

	mmapAddr.clear();
 	mmapAddress.clear();
	nodeVector.clear();

	distributeReduceData(wordVector, num_reduces);

	for(int i = 0; i < nodeVector.size(); i++){
		std::sort(nodeVector[i].begin(), nodeVector[i].end());
  		nodeVector[i].erase(std::unique(nodeVector[i].begin(), nodeVector[i].end()), nodeVector[i].end());
	}
}

int forkMaps(int num_maps){
	int i = 0;
	sort();
	int status = 0;
	pid_t wpid;
	for(i = 0; i < num_maps; i++){
		pid_t pid = fork();
		if(pid == 0) // only execute this if child
		{
		    Map(i);
			return 0;
		}
	}

	while ((wpid = wait(&status)) > 0); // only the parent waits
	shuffle();
	// kill(childPID[0], SIGKILL);
	int size = 10000 * sizeof(int);
	for(int i = 0; i < mmapAddress.size(); i++){
		munmap(mmapAddress[i], size);
	}
	return 0;
}

void reduce(int numOfProcess){
	int* shared = mmapAddr[numOfProcess];
	vector <string> wordVec = nodeVector[numOfProcess];

	int count = 0;
	for(int i = 0; i < wordVec.size(); i++){
		std::pair <std::multimap<string,int>::iterator, std::multimap<string,int>::iterator> ret;
		ret = mymultimap.equal_range(wordVec[i]);
		for (std::multimap<string,int>::iterator it=ret.first; it!=ret.second; ++it)
			count += it->second;
		// mymultimap.erase(wordVec[i]);
		shared[i] = count;
		count = 0;
	}
}

void combine(){
	for(int j = 0; j < nodeVector.size(); j++){
		for(int i = 0; i < nodeVector[j].size(); i++){
			mainMap.insert ( std::pair<string,int>(nodeVector[j][i], mmapAddr[j][i]) );
		}	
	}

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
		    reduce(i);
			return 0;
		}
	}

	while ((wpid = wait(&status)) > 0); // only the parent waits
	combine();
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
	ifstream ifs(infile.c_str());

	if(impl == "procs"){
	  	string content( (istreambuf_iterator<char>(ifs) ),
	                       (istreambuf_iterator<char>()    ) );
	  	if(app == "wordcount"){
	  		split(num_maps, content);
	  		forkMaps(num_maps);
	  		if(getpid() == parent_pid){
		  		parseReduce(num_reduces);
		  		forkReduces(num_reduces);  		
	  		}
	  	}
  	}
  	
  	ifs.close();
  	char output[outfile.length() + 1];
  	strcpy(output, outfile.c_str());  

 	ofstream myfile;
 	myfile.open(output);
	for(map<string, int>::iterator x = mainMap.begin(); x != mainMap.end(); ++x){
		myfile << x->first << '\t' << x->second << endl;
	}
 	myfile.close();


  	return 0;
}