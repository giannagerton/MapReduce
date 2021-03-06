﻿/* mapred –-app [wordcount, sort] –-impl [procs, threads]
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
#include <pthread.h>
#include <map>

//Make sure to add delimiter to                   account for Em Dash
void *mapFunction(void *args);

using namespace std;

pthread_mutex_t lock; 
vector< vector<string> > nodeVector;
map<string,int> mainMap;
vector< map<string, int> > miniMaps;

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
  			if(isupper(content[i])) content[i]=tolower(content[i]);
  			buffer += content[i];
  			i++;
  			if(i == content.length()){
  				wordVector.push_back(buffer);
  			}
  		}
  	}
   	return wordVector;
}


vector<vector<string> > split(vector<string> wordVector, int numNodes){
  	
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

 	return nodeVector;
}

void *mapFunction(void *args){
	vector<string> v = nodeVector.at(*((int *)args));
	std::map<string, int> tempMap;
	//feed the words into the map
	
	for (vector<string>::iterator i = v.begin(); i != v.end(); ++i)
	{
			map<string, int>::iterator mapIterator = tempMap.find(*i);
			if(mapIterator == tempMap.end()){
				tempMap.insert(pair <string,int> (*i, 1));
			}
			else{
				mapIterator->second++;
			}
	}
	//lock
	pthread_mutex_lock(&lock);
	//add map into the miniMaps global vector
	miniMaps.push_back(tempMap);
	//unlock
    pthread_mutex_unlock(&lock); 

}

void shuffle(){
	//vector iterator 
	for(vector< map<string, int> >::iterator mM = miniMaps.begin(); mM != miniMaps.end(); ++mM){
		//map iterator
		for(map<string, int>::iterator m = (*mM).begin(); m != (*mM).end(); ++m){
			map<string, int>::iterator mapIterator = mainMap.find(m->first);
			if(mapIterator == mainMap.end()){
				mainMap.insert(*m);
			}
			else{
				mapIterator->second += m->second;
			}

		}
	}

}

/*
void reduceFunction(*void args){

}

	Saving for reduce 

	
	for(i = 0; i < num_reduces; i++){
		pid_t pid = fork();
		if(pid == 0) // only execute this if child
		{
			reducePID.push_back(getpid());
		    // wait on the semaphore, unless it's value is non-negative. 
		    sem_op.sem_num = 0;
		    sem_op.sem_op = -1;
		    sem_op.sem_flg = 0;
		    semop(reduceSemVec[i], &sem_op, 1);
		    cout << i << " " << getpid() << endl;
			return 0;
		}
	}
	// shm->index++;
	// while ((wpid = wait(&status)) > 0); // only the parent waits
	cout << "here in parent " << getpid() << endl;
	for (int count = 0; count < mapSemVec.size(); count++) {
		semctl(mapSemVec[count], 0, IPC_RMID);
	}

	for (int count = 0; count < reduceSemVec.size(); count++) {
		semctl(reduceSemVec[count], 0, IPC_RMID);
	}
	return 0;
}
*/

int main(int argc, char* argv[]){
	string app, impl, infile, outfile;
	int num_maps, num_reduces;
	int parent_pid = getpid();

	for(int i = 1; i < argc; i++){
		if(strcmp(argv[i], "--app") == 0){
			app = argv[++i];
		}
		else if(strcmp(argv[i], "--impl") == 0){
			impl = argv[++i];
		}
		else if(strcmp(argv[i], "--maps") == 0){
			num_maps = atoi(argv[++i]);
		}
		else if(strcmp(argv[i], "--reduces") == 0){
			num_reduces = atoi(argv[++i]);
		}
		else if(strcmp(argv[i], "--input") == 0){
			infile = argv[++i];
		}
		else if(strcmp(argv[i], "--output") == 0){
			outfile = argv[++i];
		}
	}
	

	/*

		Include logic here which changes the path of execution based on program arguments...

	*/


	ifstream ifs(infile.c_str());
  	string content( (istreambuf_iterator<char>(ifs) ),
                       (istreambuf_iterator<char>()    ) );
 	
  	pthread_t* mapthread_handles = new pthread_t[num_maps];
  	pthread_t* reducethread_handles = new pthread_t[num_reduces];

	nodeVector = split(parseInput(content), num_maps);

  	//now nodevector is complete

	for (int i = 0; i < num_maps; i++){
		int *arg = (int *) malloc(sizeof(int));
		*arg = i;
		pthread_create(&mapthread_handles[i], NULL, mapFunction, (void *) arg);
	}	


	//shuffle here
	shuffle();	

	//reduce here.



	ofstream myfile;
  	myfile.open ("output1.txt");

  	for(map<string, int>::iterator x = mainMap.begin(); x != mainMap.end(); ++x){

  			myfile << x->first  // string (key)
              << ':' 
              << x->second // string's value 
              << std::endl ;
  	
  		
  	} 
  	
  	myfile.close();
  	
  	ifs.close();
  	return 0;
}
