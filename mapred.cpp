// run with g++ -o mapred mapred.cpp -> ./mapred (# of nodes)

#include <fstream>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <semaphore.h>
#include <vector>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <map>
#include <mutex>
#include <iterator>

using namespace std;
int numNodes;

struct commonData {
	// int index;
	vector<vector<string> >wordVector;
	vector<std::pair <std::string,int> > wordMap;
	// std::pair <std::string,int> pairs; 
	// map<string, int> mapOfWords;
struct CommonData* next;
};

int foo(string whoami){
	cout << "I am a " << whoami << ". My pid is: " << getpid() << " my ppid is " << getppid() << endl;
	return 1;
}

int mapWords(struct commonData* shm, int index){
	vector<string> wordVec = shm->wordVector[index];
	cout << "in map " << index << endl;
	cout << wordVec.size() << endl;
	cout << "HERE " << endl;
	// pairs.first = "shoes";                  // the type of first is string
 //  	pairs.second = 39;  
 //  	cout << "The price of " << pairs.first << " is $" << pairs.second << '\n';
 //  	shm->wordMap.push_back(pairs);
 //  	cout << shm->wordMap[0].second << endl;
	std::pair <std::string,int> pairs;
  	for(int i = 0; i < wordVec.size(); i++){
		cout << "HERE " << i << endl;
  		pairs.first = wordVec[i];
  		pairs.second = 1;
  		shm->wordMap.push_back(pairs);
  	}
  	cout << "wordmapsize : " << shm->wordMap.size() << endl;
  	for(int i = 0; i < shm->wordMap.size(); i++){
  		cout << shm->wordMap[i].first << " " << shm->wordMap[i].second << endl;
  	}
	// map< string, int > mp;
 //    map<string, int>::iterator itr; 
 //    map<string, int>::iterator it1; 
 //    map<string, int>::iterator it2;
 //    pair <map<string, int>::iterator, bool> ptr;
 //    string word = wordVec[1];
 //    cout << word << endl;

 //    ptr = mp.insert( pair<string, int>(word, 20) );
 //    for (itr = mp.begin(); itr != mp.end(); ++itr) 
 //    { 
 //        cout << itr->first << " "<< itr->second << endl; 
 //    } 
	
	return 0;
}

int forkYeah(struct commonData* shm){
	struct sembuf sem_op;
	/* ID of the semaphore set.     */
	int sem_set_id;

	/* create a private semaphore set with one semaphore in it, */
	/* with access only to the owner.                           */
	sem_set_id = semget(IPC_PRIVATE, 1, IPC_CREAT | 0600);
	if (sem_set_id == -1) {
	    perror("main: semget");
	    exit(1);
	}

	/* use this to store return values of system calls.   */
	int rc;

	/* use this variable to pass the value to the semctl() call */
	union semun sem_val;

	/* initialize the first semaphore in our set to '3'. */
	sem_val.val = 1;
	rc = semctl(sem_set_id, 0, SETVAL, sem_val);
	if (rc == -1) {
	    perror("main: semctl");
	    exit(1);
	}

	int i = 0;
	int status = 0;
	pid_t wpid;
	cout << "creating " << numNodes << " children" << endl;
	foo("parent");
	for(i = 0; i < numNodes; i++){
		pid_t pid = fork();
		if(pid == 0) // only execute this if child
		{
		    /* wait on the semaphore, unless it's value is non-negative. */
		    sem_op.sem_num = 0;
		    sem_op.sem_op = -1;
		    sem_op.sem_flg = 0;
		    semop(sem_set_id, &sem_op, 1);
			cout << "here in child! " << i << endl;
			if(i == 0 || i == 1){
				mapWords(shm, i);				
			}
			// shm->index++;
		    /* finally, signal the semaphore - increase its value by one. */
		    sem_op.sem_num = 0;
		    sem_op.sem_op = 1;   /* <-- Comment 3 */
		    sem_op.sem_flg = 0;
		    semop(sem_set_id, &sem_op, 1);
			return 0;
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

void createSharedMem(vector<vector<string> >nodeVector){
	char c;
	pid_t pid;
	int shmflg; /* shmflg to be passed to shmget() */ 
	int shmid; /* return value from shmget() */ 
	size_t shmSize = 4096; /* size to be passed to shmget() */ 
	struct commonData* shm; /* returns a pointer */
	void* s;
	// size = sizeof(vector<vector <string> >) + (sizeof(string) * nodeVector.size() * sizeof(nodeVector[0]));
	// size = sizeof(vector<vector <string> >);
	shmid = shmget (IPC_PRIVATE, shmSize, IPC_CREAT | 0666);
	// if ((shmid = shmget (IPC_PRIVATE, size, IPC_CREAT | 0666)) == -1) {
	// 	perror("shmget: shmget failed"); 
	// 	exit(1); 
	// } 
	cout << shmid << endl;

	if ((shm = (struct commonData*)shmat(shmid, NULL, 0)) == (struct commonData*)-1) {
        perror("shmat");
        exit(1);
    }
    // shm->index = 0;
    shm->wordVector = nodeVector;
    // cout << shm->index << endl;
    cout << "parent process has added the wordVector to main memory" << endl;
    forkYeah(shm);
    // pid = fork();
    // if(pid == 0){
    // 	cout << "HIII IN CHILD " << endl;
    // 	forkYeah(shm);
    // }
    // shm->next = new struct* commonData();
    // vector<int>* vpInA = new(shm) vector<int>*;
 //    cout << &shm << endl;
	// cout << "Write Data: ";
	// cin >> shm;
	// (*vpInA)[0] = 22;

	// printf("data written in memory: %s\n", shm[0][0]);
	// *s = 'a';
	// cout << *s << endl;

	// for (c = 'a'; c <= 'z'; c++)
 //        *s++ = c;
 //    *s = '\0';
	// shmctl(shmid, IPC_RMID, NULL);
}

int distributeData(vector<string> wordVector){
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
 		for(int i = wordVector.size(); i > limit; i--){
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
 	createSharedMem(nodeVector);
 	// nodeVector contains n vectors (one for each node as given from user input)
 	return 0;
}
int parseInput(string content){
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
  	distributeData(wordVector);
  	return 0;
}

int main(int argc, char* argv[]){
	numNodes = atoi(argv[1]);
	ifstream ifs("word_input.txt");
  	string content( (istreambuf_iterator<char>(ifs) ),
                       (istreambuf_iterator<char>()    ) );
  	parseInput(content);
  	ifs.close();
  	return 0;
}