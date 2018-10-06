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


using namespace std;
vector<int> mapSemVec;
vector<int> reduceSemVec;
vector<int> mapPID;
vector<int> reducePID;

struct commonData {
	// int index;
	vector <string> wordVector;
	// vector<std::pair <std::string,int> > wordMap;
	// std::pair <std::string,int> pairs; 
	// map<string, int> mapOfWords;
struct CommonData* next;
};

vector<commonData*> mapShm;

int map(){
	cout << "hellooo" << endl;
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
  	cout << "here5 " << endl;
  	// distributeData(wordVector);
  	return wordVector;
}

void createSharedMem(vector<vector<string> >nodeVector, int numNodes) {
  	cout << "here7 " << endl;

	char c;
	pid_t pid;
	int shmflg; /* shmflg to be passed to shmget() */ 
	size_t shmSize = 1024; /* size to be passed to shmget() */ 
	void* s;
	// size = sizeof(vector<vector <string> >) + (sizeof(string) * nodeVector.size() * sizeof(nodeVector[0]));
	// size = sizeof(vector<vector <string> >);
	for(int i = 0 ; i < numNodes; i++){
		struct commonData* shm; /* returns a pointer */
		int shmid; /* return value from shmget() */ 
		shmid = shmget (IPC_PRIVATE, shmSize, IPC_CREAT | 0666);
		if (shmid == -1) {
			perror("shmget: shmget failed"); 
			exit(1); 
		}
		cout << "shmid: " << shmid << " i: " << i << endl;

		if ((shm = (struct commonData*)shmat(shmid, NULL, 0)) == (struct commonData*)-1) {
	        cout << "here :( " << endl;
	        perror("shmat");
	        exit(1);
	    }
		cout << shm<< endl;
		mapShm.push_back(shm);
	    // shm->index = 0;
	    // shm->wordVector = nodeVector;
	    // cout << shm->index << endl;
	    cout << "parent process has added the wordVector to main memory" << endl;
	    // forkYeah(shm);
	    /* this structure is used by the shmctl() system call. */
		if (shmdt(shm) == -1) 
			perror("shmdt failed");
		struct shmid_ds shm_desc;
		/* destroy the shared memory segment. */
		if (shmctl(shmid, IPC_RMID, &shm_desc) == -1) {
			perror("main: shmctl: ");
		}
	}
	cout << "size of mapshm: " << mapShm.size() << endl;
	for (int i = 0; i < mapShm.size(); i++){
		mapShm[i]->wordVector = nodeVector[i];
	}
	// for(int i = 0; i < mapShm[1]->wordVector.size(); i++){
	// 	cout << mapShm[1]->wordVector[i] << endl;
	// }
}

int distributeData(vector<string> wordVector, int numNodes){
  	cout << "here6 " << endl;

	/* takes in word vector which has all of the words from the file in one vector and
	partions these words between the nodes (one vector per node) and saves those vectors in a vector of vectors */
	int wordVecLength = wordVector.size();
  	int wordsPerNode = wordVecLength/numNodes;
  	int wordsLeftOver = 0;
  	vector<vector<string> >nodeVector;
  	cout << "word vec length: " << wordVecLength << endl;
  	cout << "words per node: " << wordsPerNode << endl;

  	if((wordsPerNode * numNodes) != wordVecLength){
  		wordsLeftOver = wordVecLength - (wordsPerNode * numNodes);
  	}
  	cout << "here6.1 " << endl;
 	for(int j = 0; j < numNodes; j++){
	  	cout << "here6.2 " << endl;
		vector<string> tempVec;
		int limit = wordVector.size() - wordsPerNode;
		cout << limit << endl;
		cout << wordVector.size() << endl;
 		for(int i = wordVector.size()-1; i > limit; i--){
 			cout << "here6.3" << endl; 
			tempVec.push_back(wordVector[i]);
			wordVector.pop_back();
 		}
 		cout << "here6.4" << endl;
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
 	cout << "END OF distributeData" << endl;
 	// forkYeah(numNodes);
 	createSharedMem(nodeVector, numNodes);
 	// nodeVector contains n vectors (one for each node as given from user input)
 	return 0;
}

int split(int num_maps, string content){
  	cout << "here4" << endl;
	struct sembuf sem_op;
	cout << "in split: " << num_maps << endl;
	distributeData(parseInput(content), num_maps);
	// for(int i = 0; i < wordVector.size(); i++) {
	// 	cout << wordVector[i] << endl;
	// }
	sem_op.sem_num = 0;
    sem_op.sem_op = 1;   /* <-- Comment 3 */
    sem_op.sem_flg = 0;
    semop(mapSemVec[0], &sem_op, 1);
	return 0;
}

int forkyeah(int num_maps, int num_reduces){

	struct sembuf sem_op;

	for(int i = 0; i < num_maps; i++){
		int sem_id = semget(IPC_PRIVATE, 1, IPC_CREAT | 0600);
		if (sem_id == -1) {
			// cout << "Herre" << i << endl;
    		perror("main: semget");
    		exit(1);
		}
		int rc = semctl(sem_id, 0, SETVAL, 0);
		if (rc == -1) {
    		perror("main: semctl");
    		exit(1);
		}
		mapSemVec.push_back(sem_id);
	}
	vector<int> semIds;
	for(int i = 0; i < num_reduces; i++){
		int sem_id = semget(IPC_PRIVATE, 1, IPC_CREAT | 0600);
		if (sem_id == -1) {
			// cout << "Herre" << i << endl;
    		perror("main: semget");
    		exit(1);
		}
		int rc = semctl(sem_id, 0, SETVAL, 0);
		if (rc == -1) {
    		perror("main: semctl");
    		exit(1);
		}
		reduceSemVec.push_back(sem_id);
	}
	// for(int i = 0; i<reduceSemVec.size(); i++){
	// 	cout << reduceSemVec[i] << " " << endl;
	// }

	int i = 0;
	int status = 0;
	pid_t wpid;
	for(i = 0; i < num_maps; i++){
		pid_t pid = fork();
		if(pid == 0) // only execute this if child
		{
			cout << "hereee " << getpid() << endl;
			mapPID.push_back(getpid());
		    /* wait on the semaphore, unless it's value is non-negative. */
		    sem_op.sem_num = 0;
		    sem_op.sem_op = -1;
		    sem_op.sem_flg = 0;
		    semop(mapSemVec[i], &sem_op, 1);
		    cout << i << " " << getpid() << endl;
		    map();
		    sem_op.sem_num = 0;
		    sem_op.sem_op = 1;   /* <-- Comment 3 */
		    sem_op.sem_flg = 0;
		    semop(mapSemVec[i], &sem_op, 1);
			return 0;
		}
	}

	for(i = 0; i < num_reduces; i++){
		pid_t pid = fork();
		if(pid == 0) // only execute this if child
		{
			reducePID.push_back(getpid());
		    /* wait on the semaphore, unless it's value is non-negative. */
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
	printf("made it here!!!\n");
	cout << "here in parent " << getpid() << endl;
	for (int count = 0; count < mapSemVec.size(); count++) {
		semctl(mapSemVec[count], 0, IPC_RMID);
	}

	for (int count = 0; count < reduceSemVec.size(); count++) {
		semctl(reduceSemVec[count], 0, IPC_RMID);
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
  	forkyeah(num_maps, num_reduces);
  	if(getpid() == parent_pid){
  		split(num_maps, content);  		
  	}
  	// cout << "hello" << mapPID.size() << endl;
  	ifs.close();
  	return 0;
}
