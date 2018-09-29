// run with g++ -o mapred mapred.cpp -> ./mapred (# of nodes)

#include <fstream>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <fstream>
#include <semaphore.h>
#include <vector>

using namespace std;

int parseInput(int numNodes, string content){
	// This method takes in two inputs: number of Nodes and string with all text
	// First extract words from string using delimiters and add each word to the wordVector
	// Then using the number of nodes divide the words up evenly into n vectors and 
	// add those vectors to a vector of vectors
	// return vector of vectors
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

  	// for(int j = 0; j < wordVector.size(); j++){
  	// 	cout << wordVector[j] << endl;
  	// }

  	cout << "number of nodes: " << numNodes << endl;
  	cout << "number of words: " << wordVector.size() << endl;
  	cout << "words per node: " << wordVector.size()/numNodes << endl;
  	int wordsPerNode = wordVector.size()/numNodes;
  	int wordVecLength = wordVector.size();
 	vector<vector<string> >nodeVector;
 	for(int j = 0; j < numNodes; j++){
		vector<string> tempVec;
		int limit = wordVector.size() - wordsPerNode;
 		for(int i = wordVector.size(); i > limit; i--){
 			if(i == wordVecLength){
 			}
 			else{
 				tempVec.push_back(wordVector[i]);
 				wordVector.pop_back();
 			}
 		}
 		nodeVector.push_back(tempVec);
 		cout << tempVec.size() << endl;
 	}
 	cout << nodeVector.size() << endl;

 	// nodeVector contains n vectors (one for each node as given from user input)
  	return 0;
}

int main(int argc, char* argv[]){
	ifstream ifs("word_input.txt");
  	string content( (istreambuf_iterator<char>(ifs) ),
                       (istreambuf_iterator<char>()    ) );
  	parseInput(atoi(argv[1]), content);
  	return 0;
}