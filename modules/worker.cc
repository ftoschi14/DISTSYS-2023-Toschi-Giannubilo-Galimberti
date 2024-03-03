#include <string>
#include <iostream>
#include <fstream>
#include <omnetpp.h>
#include <vector>

#include "setup_m.h"

using namespace omnetpp;

class Worker : public cSimpleModule{
private:
	std::vector<int> data;
protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
	void handleSetupMessage(SetupMessage *msg);
};

Define_Module(Worker);

void Worker::initialize(){
	// TODO
}

void Worker::handleMessage(cMessage *msg){
	// Setup Message chunk
	SetupMessage *setupMsg = dynamic_cast<SetupMessage *>(msg);
    if (setupMsg != nullptr) {
        // Successfully cast to SetupMessage, handle it
        handleSetupMessage(setupMsg);
        return;
    }

    // TODO Other messages...
}

void Worker::handleSetupMessage(SetupMessage *msg){
	int dataSize = msg->getDataArraySize();
	int worker_id = msg->getAssigned_id();

	if(par("id").intValue() == -1){ // If the ID was not previously set
		par("id") = worker_id;
	}
	data.resize(dataSize);

	//Persisting data on file
	std::string folder = "Data/";

	std::ofstream data_file;
	std::string fileName = folder + "worker_" + std::to_string(worker_id) + "_data.csv";
	data_file.open(fileName);

	for (int i = 0; i < dataSize; i++) {
	    // Directly write to the file without using std::format
	    data_file << worker_id << ',' << msg->getData(i) << '\n';
	}

	data_file.close();
}