#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <omnetpp.h>

#include "setup_m.h"
#include "BatchLoader.h"

using namespace omnetpp;

class Worker : public cSimpleModule{
private:
	std::vector<int> data;
	std::string fileName;
	std::string fileProgressName;
	int batchSize;
	int workerId;
	BatchLoader* loader;

protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
	void handleSetupMessage(SetupMessage *msg);
};

Define_Module(Worker);

void Worker::initialize(){
	// TODO
	batchSize = par("batchSize").intValue();
	data.resize(batchSize);
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
	workerId = msg->getAssigned_id();

	if(par("id").intValue() == -1){ // If the ID was not previously set
		par("id") = workerId;
	}

	int dataSize = msg->getDataArraySize();

	//Persisting data on file
	std::string folder = "Data/Worker_" + std::to_string(workerId) + "/";
	std::ofstream data_file;

	fileName = folder + "data.csv";
	data_file.open(fileName);

	for (int i = 0; i < dataSize; i++) {
	    // Directly write to the file without using std::format
	    data_file << workerId << ',' << msg->getData(i) << '\n';
	}

	data_file.close();

	// Instantiate a BatchLoader
	fileProgressName = folder + "progress.txt";
	loader = new BatchLoader(fileName, fileProgressName, batchSize);

	// Load first batch
	data = loader->loadBatch();
};