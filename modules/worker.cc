#include <stdio.h>
#include <string.h>
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
	if(par("id").intValue() == -1){ // If the ID was not previously set
		par("id") = msg->getAssigned_id();
	}
	int dataSize = msg->getDataArraySize();
	data.resize(dataSize); // ASSUMPTION: The first message contains all of the data

	for (int i = 0; i < dataSize; i++) {
    	data[i] = msg->getData(i); // Copy each data point
	}
}