#include <stdio.h>
#include <string.h>
#include <omnetpp.h>
#include <filesystem>
#include "setup_m.h"

namespace fs = std::__fs::filesystem;
using namespace omnetpp;

class Leader : public cSimpleModule{
protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
	void sendTestSetupMessage(int gateIndex);
	void createWorkersDirectory();
};

Define_Module(Leader);

void Leader::initialize(){
	//Create './Data/' Directory for worker data
	createWorkersDirectory();
}

void Leader::handleMessage(cMessage *msg){
	// TODO
}

void Leader::createWorkersDirectory(){
	fs::path dirPath = "Data";
	if(!fs::exists(dirPath)){
		fs::create_directory(dirPath);
	}
}

void Leader::sendTestSetupMessage(int gateIndex){
	SetupMessage *msg = new SetupMessage();
	msg->setAssigned_id(0);

	int numOfElements = 10;
	msg->setDataArraySize(numOfElements);

	for (int i = 0; i < numOfElements; ++i) {
    	msg->setData(i, i*2); // Fill with some values
	}
	send(msg, "out", gateIndex);
	return;
}
