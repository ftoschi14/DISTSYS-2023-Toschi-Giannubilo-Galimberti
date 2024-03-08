#include <stdio.h>
#include <string.h>
#include <omnetpp.h>
#include <filesystem>

#include "setup_m.h"
#include "schedule_m.h"
namespace fs = std::filesystem;
using namespace omnetpp;

class Leader : public cSimpleModule{
protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
	void sendTestSetupMessage(int gateIndex);
	void createWorkersDirectory();
	void sendSchedule();
};

Define_Module(Leader);

void Leader::initialize(){
	//Create './Data/Worker_i' Directory for worker data
	createWorkersDirectory();
    sendSchedule();
}

void Leader::handleMessage(cMessage *msg){
	// TODO
}

void Leader::createWorkersDirectory(){
	fs::path dirPath = "Data";
	if(!fs::exists(dirPath)){
		fs::create_directory(dirPath);
	}

	int numWorkers = par("numWorkers").intValue();
	fs::path subPath;
	for(int i = 0; i < numWorkers; i++){
		subPath = "Worker_" + std::to_string(i);
		fs::create_directory(dirPath/subPath);
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

void Leader::sendSchedule(){
    ScheduleMessage *scheduleMsg = new ScheduleMessage("scheduleMessage");
    SetupMessage *setupMsg = new SetupMessage("setupMessage");

    int scheduleSize = 5;
    std::vector<std::string> schedule = {"add", "mul", "ge", "sub", "reduce"};
    std::vector<int>parameters = {5, 2, 16, 10, 0};

    std::vector<int>data;

    for(int i=0; i<10; i++){
        data.push_back(i);
    }
    setupMsg->setDataArraySize(data.size());

    for(int i=0; i<data.size(); i++){
        setupMsg->setData(i, data[i]);
    }

    send(setupMsg, "out", 0);
    EV << "Setup message sent\n";
    scheduleMsg->setScheduleArraySize(scheduleSize);
    scheduleMsg->setParametersArraySize(scheduleSize);

    for(int i=0; i<scheduleSize; i++){
        scheduleMsg->setSchedule(i, schedule[i].c_str());
        scheduleMsg->setParameters(i, parameters[i]);
    }
    send(scheduleMsg, "out", 0);
    EV << "Schedule message sent\n";
}
