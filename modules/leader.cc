#include <stdio.h>
#include <string.h>
#include <omnetpp.h>
#include <filesystem>

#include "setup_m.h"
#include "schedule_m.h"
namespace fs = std::filesystem;
using namespace omnetpp;

class Leader : public cSimpleModule{
public:
	~Leader() {
        removeWorkersDirectory();
    }
protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
	void sendTestSetupMessage(int gateIndex);
	void createWorkersDirectory();
	void sendSchedule();
private:
	void removeWorkersDirectory() {
        fs::path dirPath = "Data";
        // Check if the directory exists before trying to remove it
        if (fs::exists(dirPath) && fs::is_directory(dirPath)) {
            // Remove the directory and its contents recursively
            fs::remove_all(dirPath);
        }
    }
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
    int scheduleSize = 5;
	int datasize = 30;
	int numWorkers = par("numWorkers").intValue();
    std::vector<std::string> schedule = {"add", "mul", "ge", "lt", "reduce"};
    std::vector<int>parameters = {5, 2, 10, 30, 0};

    std::vector<int>data;

    for(int i=0; i<datasize; i++){
        data.push_back(i);
    }
	for(int i=0; i<numWorkers; i++){
		ScheduleMessage *scheduleMsg = new ScheduleMessage("scheduleMessage");
    	SetupMessage *setupMsg = new SetupMessage("setupMessage");
		setupMsg->setDataArraySize(data.size());
		setupMsg->setAssigned_id(i);
		for(int j=0; j<data.size(); j++){
        	setupMsg->setData(j, data[j]);
    	}
		send(setupMsg, "out", i);
		scheduleMsg->setScheduleArraySize(scheduleSize);
    	scheduleMsg->setParametersArraySize(scheduleSize);

		for(int k=0; k<scheduleSize; k++){
			scheduleMsg->setSchedule(k, schedule[k].c_str());
			scheduleMsg->setParameters(k, parameters[k]);
		}
	
		send(scheduleMsg, "out", i);
	}

}
