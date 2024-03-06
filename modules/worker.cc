#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <omnetpp.h>
#include <algorithm>

#include "setup_m.h"
#include "datainsert_m.h"
#include "schedule_m.h"
#include "BatchLoader.h"



using namespace omnetpp;

class Worker : public cSimpleModule{
private:
	std::vector<int> data;
	std::map<int, DataInsertMessage*> unstableMessages;
	std::map<int, cMessage*> timeouts;
	std::string fileName;
	std::string fileProgressName;
	int batchSize;
	int workerId;
	float timeout;
	BatchLoader* loader;

protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
	void handleSetupMessage(SetupMessage *msg);
	void handleDataInsertMessage(DataInsertMessage *msg);
	void handleScheduleMessage(ScheduleMessage *msg);
	void applySchedule(std::vector<std::string> schedule, std::vector<int> parameters);
	void printingVector(std::vector<int> vector);
	int changeKey(int data);
	int reduce(int data[]);
	void sendData(int newKey, int value);
	int getWorkerGate(int destID);
};

Define_Module(Worker);

void Worker::initialize(){
	// TODO
	batchSize = par("batchSize").intValue();
	data.resize(batchSize);
	timeout=5;
}

void Worker::handleMessage(cMessage *msg){
	/* Timeout Message chunk
	*  Get the context pointer, if not null it should correspond to a request ID
	*  Get the corresponding timeout and insertMsg, re-send it and re-schedule a timeout.
	*/
	int *pReqID = static_cast<int *>(msg->getContextPointer());
	if(pReqID != nullptr){
		int reqID = *pReqID;
		delete pReqID;
		// Check if the corresponding timeout exists
		auto timeoutMsg = timeouts.find(reqID);
		if(timeoutMsg != timeouts.end()){
			delete timeoutMsg->second; // Safe because it's not scheduled anymore.

			// Check if the corresponding insertMessage exists
			auto insertMsg = unstableMessages.find(reqID);
			if(insertMsg != unstableMessages.end()){

				// Get the worker ID from the message
				int destWorker = insertMsg->second->getDestWorker();
				DataInsertMessage* insertMsgCopy = insertMsg->second->dup();
				send(insertMsgCopy, "out", getWorkerGate(destWorker));

				// Reschedule the timeout for this message
                cMessage* newTimeoutMsg = new cMessage(("Timeout-" + std::to_string(reqID)).c_str());
                newTimeoutMsg->setContextPointer(new int(reqID));
                scheduleAt(simTime() + timeout, newTimeoutMsg);

                // Update maps with the new timeout message and remove the old one
                timeouts[reqID] = newTimeoutMsg;
                return;
			}
			return;
		}
	}

	// Setup Message chunk
	SetupMessage *setupMsg = dynamic_cast<SetupMessage *>(msg);
    if(setupMsg != nullptr){
        // Successfully cast to SetupMessage, handle it
        handleSetupMessage(setupMsg);
        return;
    }

    // DataInsertMessage (Could be either to insert here, or an ACK)
    DataInsertMessage *dataInsertMsg = dynamic_cast<DataInsertMessage *>(msg);
    if(dataInsertMsg != nullptr){
    	//Successfully cast to DataInsertMessage, handle it
    	handleDataInsertMessage(dataInsertMsg);
    	return;
    }
    ScheduleMessage *scheduleMsg = dynamic_cast<ScheduleMessage *>(msg);
        if (scheduleMsg != nullptr) {
            // Successfully cast to ScheduleMessage, handle it
            EV << "received schedule message\n";
            handleScheduleMessage(scheduleMsg);
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
	    // Directly write to the file
	    data_file << workerId << ',' << msg->getData(i) << '\n';
	}

	data_file.close();

	// Instantiate a BatchLoader
	fileProgressName = folder + "progress.txt";
	loader = new BatchLoader(fileName, fileProgressName, batchSize);

	// Load first batch
	//data = loader->loadBatch();
}

void Worker::handleScheduleMessage(ScheduleMessage *msg){
    int scheduleSize = msg->getScheduleArraySize();
    std::vector<std::string> schedule;
    std::vector<int> parameters;

    for(int i=0; i<scheduleSize; i++){
        schedule.push_back(msg->getSchedule(i)) ;
    }
    for(int i=0; i<scheduleSize; i++){
        parameters.push_back(msg->getParameters(i));
    }
    EV << "Applying schedule\n";
    data = loader->loadBatch();
    applySchedule(schedule, parameters);

}

void Worker::applySchedule(std::vector<std::string> schedule, std::vector<int> parameters) {
    int scheduleSize = schedule.size();
    std::vector<int> discard;
    for (int i = 0; i < scheduleSize; i++) {
        EV << "Applying "<<schedule[i]<<" with parameter "<<parameters[i]<<"\n";
        for (int j = 0; j < data.size(); j++) {
            if (schedule[i] == "add") {
                EV << "Initial data: "<<data[j]<<"\n";
                data[j] += parameters[i];
                EV << "Data after addition: "<<data[j]<<"\n";
            } else if (schedule[i] == "sub") {
                EV<<"Initial data: "<<data[j]<<"\n";
                data[j] -= parameters[i];
                EV<<"Data after subtraction: "<<data[j]<<"\n";
            } else if (schedule[i] == "mult") {
                EV<<"Initial data: "<<data[j]<<"\n";
                data[j] *= parameters[i];
                EV<<"Data after multiplication: "<<data[j]<<"\n";
            } else if (schedule[i] == "div") {
                EV<<"Initial data: "<<data[j]<<"\n";
                data[j] /= parameters[i];
                EV<<"Data after division: "<<data[j]<<"\n";
            } else if (schedule[i] == "lt") {
                if (j == 0) {
                    discard.assign(data.size(), 0);
                }
                if (data[j] >= parameters[i]) {
                    discard[j] = 1;
                }
                if (i == scheduleSize - 1) {
                    EV<<"Initial vector: ";
                    printingVector(discard);
                    data.erase(std::remove_if(data.begin(), data.end(), [&](int i) { return discard[i]; }), data.end());
                    EV<<"Data after removal: ";
                    printingVector(data);
                }
            } else if (schedule[i] == "gt") {
                if (j == 0) {
                    discard.assign(data.size(), 0);
                }
                if (data[j] <= parameters[i]) {
                    discard[j] = 1;
                }
                if (j == data.size() - 1) {
                    EV<<"Initial vector: ";
                    printingVector(discard);
                    data.erase(std::remove_if(data.begin(), data.end(), [&](int i) { return discard[i]; }), data.end());
                    EV<<"Data after removal: ";
                    printingVector(data);
                }
            } else if (schedule[i] == "le") {
                if (j == 0) {
                    discard.assign(data.size(), 0);
                }
                if (data[j] > parameters[i]) {
                    discard[j] = 1;
                }
                if (j == data.size() - 1) {
                    EV<<"Initial vector: ";
                    printingVector(discard);
                    data.erase(std::remove_if(data.begin(), data.end(), [&](int i) { return discard[i]; }), data.end());
                    EV<<"Data after removal: ";
                    printingVector(data);
                }
            } else if (schedule[i] == "ge") {
                if (j == 0) {
                    discard.assign(data.size(), 0);
                }
                if (data[j] < parameters[i]) {
                    discard[j] = 1;
                }
                if (j == data.size() - 1) {
                    EV<<"Initial vector: ";
                    printingVector(discard);
                    data.erase(std::remove_if(data.begin(), data.end(), [&](int i) { return discard[i]; }), data.end());
                    EV<<"Data after removal: ";
                    printingVector(data);
                }
            } else if (schedule[i] == "reduce") {
                // TODO: reduce function
            } else if (schedule[i] == "changekey") {
                // TODO: changekey function
            } else {
                std::cerr << "Invalid operation: " << schedule[i] << std::endl;
            }
        }
    }
}

void Worker::printingVector(std::vector<int> vector){
    for(int i=0; i<vector.size(); i++){
        EV<<vector[i]<<" ";
    }
    EV<<"\n";
}

void Worker::handleDataInsertMessage(DataInsertMessage *msg){
	// Check if it is an ACK or an insertion to me (workerID)
	if(msg->getAck()){
		// Look for corresponding reqID and cancel timeout
		int reqID = msg->getReqID();

		auto timeoutMsg = timeouts.find(reqID);
		if(timeoutMsg != timeouts.end()){
			EV << "Received ACK, cancelling timeout";
			cancelAndDelete(timeoutMsg->second);
			timeouts.erase(reqID);
		}

		auto insertMsg = unstableMessages.find(reqID);
		if(insertMsg != unstableMessages.end()){
			EV << "Received ACK, deleting unstable message";
			unstableMessages.erase(reqID);
		}
		return;
	} else {
		// Insert new data point into data vector
		DataInsertMessage* insertMsg = new DataInsertMessage();
		insertMsg->setReqID(msg->getReqID());
		insertMsg->setAck(true);

		send(insertMsg, "out", msg->getArrivalGate()->getIndex());
		delete msg;
	}
}

int Worker::changeKey(int data){
	// TODO crash probability
	int res = bernoulli(0.05); // 5% of a data point changing key
	if(res){
		// Pick new random key, different from current worker
		int newKey = -1;
		while(newKey < 0 || newKey == workerId){
			newKey = uniform(0, par("numWorkers").intValue());
		}
		return newKey;
	}
	return -1;
}

int Worker::reduce(int data[]){
	// TODO crash probability
	return 0;
}

void Worker::sendData(int newKey, int value){
	// Create message
	DataInsertMessage* insertMsg = new DataInsertMessage();

	// Insert <k, v> pair
	insertMsg->setDestWorker(newKey);
	insertMsg->setValue(value);
	
	// Generate request ID
	int reqID = uniform(INT_MIN, INT_MAX);
	insertMsg->setReqID(reqID);

	DataInsertMessage* insertMsgCopy = insertMsg->dup();

	// Get correct gate and send duplicate insert message
	int outputGate = getWorkerGate(newKey);
	send(insertMsgCopy, "out", outputGate);

	//add req to queue and wait for ACK, set timeout
	unstableMessages[reqID] = insertMsg;

	cMessage* timeoutMsg = new cMessage(("Timeout-" + std::to_string(reqID)).c_str());
	timeoutMsg->setContextPointer(new int(reqID));

	scheduleAt(simTime()+timeout, timeoutMsg);

	timeouts[reqID] = timeoutMsg;
}

int Worker::getWorkerGate(int destID){
	if(destID < workerId){
		//out[newKey + 1] (Corresponding to worker "newKey")
		return destID + 1;
	} else if(destID > workerId) {
		//out[newKey]
		return destID;
	}
	EV << "Error: self getWorkerGate";
	return -1;
}
