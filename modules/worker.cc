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
	int iterations;

protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
	void handleSetupMessage(SetupMessage *msg);
	void handleDataInsertMessage(DataInsertMessage *msg);
	void handleScheduleMessage(ScheduleMessage *msg);
	void applySchedule(std::vector<std::string> schedule, std::vector<int> parameters);
	int map(std::string operation, int parameter, int data);
	std::vector<int> filter(std::string operation, std::vector<int> discard, int data, int parameter);
	std::vector<int> discardingData(std::vector<int> discard, std::vector<int> data);
	void persistingResult(std::vector<int> result);
	void printingVector(std::vector<int> vector);
	int changeKey(int data);
	int reduce(int data, int reduce, int iteration);
	void persistingReduce(int reduce);
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
	iterations = (dataSize / batchSize) +1;
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
    
	for(int i=0; i<iterations; i++){
		data = loader->loadBatch();
		applySchedule(schedule, parameters);
	}
	
}

void Worker::applySchedule(std::vector<std::string> schedule, std::vector<int> parameters) {
    int scheduleSize = schedule.size();
    std::vector<int> discard;
	int reducedValue = 0;
    for (int i = 0; i < scheduleSize; i++) {
        for (int j = 0; j < data.size(); j++) {
            if (schedule[i] == "add" || schedule[i] == "sub" || schedule[i] == "mul" || schedule[i] == "div") {
                data[j] = map(schedule[i], parameters[i], data[j]);
            } else if (schedule[i] == "lt" || schedule[i] == "gt" || schedule[i] == "le" || schedule[i] == "ge") {
                discard = filter(schedule[i], discard, data[j], parameters[i]);
                if (j == data.size() - 1) {
                    data = discardingData(discard, data);
                } 
            } else if (schedule[i] == "reduce") {
				reducedValue = reduce(data[j], reducedValue, j);
            } else if (schedule[i] == "changekey") {
                // TODO: changekey function
            } else {
                EV << "Invalid operation: " << schedule[i] << "\n";
            }
        }
    }
	if(schedule[scheduleSize-1] == "reduce"){
		persistingReduce(reducedValue);
	}else{
		persistingResult(data);
	}
}

int Worker::map(std::string operation, int parameter, int data){
	if(operation == "add"){
		return data + parameter;
	} else if(operation == "sub"){
		return data - parameter;
	} else if(operation == "mul"){
		return data * parameter;
	} else if(operation == "div"){
		return data / parameter;
	} else {
		return 0;
	}
}

std::vector<int> Worker::filter(std::string operation, std::vector<int> discard, int data, int parameter){
	if(operation == "lt"){
		if(data >= parameter){
			discard.push_back(1);
		} else {
			discard.push_back(0);
		}
	} else if(operation == "gt"){
		if(data <= parameter){
			discard.push_back(1);
		} else {
			discard.push_back(0);
		}
	} else if(operation == "le"){
		if(data > parameter){
			discard.push_back(1);
		} else {
			discard.push_back(0);
		}
	} else if(operation == "ge"){
		if(data < parameter){
			discard.push_back(1);
		} else {
			discard.push_back(0);
		}
	}
	return discard;
}

std::vector<int> Worker::discardingData(std::vector<int> discard, std::vector<int> data) {
    for (int i = discard.size() - 1; i >= 0; i--) {
        if (discard[i] == 1) {
            data.erase(data.begin() + i);
        }
    }
    return data;
}

void Worker::persistingResult(std::vector<int> result) {
    std::string folder = "Data/Worker_" + std::to_string(workerId) + "/";
    std::ofstream result_file;

    std::string fileName = folder + "result.csv";
    result_file.open(fileName, std::ios_base::app); // Open file in append mode

    for (int i = 0; i < result.size(); i++) {
        // Append data to the file
        result_file << result[i] << '\n';
    }

    result_file.close();
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

int Worker::reduce(int data, int reducedValue, int iteration){
	std::string filename = "Data/Worker_" + std::to_string(workerId) + "/result.csv";
	std::ifstream file(filename);
	if(iteration == 0){
		std::string line;
		if (std::getline(file, line)) { 
			std::istringstream iss(line);
			int firstInteger;
			if (iss >> firstInteger) {
				reducedValue += firstInteger; 
			}
		}
		file.close();
	}
	reducedValue += data;

    return reducedValue; 
}

void Worker::persistingReduce(int reducedValue){
	std::string folder = "Data/Worker_" + std::to_string(workerId) + "/";
	std::ofstream result_file;

	std::string fileName = folder + "result.csv";
	result_file.open(fileName);

	result_file << reducedValue;

	result_file.close();
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
