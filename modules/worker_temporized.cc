#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <omnetpp.h>
#include <algorithm>
#include <deque>

#include "setup_m.h"
#include "datainsert_m.h"
#include "schedule_m.h"
#include "finishLocalElaboration_m.h"
#include "checkChangeKeyAck_m.h"
#include "restart_m.h"
#include "ping_m.h"
#include "finishSim_m.h"
#include "BatchLoader.h"
#include "InsertManager.h"

#define LEADER_PORT 0

#define MAP_EXEC_TIME_AVG 0.0001
#define MAP_EXEC_TIME_STD 0.0003

#define FILTER_EXEC_TIME_AVG 0.0001
#define FILTER_EXEC_TIME_STD 0.0003

#define REDUCE_EXEC_TIME_AVG 0.02
#define REDUCE_EXEC_TIME_STD 0.01

#define CHANGEKEY_EXEC_TIME_AVG 0.0005
#define CHANGEKEY_EXEC_TIME_STD 0.0005

#define FINISH_EXEC_DELAY_AVG 0.02
#define FINISH_EXEC_DELAY_STD 0.02

#define PING_DELAY_AVG 0
#define PING_DELAY_STD 0.01

#define RESTART_DELAY_AVG 0.5
#define RESTART_DELAY_STD 0.02

using namespace omnetpp;

class Worker : public cSimpleModule{
private:
	// Data structures to hold batch, and insertions in progress
	std::map<int, std::deque<int>> data;
	std::map<int, DataInsertMessage*> unstableMessages;
	std::map<int, cMessage*> timeouts;

	// Information on working folder and files
	std::string folder;
	std::string fileName;
	std::string fileProgressName;

	// Data loader instances
	BatchLoader* loader;
	InsertManager* insertManager;

	// Worker information
	int workerId;
	int numWorkers;
	int changeKeyCtr;
	float failureProbability;
	float changeKeyProbability;
	bool failed;
	float insertTimeout;

	// Current Batch Elaboration information
	int currentScheduleStep;
	int batchSize;
	bool reduceLast;
	bool localBatch;

	// General Elaboration Information
	bool finishedLocalElaboration;
	bool finishedPartialCK;
	bool checkChangeKeyReceived;
	bool finishNoticeSent;
	std::vector<std::string> schedule;
	std::vector<int> parameters;

	// Partial Results
	int tmpReduce;
	std::vector<int> tmpResult;

	// Event Message holders
	cMessage *pingResEvent;
	cMessage *nextStepMsg;

	// Ping reply holder
	PingMessage *replyPingMsg;
protected:
	virtual void initialize() override;
	virtual void finish() override;

	// Message handling
	virtual void handleMessage(cMessage *msg) override;
	void handlePingMessage(cMessage *msg);
	void handleSetupMessage(SetupMessage *msg);
	void handleScheduleMessage(ScheduleMessage *msg);
	void handleDataInsertMessage(DataInsertMessage *msg);
	void handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg);
	void handleFinishSimMessage(FinishSimMessage *msg);
	void handleRestartMessage(RestartMessage *msg);

	// Processing data
	void processStep();
	void processReduce();
	void loadNextBatch();
	bool applyOperation(int& value);

	// Next event scheduling
	float calculateDelay(const std::string& operation);

	// Worker operations
	int map(std::string operation, int parameter, int data);
	bool filter(std::string operation, int parameter, int data);
	int changeKey(int data, float probability);
	int reduce(std::vector<int> data);

	//Crash related functions
	void initializeDataModules();
	void loadPartialReduce();
	bool failureDetection();
	void deallocatingMemory();

	// ChangeKey Remote Data Insertion
	void sendData(int newKey, int value, int scheduleStep);

	// Network utilities
	int getWorkerGate(int destID);
	int getInboundWorkerID(int gateIndex);

	// Persisting functions
	void persistingResult(std::vector<int> result);
	void persistingReduce(int reduce);

	// Other utils
	void printingVector(std::vector<int> vector);
};

Define_Module(Worker);

void Worker::initialize(){
	batchSize = par("batchSize").intValue();
	failureProbability = (par("failureProbability").intValue()) / 1000.0;
	numWorkers = par("numWorkers").intValue();

	changeKeyProbability = 0.35;
	insertTimeout = 0.5; //500 ms
	localBatch = true;
	failed = false;
}

void Worker::finish(){
		//data.clear();
		for(auto &pair : unstableMessages) {
	        DataInsertMessage* msg = pair.second; // Extract the pointer to the message
	        delete msg; // Delete the message
	    }
	    unstableMessages.clear();

	    for(auto &pair : timeouts) {
	        cMessage* msg = pair.second; // Extract the pointer to the message
	        delete msg; // Delete the message
	    }
		timeouts.clear();

		// Data loader instances
		delete loader;
		delete insertManager;

		// Event Message holders
		//delete pingResEvent;
		//delete nextStepMsg;

		// Ping reply holder
		delete replyPingMsg;
}

void Worker::handleMessage(cMessage *msg){
	if(msg == nextStepMsg) {
		delete msg;
		processStep();

		return;
	}

    if(msg == pingResEvent){
    	EV<<"\nPingResEvent at worker: "<<workerId<<"\n\n";
    	handlePingMessage(msg);
    	delete msg;
    	return;
    }

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
				int destWorker = insertMsg->second->getDestID();
				DataInsertMessage* insertMsgCopy = insertMsg->second->dup();
				send(insertMsgCopy, "out", getWorkerGate(destWorker));

				// Reschedule the timeout for this message
                cMessage* newTimeoutMsg = new cMessage(("Timeout-" + std::to_string(reqID)).c_str());
                newTimeoutMsg->setContextPointer(new int(reqID));
                scheduleAt(simTime() + insertTimeout, newTimeoutMsg);

                // Update maps with the new timeout message and remove the old one
                timeouts[reqID] = newTimeoutMsg;
                return;
			}
			return;
		}
	}

	PingMessage *pingMsg = dynamic_cast<PingMessage *>(msg);
	if(pingMsg != nullptr){
		replyPingMsg = pingMsg;
		// Generate random delay
		float delay = lognormal(PING_DELAY_AVG, PING_DELAY_STD); // Log-normal to have always positive increments
		pingResEvent = new cMessage("PingResEvent");
		// Schedule response event
        scheduleAt(simTime() + delay , pingResEvent);
		return;
	}

	SetupMessage *setupMsg = dynamic_cast<SetupMessage *>(msg);
    if(setupMsg != nullptr){
        // Successfully cast to SetupMessage, handle it
        handleSetupMessage(setupMsg);
    	delete msg;
        return;
    }

	ScheduleMessage *scheduleMsg = dynamic_cast<ScheduleMessage *>(msg);
    if (scheduleMsg != nullptr) {
        // Successfully cast to ScheduleMessage, handle it
        handleScheduleMessage(scheduleMsg);
    	delete msg;
        return;
    }
    // DataInsertMessage (Could be either to insert here, or an ACK)
    DataInsertMessage *dataInsertMsg = dynamic_cast<DataInsertMessage *>(msg);
    if(dataInsertMsg != nullptr){
    	//Successfully cast to DataInsertMessage, handle it
    	handleDataInsertMessage(dataInsertMsg);
    	delete msg;
    	return;
    }

	FinishLocalElaborationMessage *finishLocalMsg = dynamic_cast<FinishLocalElaborationMessage *>(msg);
	if(finishLocalMsg != nullptr){
		EV<<"Start executing the remain schedule for the latecomers change key data\n";
		handleFinishLocalElaborationMessage(finishLocalMsg);
    	delete msg;
		return;
	}

    FinishSimMessage *finishSimMsg = dynamic_cast<FinishSimMessage *>(msg);
	if(finishSimMsg != nullptr){
		handleFinishSimMessage(finishSimMsg);
    	delete msg;
		return;
	}

	RestartMessage *restartMsg = dynamic_cast<RestartMessage *>(msg);
    if(restartMsg != nullptr) {
    	// Successfully cast to RestartMessage, handle it
    	handleRestartMessage(restartMsg);
    	delete msg;
    	return;
    }
}

void Worker::handlePingMessage(cMessage *msg){
	EV<<"\nReceived ping message at worker: "<<workerId<<"\n\n";
	
	send(replyPingMsg, "out", LEADER_PORT);
	replyPingMsg = nullptr;
	return;
}

void Worker::handleSetupMessage(SetupMessage *msg){
	workerId = msg->getAssigned_id();

	if(par("id").intValue() == -1){ // If the ID was not previously set
		par("id") = workerId;
	}

	int dataSize = msg->getDataArraySize();

	//Persisting data on file
	folder = "Data/Worker_" + std::to_string(workerId) + "/";
	std::ofstream data_file;

	fileName = folder + "data.csv";
	data_file.open(fileName);

	for (int i = 0; i < dataSize; i++) {
	    // Directly write to the file
	    data_file << workerId << ',' << msg->getData(i) << '\n';
	}

	data_file.close();

	initializeDataModules();
}

void Worker::handleScheduleMessage(ScheduleMessage *msg){
	
    int scheduleSize = msg->getScheduleArraySize();

    for(int i=0; i<scheduleSize; i++){
        schedule.push_back(msg->getSchedule(i)) ;
		parameters.push_back(msg->getParameters(i));
    }
    reduceLast = (schedule.back() == "reduce");

    loadNextBatch(); // Load first batch

	nextStepMsg = new cMessage("NextStep");
	scheduleAt(simTime(), nextStepMsg);
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

		auto insertMsgPair = unstableMessages.find(reqID);
		if(insertMsgPair != unstableMessages.end()){
			EV << "Received ACK, deleting unstable message";
			DataInsertMessage *dupMsg = dynamic_cast<DataInsertMessage *>(insertMsgPair->second);
			delete dupMsg;
			unstableMessages.erase(reqID);
		}
	} else {
		// Insert new data point into data vector
		int gateIndex = msg->getArrivalGate()->getIndex();
		int senderID = getInboundWorkerID(gateIndex);

		insertManager->insertValue(senderID, msg->getReqID(), msg->getScheduleStep(), msg->getData());

		finishedPartialCK = false;

		// Send back ACK after insertion
		DataInsertMessage* insertMsg = new DataInsertMessage();
		insertMsg->setReqID(msg->getReqID());
		insertMsg->setAck(true);

		send(insertMsg, "out", gateIndex);
	}
}

void Worker::handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg){
	nextStepMsg = new cMessage("NextStep");

	float delay = lognormal(FINISH_EXEC_DELAY_AVG, FINISH_EXEC_DELAY_STD);
	scheduleAt(simTime() + delay , nextStepMsg);

	checkChangeKeyReceived = true;
	finishedPartialCK = false;
}

void Worker::handleRestartMessage(RestartMessage *msg){
	if(!failed){
		std::cout << "Worker " << workerId << " received a RestartMessage, but has not failed: Restarting..." << std::endl;
		deallocatingMemory();
	}
	failed = false;
	workerId = msg->getWorkerID();
	batchSize = par("batchSize").intValue();
	numWorkers = par("numWorkers").intValue();

	initializeDataModules();
	
	// Re-Initialized worker and data modules, now copy schedule and re-start processing

	int scheduleSize = msg->getScheduleArraySize();

    for(int i=0; i<scheduleSize; i++){
        schedule.push_back(msg->getSchedule(i)) ;
		parameters.push_back(msg->getParameters(i));
    }
    reduceLast = (schedule.back() == "reduce");

    // Load previous partial result

	if(reduceLast) loadPartialReduce();

	nextStepMsg = new cMessage("NextStep");
	float delay = lognormal(RESTART_DELAY_AVG, RESTART_DELAY_STD);

	scheduleAt(simTime() + delay, nextStepMsg);
	return;
}

void Worker::handleFinishSimMessage(FinishSimMessage *msg){
	EV<<"\nApplication finished at worker: "<<workerId<<"\n\n";
}

void Worker::initializeDataModules() {
	fileName = folder + "data.csv";

	// Instantiate a BatchLoader
	fileProgressName = folder + "progress.txt";
	loader = new BatchLoader(fileName, fileProgressName, batchSize);

	// Instantiate an InsertManager
	std::string insertFilename = folder + "inserted.csv";
	std::string requestFilename = folder + "requests_log.csv";
	insertManager = new InsertManager(insertFilename, requestFilename, batchSize);
}

void Worker::processStep(){
	if(failed) return;

	if(failureDetection()){
		failed = true;
		EV<<"FAILURE DETECTED AT WORKER: "<<workerId<<", deallocating memory\n";
		deallocatingMemory();
		return;
	}

	if(currentScheduleStep >= schedule.size()) {
		if(reduceLast) {
			persistingReduce(tmpReduce);
			tmpReduce = 0;
		} else {
			persistingResult(tmpResult);
			tmpResult.clear();
		}

		loadNextBatch();
		
		EV<<"Status - Worker " << workerId << " - FinishedLocal: " << finishedLocalElaboration << " - FinishedCK: " << finishedPartialCK << " - CheckCKReceived: " << checkChangeKeyReceived << std::endl;
		
		if(finishedLocalElaboration && finishedPartialCK && !finishNoticeSent) {
			EV<<"\nSENDING FINISHED LOCAL ELABORATION WORKER: "<<workerId<<"\n\n";
			FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
			finishLocalMsg->setWorkerId(workerId);
			send(finishLocalMsg, "out", 0);	
			finishNoticeSent = true;
		}

		if(finishNoticeSent && finishedPartialCK && !checkChangeKeyReceived) {
			EV<<"\nTemporarily finished elaborating ChangeKeys - Status: Idle\n\n";
			return;
		}

		if(finishNoticeSent && finishedPartialCK && checkChangeKeyReceived) {
			CheckChangeKeyAckMessage* checkChangeKeyAckMsg = new CheckChangeKeyAckMessage();
			checkChangeKeyAckMsg->setWorkerId(workerId);
			send(checkChangeKeyAckMsg, "out", 0);
			EV<<"\nChangeKey checked at worker: "<<workerId<<"\n\n";
			return;
		}
	}

	if(!data[currentScheduleStep].empty()){
		int value = data[currentScheduleStep].front();
		data[currentScheduleStep].pop_front();
		EV << "Worker " << workerId << " - Elaborating: " << value << " - Operation: " << schedule[currentScheduleStep] << std::endl;

		bool result = applyOperation(value);

		EV << "Result: " << value << std::endl;

		/*
		* 2 Cases in which we add data:
		*   - To proceed with the schedule
		*   - If our last operation is not a reduce
		*/
		if(result){
			if(currentScheduleStep + 1 < schedule.size()){
				data[currentScheduleStep + 1].push_back(value);
			} else if(!reduceLast){
				tmpResult.push_back(value);
			}
		}

		float delay = calculateDelay(schedule[currentScheduleStep]);

		nextStepMsg = new cMessage("NextStep");
		scheduleAt(simTime()+delay, nextStepMsg);
	} else {
		EV << "Empty map, finished current step\n\n";
		currentScheduleStep++;
		if(reduceLast && currentScheduleStep == schedule.size() - 1) {
			EV << "Entering reduce\n\n";
			processReduce();
			return;
		}
		processStep();
	}
}

void Worker::processReduce(){
	tmpReduce += reduce({data[currentScheduleStep].begin(), data[currentScheduleStep].end()});
	EV << "Current value: " << tmpReduce << std::endl;

	currentScheduleStep++;

	float delay = calculateDelay(schedule[currentScheduleStep]);

	nextStepMsg = new cMessage("NextStep");
	scheduleAt(simTime()+delay, nextStepMsg);
}

void Worker::loadNextBatch(){
	if(localBatch) {
		EV << "Loading local..." << std::endl;
		std::vector<int> batch = loader->loadBatch();
		if(batch.empty()){
			finishedLocalElaboration = true;
			localBatch = false;
			loadNextBatch();
			std::cout << "Finished local, switching to ck" << std::endl;
			return;
		}
        data[0].insert(data[0].end(), batch.begin(), batch.end());
	} else if(!finishedPartialCK) {
		EV << "Loading CK..." << std::endl;
		std::map<int, std::vector<int>> ckBatch = insertManager->getBatch();

		if(ckBatch.empty()){
			finishedPartialCK = true;
			EV << "CK data empty" << std::endl;
		} else {
			EV << "\n\nCK not empty";
			for(int i=0; i < schedule.size(); i++) {
				data[i].insert(data[i].end(), ckBatch[i].begin(), ckBatch[i].end());
			}
		}
	}
	localBatch = !localBatch && !finishedLocalElaboration; // Alternates only if local elaboration is not finished
	currentScheduleStep = 0;
}

bool Worker::applyOperation(int& value){
	const std::string& operation = schedule[currentScheduleStep];
	const int& parameter = parameters[currentScheduleStep];

	if(operation == "add" || operation == "sub" || operation == "mul" || operation == "div") {
		int res = map(operation, parameter, value);
		EV << "Map result: " << res << std::endl;
		value = res;

		return true;
	} else if(operation == "lt" || operation == "gt" || operation == "le" || operation == "ge") {
		return filter(operation, parameter, value);

	} else if(operation == "changekey") {
		int newKey = changeKey(value, changeKeyProbability);
        if(newKey != -1) {
        	EV << "Changing Key: " << workerId << " -> " << newKey << " for value: "<<value<<"\n";
        	sendData(newKey, value, currentScheduleStep); // 'i' == Schedule step

        	return false;
        }
        return true;
	}
	return false;
}

float Worker::calculateDelay(const std::string& operation){
	if(operation == "add" || operation == "sub" || operation == "mul" || operation == "div") {
		return lognormal(MAP_EXEC_TIME_AVG, MAP_EXEC_TIME_STD);
	} else if(operation == "lt" || operation == "gt" || operation == "le" || operation == "ge") {
		return lognormal(FILTER_EXEC_TIME_AVG, FILTER_EXEC_TIME_STD);
	} else if(operation == "changekey") {
		return lognormal(CHANGEKEY_EXEC_TIME_AVG, CHANGEKEY_EXEC_TIME_STD);
	} else if(operation == "reduce") {
		return lognormal(REDUCE_EXEC_TIME_AVG, REDUCE_EXEC_TIME_STD);
	}
	return 0;
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

bool Worker::filter(std::string operation, int parameter, int data){
	if(operation == "lt"){
		return data < parameter;

	} else if(operation == "gt"){
		return data > parameter;

	} else if(operation == "le"){
		return data <= parameter;

	} else if(operation == "ge"){
		return data >= parameter;

	}
	return false;
}

int Worker::changeKey(int data, float probability){
	int ckValue = data % (static_cast<int>(1/(probability))*numWorkers);
	if(ckValue == workerId || ckValue >= numWorkers || ckValue < 0) {
		return -1;
	}
	return ckValue;
}

int Worker::reduce(std::vector<int> data){
	int result = 0;

	for(int& value : data) {
		result += value;
	}

	return result;
}

void Worker::loadPartialReduce(){
	std::string filename = "Data/Worker_" + std::to_string(workerId) + "/result.csv";
	std::ifstream file(filename);
	std::string line;

	if(file.is_open()){
		if (std::getline(file, line)) {
			std::istringstream iss(line);

			int savedResult;
			if (iss >> savedResult) {
				tmpReduce = savedResult; 
			}
		}
		file.close();
	}
}

bool Worker::failureDetection(){
	int res = bernoulli(failureProbability);
	if(res){
		return true;
	}
	return false;

}

void Worker::deallocatingMemory(){
	data.clear();
	for (auto& pair : unstableMessages) {
		cancelAndDelete(pair.second);
	}
	unstableMessages.clear();
	
	for(auto& pair : timeouts){
		cancelAndDelete(pair.second);
	}
	timeouts.clear();

	fileName = "";
	fileProgressName = "";
	batchSize = 0;
	workerId = 0;

	// Data loader instances
	delete loader;
	delete insertManager;

	// Worker information
	numWorkers = 0;
	changeKeyCtr = 0;

	// Current Batch Elaboration information
	currentScheduleStep = 0;
	reduceLast = false;
	localBatch = true;

	// General Elaboration Information
	finishedLocalElaboration = false;
	finishedPartialCK = false;
	checkChangeKeyReceived = false;
	finishNoticeSent = false;

	schedule.clear();
	parameters.clear();

	// Partial Results
	tmpReduce = 0;
	tmpResult.clear();

	// Event Message holders
	cancelAndDelete(nextStepMsg);
	//cancelAndDelete(pingResEvent);
	//std::cout << "DEL7";
}

void Worker::sendData(int newKey, int value, int scheduleStep){
	// Create message
	DataInsertMessage* insertMsg = new DataInsertMessage();

	// Insert <k, v> pair
	insertMsg->setDestID(newKey);
	insertMsg->setData(value);
	
	// Generate request ID
	insertMsg->setReqID(changeKeyCtr);

	insertMsg->setScheduleStep(scheduleStep);
	insertMsg->setAck(false);

	DataInsertMessage* insertMsgCopy = insertMsg->dup();

	// Get correct gate and send duplicate insert message
	int outputGate = getWorkerGate(newKey);
	send(insertMsgCopy, "out", outputGate);

	//add req to queue and wait for ACK, set timeout
	unstableMessages[changeKeyCtr] = insertMsg;

	cMessage* timeoutMsg = new cMessage(("Timeout-" + std::to_string(changeKeyCtr)).c_str());
	timeoutMsg->setContextPointer(new int(changeKeyCtr));

	scheduleAt(simTime() + insertTimeout, timeoutMsg);

	timeouts[changeKeyCtr] = timeoutMsg;
	changeKeyCtr++;
}

int Worker::getInboundWorkerID(int gateIndex) {
	if(gateIndex <= workerId) {
		return gateIndex - 1;
	}
	return gateIndex;
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

void Worker::persistingReduce(int reducedValue){
	std::string folder = "Data/Worker_" + std::to_string(workerId) + "/";
	std::string fileName = folder + "result.csv";

	std::ofstream result_file(fileName);
	if(result_file.is_open()){
		EV << "Opened reduce file\n";
		result_file << reducedValue;

		result_file.close();
		
	}else{
		EV << "Can't open file: " << fileName << "\n";
	}
	
}

void Worker::printingVector(std::vector<int> vector){
    for(int i=0; i<vector.size(); i++){
        EV<<vector[i]<<" ";
    }
}