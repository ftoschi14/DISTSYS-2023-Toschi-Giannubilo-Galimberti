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
#include "nextstep_m.h"
#include "pingres_m.h"

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
	std::map<int, std::deque<int>> debugData;

	DataInsertMessage* unstableMessage;
	cMessage* insertTimeoutMsg;

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

	// For ChangeKey protocol
	int changeKeySent;
	int changeKeyReceived;
	bool waitingForInsert;

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
	PingResMessage *pingResEvent;
	NextStepMessage *nextStepMsg;

	// Ping reply holder
	PingMessage *replyPingMsg;

	SimTime previousCrash;
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
	void loadPartialResults();
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
	void persistCKCounter();

	// Other utils
	void printingVector(std::vector<int> vector);
	void printScheduledData(std::map<int, std::deque<int>> data);
	void printDataInsertMessage(DataInsertMessage* msg, bool recv);
	bool isScheduleEmpty();
};

Define_Module(Worker);

void Worker::initialize(){
	batchSize = par("batchSize").intValue();
	failureProbability = (par("failureProbability").intValue()) / 1000.0;
	numWorkers = par("numWorkers").intValue();

	changeKeyProbability = 0.85;
	insertTimeout = 0.5; //500 ms
	localBatch = true;
	failed = false;

	nextStepMsg = new NextStepMessage("NextStep");
	pingResEvent = new PingResMessage("PingRes");
}

void Worker::finish(){
	std::cout << "Worker " << workerId << " finished with value: ";
	if(reduceLast){
		std::cout << tmpReduce << std::endl;
	} 
	//data.clear();
	if(waitingForInsert) {
		delete unstableMessage;
	}

	if(insertTimeoutMsg != nullptr && insertTimeoutMsg->isScheduled() && waitingForInsert){
		cancelEvent(insertTimeoutMsg);
	}


	if(failed){
		std::cout << "Finished, but a worker has failed" << std::endl;
		return;
	} else {
		std::cout << "Worker " << workerId << " - Inserted Data Empty? " << insertManager->isEmpty() << std::endl;
	}

	// Data loader instances
	delete loader;
	delete insertManager;

	// Event Message holders
	delete pingResEvent;
	delete nextStepMsg;

	delete replyPingMsg;
}

void Worker::handleMessage(cMessage *msg){
	//std::cout << "Worker " << workerId << " received a message - Scheduled at: " << msg->getArrivalTime() << std::endl;
	if(msg == nextStepMsg) {
		processStep();
		return;
	}

    if(msg == pingResEvent){
    	handlePingMessage(msg);
    	return;
    }

	/* Timeout Message chunk
	*  Get the context pointer, if not null it should correspond to a request ID
	*  Get the corresponding timeout and insertMsg, re-send it and re-schedule a timeout.
	*/
	if(msg == insertTimeoutMsg) {
		// Get the worker ID from the message
		int destWorker = unstableMessage->getDestID();
		DataInsertMessage* insertMsgCopy = unstableMessage->dup();
		send(insertMsgCopy, "out", getWorkerGate(destWorker));

		// Reschedule the timeout for this message
        scheduleAt(simTime() + insertTimeout, insertTimeoutMsg);
        return;
	}

	PingMessage *pingMsg = dynamic_cast<PingMessage *>(msg);
	if(pingMsg != nullptr){

		if(failed){
			delete msg;
			return;
		}

		if(replyPingMsg == nullptr){
			replyPingMsg = pingMsg;
		} else {
			delete msg;
		}

		if(pingResEvent != nullptr && pingResEvent->isScheduled()){
			cancelEvent(pingResEvent);
		}

		// Generate random delay
		float delay = lognormal(PING_DELAY_AVG, PING_DELAY_STD); // Log-normal to have always positive increments
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
	if(failed) {
		return;
	}
	
	send(replyPingMsg->dup(), "out", LEADER_PORT);
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

	scheduleAt(simTime(), nextStepMsg);
}

void Worker::handleDataInsertMessage(DataInsertMessage *msg){
	if(failed){
		EV << "Received DataInsert - dropped" << std::endl;
		delete msg;
		return;
	}
	// Check if it is an ACK or an insertion to me (workerID)
	if(msg->getAck()){
		// Look for corresponding reqID and cancel timeout
		cancelEvent(insertTimeoutMsg);
		delete unstableMessage;

		scheduleAt(simTime(), nextStepMsg);
		waitingForInsert = false;
		changeKeySent++;
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
		changeKeyReceived++;
	}
	delete msg;
}

void Worker::handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg){
	if(nextStepMsg != nullptr && nextStepMsg->isScheduled()) {
		cancelEvent(nextStepMsg);
	}

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

	if(reduceLast) loadPartialResults();

	loadNextBatch();

	if(nextStepMsg != nullptr && nextStepMsg->isScheduled()) {
		cancelEvent(nextStepMsg);
	}

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

	if(currentScheduleStep >= schedule.size()) {
		if(reduceLast) {
			persistingReduce(tmpReduce);
		} else {
			persistingResult(tmpResult);
			tmpResult.clear();
		} 

		persistCKCounter();

		while(isScheduleEmpty() && (!finishedLocalElaboration || !finishedPartialCK)){
			loadNextBatch();
		}

		std::cout << "Worker " << workerId << " - Loaded data:" << std::endl;
		printScheduledData(data);
		
		EV<<"Status - Worker " << workerId << " - FinishedLocal: " << finishedLocalElaboration << " - FinishedCK: " << finishedPartialCK << " - CheckCKReceived: " << checkChangeKeyReceived << std::endl;
		
		if(finishedLocalElaboration && finishedPartialCK && !finishNoticeSent) {
			EV<<"\nSENDING FINISHED LOCAL ELABORATION WORKER: "<<workerId<<"\n\n";
			FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
			finishLocalMsg->setWorkerId(workerId);
			finishLocalMsg->setChangeKeyReceived(changeKeyReceived);
			finishLocalMsg->setChangeKeySent(changeKeySent);
			send(finishLocalMsg, "out", 0);	
			finishNoticeSent = true;
		}

		if(finishNoticeSent && finishedPartialCK && !checkChangeKeyReceived) {
			std::cout<<"Worker " << workerId << " - Temporarily finished elaborating ChangeKeys - Status: Idle\n\n";
			return;
		}

		if(finishNoticeSent && finishedPartialCK && checkChangeKeyReceived) {
			CheckChangeKeyAckMessage* checkChangeKeyAckMsg = new CheckChangeKeyAckMessage();
			checkChangeKeyAckMsg->setWorkerId(workerId);
			checkChangeKeyAckMsg->setPartialRes(tmpReduce);
			checkChangeKeyAckMsg->setChangeKeyReceived(changeKeyReceived);
			checkChangeKeyAckMsg->setChangeKeySent(changeKeySent);
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

		if(failed) return;

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

		if(waitingForInsert) return;

		float delay = calculateDelay(schedule[currentScheduleStep]);
		scheduleAt(simTime()+delay, nextStepMsg);
	} else {
		EV << "Empty map, finished current step\n\n";
		std::cout << "Worker " << workerId << " finished step " << currentScheduleStep << " - New step data: " << std::endl;
		printScheduledData(data);
		std::cout << "\n";
		currentScheduleStep++;
		if(reduceLast && currentScheduleStep == schedule.size() - 1) {
			EV << "Entering reduce\n\n";
			std::cout << "Worker " << workerId << " reducing: ";
			processReduce();
			return;
		}
		processStep();
	}
}

void Worker::processReduce(){
	int batchRes = reduce({data[currentScheduleStep].begin(), data[currentScheduleStep].end()});

	std::cout << tmpReduce << " + " << batchRes << " = " << (tmpReduce + batchRes) << std::endl;

	tmpReduce = tmpReduce + batchRes;

	data[currentScheduleStep].clear();

	currentScheduleStep++;

	float delay = calculateDelay(schedule[currentScheduleStep]);

	scheduleAt(simTime()+delay, nextStepMsg);
}

void Worker::loadNextBatch(){
	data.clear();

	insertManager->persistData();
	loader->saveProgress();

	if(localBatch) {
		std::cout << "Worker " << workerId << " - Loading local:" << std::endl;
		std::vector<int> batch = loader->loadBatch();
		if(batch.empty()){
			finishedLocalElaboration = true;
			localBatch = false;
			//std::cout << "Finished local, switching to ck" << std::endl;
		} else {
        	data[0].insert(data[0].end(), batch.begin(), batch.end());
		}
	} else {
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
	if(failureDetection()){
		failed = true;
		std::cout<<"FAILURE DETECTED AT WORKER: "<<workerId<<", deallocating memory\n";
		deallocatingMemory();
		return false;
	}

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
        	std::cout << "Changing Key: " << workerId << " -> " << newKey << " for value: "<<value<<"\n";
        	sendData(newKey, value, currentScheduleStep + 1); // 'i' == Schedule step

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

void Worker::loadPartialResults(){
	std::string res_filename = "Data/Worker_" + std::to_string(workerId) + "/result.csv";
	std::ifstream res_file(res_filename, std::ios::binary);
	std::string line;

	if(res_file.is_open()){
		if (std::getline(res_file, line)) {
			std::istringstream iss(line);

			int savedResult;
			if (iss >> savedResult) {
				tmpReduce = savedResult; 
			}
		}
		res_file.close();
	}

	std::string ck_filename = "Data/Worker_" + std::to_string(workerId) + "/Data_CK.csv";
	std::ifstream ck_file(ck_filename, std::ios::binary);

	if(ck_file.is_open()){
		if (std::getline(ck_file, line)) {
			std::istringstream iss(line);

			int savedResult;
			if (iss >> savedResult) {
				changeKeyCtr = savedResult; 
			}
		}
		ck_file.close();
	}

	std::cout << "Worker " << workerId << " loaded reduce: " << tmpReduce << " [CRASH RECOVERY]" << std::endl;
}

bool Worker::failureDetection(){
	int res = bernoulli(failureProbability);
	if(res){
		return true;
	}
	return false;

}

void Worker::deallocatingMemory(){
	std::cout << "Worker " << workerId << " failing..." << std::endl;
	failed = true;

	std::cout << "\nFailed at: \n";
	printScheduledData(debugData);
	std::cout << "\n";

	debugData = data;
	data.clear();

	if(waitingForInsert) {
		delete unstableMessage;
	}

	if(insertTimeoutMsg != nullptr && insertTimeoutMsg->isScheduled() && waitingForInsert){
		cancelEvent(insertTimeoutMsg);
	}

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
	localBatch = !localBatch;

	// General Elaboration Information
	finishedLocalElaboration = false;
	finishedPartialCK = false;
	waitingForInsert = false;
	//checkChangeKeyReceived = false;
	//finishNoticeSent = false;

	schedule.clear();
	parameters.clear();

	// Partial Results
	tmpReduce = 0;
	tmpResult.clear();

	if(pingResEvent != nullptr && pingResEvent->isScheduled()) {
		cancelEvent(pingResEvent);
	}

	if(nextStepMsg != nullptr && nextStepMsg->isScheduled()) {
		cancelEvent(nextStepMsg);
	}
	std::cout << "Crashed successfully\n";
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
	unstableMessage = insertMsg;

	cMessage* timeoutMsg = new cMessage(("Timeout-" + std::to_string(changeKeyCtr)).c_str());
	timeoutMsg->setContextPointer(new int(changeKeyCtr));

	scheduleAt(simTime() + insertTimeout, timeoutMsg);

	insertTimeoutMsg = timeoutMsg;
	changeKeyCtr++;

	waitingForInsert = true;
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

	//std::cout << "Worker " << workerId << " persisting: " << reducedValue << std::endl;

	std::ofstream result_file(fileName);
	if(result_file.is_open()){
		EV << "Opened reduce file\n";
		result_file << reducedValue;

		result_file.close();
		
	}else{
		EV << "Can't open file: " << fileName << "\n";
	}
	
}

void Worker::persistCKCounter(){
	std::string folder = "Data/Worker_" + std::to_string(workerId) + "/";
	std::string fileName = folder + "Data_CK.csv";

	std::ofstream result_file(fileName);
	if(result_file.is_open()){
		EV << "Opened CK file\n";
		result_file << changeKeyCtr;

		result_file.close();
		
	}else{
		EV << "Can't open file: " << fileName << "\n";
	}
}

void Worker::printingVector(std::vector<int> vector){
    for(int i=0; i<vector.size(); i++){
        std::cout<<vector[i]<<" ";
    }
    std::cout << std::endl;
}

void Worker::printScheduledData(std::map<int, std::deque<int>> data){
	for(int i = 0; i<data.size(); i++){
		if(data[i].size() > 0) std::cout << "Step " << i << ": ";

		for(int j = 0; j < data[i].size(); j++){
			std::cout << data[i][j] << " ";
		}

		if(data[i].size() > 0) std::cout << std::endl;
	}
}

void Worker::printDataInsertMessage(DataInsertMessage* msg, bool recv){
	if(recv){
		std::cout << "Worker " << workerId << " received DataInsert:\n";
		std::cout << "Sender: " << getInboundWorkerID(msg->getArrivalGate()->getIndex()) << std::endl;
	}
	std::cout << "Dest ID: " << msg->getDestID() << std::endl;
	std::cout << "Req ID: " << msg->getReqID() << std::endl;
	std::cout << "Data: " << msg->getData() << std::endl;
	std::cout << "Schedule step: " << msg->getScheduleStep() << std::endl;
	std::cout << "Ack: " << msg->getAck() << std::endl;
	return;
}

bool Worker::isScheduleEmpty(){
	for (const auto& pair : data) {
        if (!pair.second.empty()) {
            // Found a non-empty deque
            return false;
        }
    }
    // All deques are empty
    return true;
}