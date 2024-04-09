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

#define EXPERIMENT_NAME "Increasing_Number_of_Data"

#define LEADER_PORT 0

// ----- Fast operations -----

#define PING_DELAY_AVG 0.0001
#define PING_DELAY_STD 0.001

#define FINISH_EXEC_DELAY_AVG 0.0002
#define FINISH_EXEC_DELAY_STD 0.001

#define MAP_EXEC_TIME_AVG 0.001
#define MAP_EXEC_TIME_STD 0.003

#define FILTER_EXEC_TIME_AVG 0.001
#define FILTER_EXEC_TIME_STD 0.003

// (Fast when local)
#define CHANGEKEY_EXEC_TIME_AVG 0.001
#define CHANGEKEY_EXEC_TIME_STD 0.003

// ----- Medium-duration operations -----

#define REDUCE_EXEC_TIME_AVG 0.01
#define REDUCE_EXEC_TIME_STD 0.005

#define BATCH_LOAD_TIME_AVG 0.01
#define BATCH_LOAD_TIME_STD 0.005

// ----- Slow operations -----
#define RESTART_DELAY_AVG 1.5
#define RESTART_DELAY_STD 0.2

using namespace omnetpp;

class Worker : public cSimpleModule{
private:
	// Data structures to hold batch, and insertions in progress
	std::map<int, std::deque<int>> data;

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
	int changeKeySent; //to be saved
	int changeKeyReceived; //to be saved
	bool waitingForInsert;

	// Current Batch Elaboration information
	int currentScheduleStep;
	int batchSize;
	bool reduceLast;
	bool localBatch;
	bool previousLocal;
	bool idle;

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

	// Parameter conversion for lognormal distribution
	std::map<std::string, std::pair<double, double>> lognormal_params;

	// Others - For Logging (Ignore)
	std::map<std::string, std::vector<simtime_t>> per_op_exec_times;
	std::vector<simtime_t> per_schedule_exec_times;
	simtime_t begin_op;
	simtime_t begin_batch;
	simtime_t begin_elab;

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
	void loadChangeKeyData();
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
	void persistCKSentReceived();
	void persistCKCounter();

	// Delay distrbution utils
	void convertParameters();
	std::pair<double, double> calculateDistributionParams(double mean, double std);

	// Other utils
	void loadSavedResult();
	void printingVector(std::vector<int> vector);
	void printScheduledData(std::map<int, std::deque<int>> data);
	void printDataInsertMessage(DataInsertMessage* msg, bool recv);
	bool isScheduleEmpty();
	void logSimData();
	std::string getParentOperation(const std::string& op);
};

Define_Module(Worker);

void Worker::initialize(){
	changeKeyCtr = 0;
	changeKeySent = 0;
	changeKeyReceived = 0;

	waitingForInsert = false;
	idle = false;

	// Current Batch Elaboration information
	currentScheduleStep = 0;

	// General Elaboration Information
	finishedLocalElaboration = false;
	finishedPartialCK = false;
	checkChangeKeyReceived = false;
	finishNoticeSent = false;

	// Partial Results
	tmpReduce = 0;
	std::vector<int> tmpResult = {};

	batchSize = par("batchSize").intValue();
	failureProbability = (par("failureProbability").intValue()) / 1000.0;
	numWorkers = par("numWorkers").intValue();

	changeKeyProbability = 0.85;
	insertTimeout = 0.5; //500 ms
	localBatch = true;
	failed = false;

	convertParameters();

	nextStepMsg = new NextStepMessage("NextStep");
	pingResEvent = new PingResMessage("PingRes");
	unstableMessage = new DataInsertMessage();
	insertTimeoutMsg = new cMessage("Timeout");
}

void Worker::finish(){
	std::cout << "Worker " << workerId << " finished with value: ";
	if(reduceLast){
		std::cout << tmpReduce << "\n";
	} 
	//data.clear()

	if(failed){
		std::cout << "Finished, but a worker has failed" << "\n";
		return;
	} else {
		std::cout << "Worker " << workerId << " - Inserted Data Empty? " << insertManager->isEmpty() << "\n";
	}
	// Data loader instances
	delete loader;
	delete insertManager;

	// Event Message holders
	delete pingResEvent;
	delete nextStepMsg;

	logSimData();
}

void Worker::handleMessage(cMessage *msg){
	//std::cout << "Worker " << workerId << " received a message - Scheduled at: " << msg->getArrivalTime() << "\n";
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

		delete msg;

		if(pingResEvent != nullptr && pingResEvent->isScheduled()){
			cancelEvent(pingResEvent);
		}

		// Generate random delay
		double delay = calculateDelay("ping"); // Log-normal to have always positive increments
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
	PingMessage *pingMsg = new PingMessage();
	pingMsg->setWorkerId(workerId);
	send(pingMsg, "out", LEADER_PORT);
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

    // Logging code (IGNORE)
    begin_elab = simTime();
    begin_batch = simTime();
    begin_op = simTime();
    // End of logging code

	scheduleAt(simTime(), nextStepMsg);
}

void Worker::handleDataInsertMessage(DataInsertMessage *msg){
	if(failed){
		EV << "Received DataInsert - dropped" << "\n";
		delete msg;
		return;
	}
	// Check if it is an ACK or an insertion to me (workerID)
	if(msg->getAck()){
		// Look for corresponding reqID and cancel timeout
		if(insertTimeoutMsg != nullptr && insertTimeoutMsg->isScheduled()) {
			cancelEvent(insertTimeoutMsg);
			delete unstableMessage;
		}

		if(nextStepMsg->isScheduled()) {
			cancelEvent(nextStepMsg);
		}

		scheduleAt(simTime(), nextStepMsg);
		waitingForInsert = false;
		changeKeySent++;
		persistCKSentReceived();
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
		persistCKSentReceived();
	}
	delete msg;
}

void Worker::handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg){
	checkChangeKeyReceived = true;
	finishedPartialCK = false;

	if(waitingForInsert && insertTimeoutMsg != nullptr && insertTimeoutMsg->isScheduled()) return;

	if(nextStepMsg != nullptr && nextStepMsg->isScheduled()) {
	cancelEvent(nextStepMsg);
	}
	
	// Logging
	if(idle) {
		begin_batch = simTime();
		begin_op = simTime();
	}
	// End of logging
	
	double delay = calculateDelay("finish");
	scheduleAt(simTime() + delay , nextStepMsg);
}

void Worker::handleRestartMessage(RestartMessage *msg){
	if(!failed){
		std::cout << "Worker " << workerId << " received a RestartMessage, but has not failed: Restarting..." << "\n";
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

	loadChangeKeyData();
	/*
	Adding a integer when saving the change key counter (1 for local data, 0 for change key data) in this way when the changeKeyData are loaded can be retrieved 
	which batch has to be loaded to continue the elaboration

	if(changeKeyCtr == 1) localBatch = true;
	else localBatch = false;

	In this way is differentiated the load next batch operation and it doesn't restart always from the change key batch because when restarting the localBatch is 
	always the opposite
	*/
	loadNextBatch();

	if(nextStepMsg != nullptr && nextStepMsg->isScheduled()) {
		cancelEvent(nextStepMsg);
	}

	double delay = calculateDelay("restart");

	// Logging
    begin_batch = simTime();
    begin_op = simTime();
	// End of logging
	
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
	std::string tempFilename = folder + "ck_batch.csv";
	insertManager = new InsertManager(insertFilename, requestFilename, tempFilename, batchSize);
}

void Worker::processStep()
{
	if(failed)
	{
	    EV << "Worker failed!\n";
	    return;
	}

	if(currentScheduleStep >= schedule.size())
	{
		// Logging code (IGNORE)

		simtime_t end_batch = simTime();
		simtime_t batch_duration = end_batch - begin_batch;

		if(reduceLast) {
			simtime_t last_op_duration = end_batch - begin_op;
			per_op_exec_times[getParentOperation(schedule[schedule.size() - 1])].push_back(last_op_duration);
		}

		per_schedule_exec_times.push_back(batch_duration);

		// End of Logging code

		if(reduceLast) {
			persistingReduce(tmpReduce);
		} else {
			// Handle changeKeys in last schedule step
			if(data.size() == (schedule.size() + 1) && !data[schedule.size()].empty()) {
				for(const auto& value : data[schedule.size()]) {
					tmpResult.push_back(value);
				}
				data[schedule.size()].clear(); // Do this to make the scheduled data empty
			}

			persistingResult(tmpResult);
			tmpResult.clear();
		} 

		if(previousLocal) {
			loader->saveProgress();
		} else {
			insertManager->persistData();
		}

		while(isScheduleEmpty() && (!finishedLocalElaboration || !finishedPartialCK)){
			loadNextBatch();
		}

		persistCKCounter();

		//std::cout << "Worker " << workerId << " - Loaded data:" << "\n";
		//printScheduledData(data);
		
		EV<<"Status - Worker " << workerId << " - FinishedLocal: " << finishedLocalElaboration << " - FinishedCK: " << finishedPartialCK << " - CheckCKReceived: " << checkChangeKeyReceived << "\n";
		
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
			idle = true;
			return;
		}

		if(finishNoticeSent && finishedPartialCK && checkChangeKeyReceived) {
			CheckChangeKeyAckMessage* checkChangeKeyAckMsg = new CheckChangeKeyAckMessage();
			checkChangeKeyAckMsg->setWorkerId(workerId);

			if(reduceLast) {
				checkChangeKeyAckMsg->setPartialRes(tmpReduce);
			} else {
				//Load full result
				loadSavedResult();
				std::cout << "Sending partial result: ";
				printingVector(tmpResult);
				checkChangeKeyAckMsg->setPartialVectorArraySize(tmpResult.size());
				for(int i = 0; i < tmpResult.size(); i++) {
					checkChangeKeyAckMsg->setPartialVector(i, tmpResult[i]);
				}
				tmpResult.clear();
			}
			checkChangeKeyAckMsg->setChangeKeyReceived(changeKeyReceived);
			checkChangeKeyAckMsg->setChangeKeySent(changeKeySent);
			send(checkChangeKeyAckMsg, "out", 0);
			EV<<"\nChangeKey checked at worker: "<<workerId<<"\n\n";
			idle = true;			
			return;
		}

		// Logging code (IGNORE)

		begin_batch = simTime();
		begin_op = simTime();

		// End of Logging code

		double delay = calculateDelay("load");
		scheduleAt(simTime() + delay, nextStepMsg);
		return;
	}

	if(!data[currentScheduleStep].empty()){
		int value = data[currentScheduleStep].front();
		data[currentScheduleStep].pop_front();

		bool result = applyOperation(value);

		if(failed) return;

		EV << "Result: " << value << "\n";

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

		double delay = calculateDelay(schedule[currentScheduleStep]);
		EV<<"Scheduling next step\n";
		scheduleAt(simTime()+delay, nextStepMsg);
		EV<<"Scheduled next step\n";
	} else {
		EV << "Empty map, finished current step\n\n";
		std::cout << "Worker " << workerId << " finished step " << currentScheduleStep << " - New step data: \n";
		
		// Logging code (IGNORE)

		simtime_t end_op = simTime();
		simtime_t duration = end_op - begin_op;

		if(duration > 0) {
			per_op_exec_times[getParentOperation(schedule[currentScheduleStep])].push_back(duration);
		}
		begin_op = simTime(); // Reset timer for next operation		

		// End of Logging code

		currentScheduleStep++;

		if(reduceLast && currentScheduleStep == schedule.size() - 1) {
			EV << "Entering reduce\n\n";
			//std::cout << "Worker " << workerId << " reducing: ";
			processReduce();
			//std::cout<<"Returning after process reduce\n";
			return;
		}
		processStep();
	}
}

void Worker::processReduce(){
	int batchRes = reduce({data[currentScheduleStep].begin(), data[currentScheduleStep].end()});

	std::cout << tmpReduce << " + " << batchRes << " = " << (tmpReduce + batchRes) << "\n";

	tmpReduce = tmpReduce + batchRes;

	data[currentScheduleStep].clear();

	double delay = calculateDelay(schedule[currentScheduleStep]);
	std::cout << "Reduce delay: " << delay << "\n";
	currentScheduleStep++;

	scheduleAt(simTime()+delay, nextStepMsg);
}

void Worker::loadNextBatch(){
	data.clear();

	if(localBatch) {
		previousLocal = true;
		std::cout << "Worker " << workerId << " - Loading local:\n";
		std::vector<int> batch = loader->loadBatch();
		if(batch.empty()){
			finishedLocalElaboration = true;
			localBatch = false;
			//std::cout << "Finished local, switching to ck" << "\n";
		} else {
        	data[0].insert(data[0].end(), batch.begin(), batch.end());
		}
	} else {
		previousLocal = false;
		EV << "Loading CK...\n";
		std::map<int, std::vector<int>> ckBatch = insertManager->getBatch();

		if(ckBatch.empty()){
			finishedPartialCK = true;
			EV << "CK data empty" << "\n";
		} else {
			EV << "\n\nCK not empty";
			for(int i=0; i < ckBatch.size(); i++) {
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
		std::cout << "Map result: " << res << "\n";
		value = res;

		return true;
	} else if(operation == "lt" || operation == "gt" || operation == "le" || operation == "ge") {
		return filter(operation, parameter, value);

	} else if(operation == "changekey") {
		int newKey = changeKey(value, changeKeyProbability);
		//std::cout<<"New key: "<<newKey<<"\n";
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
	double delay = lognormal(lognormal_params[getParentOperation(operation)].first, lognormal_params[getParentOperation(operation)].second);
	std::cout << "OP:" << operation << "- delay: " << delay << "\n";
	return delay;
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
	int ckValue = data % (static_cast<int>((1/(probability)) * numWorkers));
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
	
	std::cout << "Worker " << workerId << " loaded reduce: " << tmpReduce << " [CRASH RECOVERY]" << "\n";
}

void Worker::loadSavedResult() {
	std::string res_filename = "Data/Worker_" + std::to_string(workerId) + "/result.csv";
	std::ifstream res_file(res_filename, std::ios::binary);
	std::string line;

	if(res_file.is_open()){
		while (std::getline(res_file, line)) {
            std::istringstream iss(line);
            std::string part;

            while (std::getline(iss, part)) {
                tmpResult.push_back(std::stoi(part));
            }
        }
        res_file.close();
	}
}

bool Worker::failureDetection(){
	int res = bernoulli(failureProbability);
	if(res){
		return true;
	}
	return false;

}

void Worker::loadChangeKeyData(){

	std::string ck_filename = "Data/Worker_" + std::to_string(workerId) + "/CK_sent_received.csv";
	std::ifstream ck_file(ck_filename, std::ios::binary);
	std::string line;
	
	if(ck_file.is_open()){
		while (std::getline(ck_file, line)) {
			std::istringstream iss(line);
			std::string part;
			std::vector<int> parts;

			while (std::getline(iss, part, ',')) {
				parts.push_back(std::stoi(part));
				std::cout<<"Part: "<<part<<"\n";
			}

			if (parts.size() == 2) {
				changeKeySent = parts[0];
				changeKeyReceived = parts[1];
				std::cout<<"Worker "<<workerId<<" -> LOADING: changeKeySent: "<<changeKeySent<<", changeKeyReceived: "<<changeKeyReceived<<"\n";
			}else{
				std::cout<<"Error in loading change key data\n";
			}
		}
	}
	ck_file.close();

	std::string ck_counter = "Data/Worker_" + std::to_string(workerId) + "/CK_counter.csv";
	std::ifstream ck_counterFile(ck_counter, std::ios::binary);
	std::string counterLine;

	if(ck_counterFile.is_open()){
		while(std::getline(ck_counterFile, counterLine)){
			std::istringstream iss(counterLine);
			std::string part;
			std::vector<int> parts;

			while(std::getline(iss, part, ',')){
				parts.push_back(std::stoi(part));
			}

			if(parts.size() == 2){
				changeKeyCtr = parts[0];
				localBatch = parts[1] == 1 ? true : false;
				previousLocal = localBatch;
				std::cout<<"Worker "<<workerId<<" -> LOADING: ChangeKeyCtr: "<<changeKeyCtr<<", localBatch: "<<localBatch<<"\n";
			}else{
				std::cout<<"Error in loading change key counter\n";
			}
		}
	}
	ck_counterFile.close();
}

void Worker::deallocatingMemory(){
	std::cout << "Worker " << workerId << " failing..." << "\n";

	// Logging code (IGNORE)

	simtime_t end_op = simTime();
	simtime_t end_batch = simTime();

	simtime_t op_duration = end_op - begin_op;
	simtime_t batch_duration = end_batch - begin_batch;

	if(op_duration > 0){
		per_op_exec_times[getParentOperation(schedule[currentScheduleStep])].push_back(op_duration);
	}
	
	if(batch_duration > 0){
		per_schedule_exec_times.push_back(batch_duration);
	}

	// End of Logging code

	failed = true;

	//printScheduledData(debugData);
	std::cout << "\n";

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
	idle = false;

	// ChangeKey protocol
	changeKeySent = 0;
	changeKeyReceived = 0;

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

	if(insertTimeoutMsg == nullptr) {
		cMessage* timeoutMsg = new cMessage(("Timeout-" + std::to_string(changeKeyCtr)).c_str());
		timeoutMsg->setContextPointer(new int(changeKeyCtr));
		insertTimeoutMsg = timeoutMsg;
	}

	scheduleAt(simTime() + insertTimeout, insertTimeoutMsg);

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

	//std::cout << "Worker " << workerId << " persisting: " << reducedValue << "\n";

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
	std::string fileName = folder + "CK_counter.csv";

	int batchType = previousLocal ? 1 : 0;

	std::ofstream result_file(fileName);
	if(result_file.is_open()){
		EV << "Opened CK file\n";
		std::cout<<"Worker "<<workerId<<" -> PERSISTING: changeKeyCtr: "<<changeKeyCtr<<", localBatch: "<<localBatch<<", batchType: "<<batchType<<"\n";
		result_file<<changeKeyCtr<<","<<batchType;

		result_file.close();
		
	}else{
		EV << "Can't open file: " << fileName << "\n";
	}
}

void Worker::persistCKSentReceived(){
	std::string folder = "Data/Worker_" + std::to_string(workerId) + "/";
	std::string fileName = folder + "CK_sent_received.csv";

	std::ofstream result_file(fileName);
	if(result_file.is_open()){
		EV << "Opened CK file\n";
		std::cout<<"Worker "<<workerId<<" -> PERSISTING: changeKeySent: "<<changeKeySent<<", changeKeyReceived: "<<changeKeyReceived<<"\n";
		result_file<<changeKeySent<<","<<changeKeyReceived;

		result_file.close();
		
	}else{
		EV << "Can't open file: " << fileName << "\n";
	}
}

void Worker::convertParameters() {
	// MAP
	lognormal_params["map"] = calculateDistributionParams(MAP_EXEC_TIME_AVG, MAP_EXEC_TIME_STD);

	// FILTER
	lognormal_params["filter"] = calculateDistributionParams(FILTER_EXEC_TIME_AVG, FILTER_EXEC_TIME_STD);

	// CK
	lognormal_params["changekey"] = calculateDistributionParams(CHANGEKEY_EXEC_TIME_AVG, CHANGEKEY_EXEC_TIME_STD);

	// REDUCE
	lognormal_params["reduce"] = calculateDistributionParams(REDUCE_EXEC_TIME_AVG, REDUCE_EXEC_TIME_STD);

	// PING
	lognormal_params["ping"] = calculateDistributionParams(PING_DELAY_AVG, PING_DELAY_STD);

	// RESTART
	lognormal_params["restart"] = calculateDistributionParams(RESTART_DELAY_AVG, RESTART_DELAY_STD);

	// FINISH
	lognormal_params["finish"] = calculateDistributionParams(FINISH_EXEC_DELAY_AVG, FINISH_EXEC_DELAY_STD);

	// LOAD
	lognormal_params["load"] = calculateDistributionParams(BATCH_LOAD_TIME_AVG, BATCH_LOAD_TIME_STD);
}

/*
* Given mean, stddev, calculate the corresponding parameters of a lognormal distribution.
* We need this function because the parameters of a lognormal don't directly define its mean/stddev but they define
* the underlying normal distrbution's parameters.
*/
std::pair<double, double> Worker::calculateDistributionParams(double m, double s) {
	double mu = log((m * m) / sqrt(s * s + m * m));
    double sigma = sqrt(log((s * s) / (m * m) + 1));

    return std::make_pair(mu, sigma);
}

void Worker::printingVector(std::vector<int> vector){
    for(int i=0; i<vector.size(); i++){
        std::cout<<vector[i]<<" ";
    }
    std::cout << "\n";
}

void Worker::printScheduledData(std::map<int, std::deque<int>> data){
	for(int i = 0; i<data.size(); i++){
		if(data[i].size() > 0) std::cout << "Step " << i << ": ";

		for(int j = 0; j < data[i].size(); j++){
			std::cout << data[i][j] << " ";
		}

		if(data[i].size() > 0) std::cout << "\n";
	}
}

void Worker::printDataInsertMessage(DataInsertMessage* msg, bool recv){
	if(recv){
		std::cout << "Worker " << workerId << " received DataInsert:\n";
		std::cout << "Sender: " << getInboundWorkerID(msg->getArrivalGate()->getIndex()) << "\n";
	}
	std::cout << "Dest ID: " << msg->getDestID() << "\n";
	std::cout << "Req ID: " << msg->getReqID() << "\n";
	std::cout << "Data: " << msg->getData() << "\n";
	std::cout << "Schedule step: " << msg->getScheduleStep() << "\n";
	std::cout << "Ack: " << msg->getAck() << "\n";
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

std::string Worker::getParentOperation(const std::string& op) {
	if(op == "add" || op == "sub" || op == "mul" || op == "div") {
		return "map";
	}
	if(op == "lt" || op == "gt" || op == "le" || op == "ge") {
		return "filter";
	}
	return op;
}

void Worker::logSimData() {
    // Write duration to a file

    std::string logDir = "./Logs";
    fs::path parentDir = fs::path(logDir) / fs::path(EXPERIMENT_NAME);
	int maxId = -1;

    // Ensure the parent directory exists
	if(!fs::exists(logDir)) {
		fs::create_directory(logDir);
	}

    if (!fs::exists(parentDir)) {
        fs::create_directory(parentDir);
    }

    // Scan existing folders to find the max ID
    for (const auto& entry : fs::directory_iterator(parentDir)) {
        if (entry.is_directory()) {
            std::string folderName = entry.path().filename().string();
            try {
                int folderId = std::stoi(folderName);
                maxId = std::max(maxId, folderId);
            } catch (const std::invalid_argument& e) {
                // Not a number-named folder, ignore
            }
        }
    }

    // Determine the folder name for the new simulation
    int newFolderId = maxId;
    fs::path newFolderPath = parentDir / std::to_string(newFolderId);

    // Assuming folder was already created by leader

    // First: Save per batch, full schedule elaboration times
    fileName = newFolderPath.string() + "/WRK_" + std::to_string(workerId) + "_fullSched_" + std::to_string(batchSize) + ".log"; 
    std::ofstream outFile_fs(fileName);
    if (outFile_fs.is_open()) {
        for(const auto& entry : per_schedule_exec_times) {
        	outFile_fs << entry << "\n";
        }
        outFile_fs.close();
    } else {
        EV << "Error opening file for writing simulation duration.\n";
    }

    // Second: Save per batch, per operation elaboration times
    for(const auto& op : per_op_exec_times) {
    	std::string opName = op.first;
    	std::vector<simtime_t> execTimes = op.second;

    	fileName = newFolderPath.string() + "/WRK_" + std::to_string(workerId) + "_" + opName +"_" + std::to_string(batchSize) + ".log"; 
    	std::ofstream outFile_op(fileName);
	    if (outFile_op.is_open()) {
	        for(const auto& entry : execTimes) {
	        	outFile_op << entry << "\n";
	        }
	        outFile_op.close();
	    } else {
	        EV << "Error opening file for writing simulation duration.\n";
	    }
    }
}
