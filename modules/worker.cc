#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <omnetpp.h>
#include <algorithm>

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
#include "changeKeySelf_m.h"
#include "localExEvent_m.h"
#include "pingResEvent_m.h"

#define LEADER_PORT 0
#define BATCH_EXEC_TIME 2
#define FINISH_EXEC_DELAY 2
#define PING_DELAY_AVG 0
#define PING_DELAY_VAR 1
#define RESTART_DELAY_AVG 5
#define RESTART_DELAY_VAR 2

using namespace omnetpp;

class Worker : public cSimpleModule{
private:
	std::vector<int> data;
	std::vector<std::string> schedule;
    std::vector<int> parameters;
	std::map<int, DataInsertMessage*> unstableMessages;
	std::map<int, cMessage*> timeouts;
	std::string folder;
	std::string fileName;
	std::string fileProgressName;
	BatchLoader* loader;
	InsertManager* insertManager;
	int batchSize;
	float failureProbability;
	int workerId;
	float timeout;
	int iterations;
	int currentBatch;
	bool failed;
	bool localFinish;
	int changeKeyCtr;
	int numWorkers;
	float changeKeyProbability;

	//cMessage *localExEvent;
	//cMessage *changeKeyExEvent;
	//cMessage *pingResEvent;
	//PingMessage *replyPingMsg;
protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
	void handlePingMessage(cMessage *msg);
	void handleSetupMessage(SetupMessage *msg);
	void handleScheduleMessage(ScheduleMessage *msg);
	void handleLocalExEvent(cMessage *msg);
	void handleDataInsertMessage(DataInsertMessage *msg);
	void handleChangeKeyExEvent(cMessage *msg);
	void handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg);
	void handleFinishSimMessage(FinishSimMessage *msg);
	void handleRestartMessage(RestartMessage *msg);
	void initializeDataModules();
	std::vector<int> getRemainingParameters(int scheduleStep);
	std::vector<std::string> getRemainingSchedule(int scheduleStep);
	void applySchedule(std::vector<std::string> schedule, std::vector<int> parameters, std::vector<int> currentData);
	int map(std::string operation, int parameter, int data);
	std::vector<int> filter(std::string operation, std::vector<int> discard, int data, int parameter);
	int changeKey(int data, float probability);
	int reduce(int data, int reduce, int iteration);
	bool failureDetection();
	void deallocatingMemory();
	std::vector<int> discardingData(std::vector<int> discard, std::vector<int> data);
	void sendData(int newKey, int value, int scheduleStep);
	int getWorkerGate(int destID);
	int getInboundWorkerID(int gateIndex);
	void persistingResult(std::vector<int> result);
	void persistingReduce(int reduce);
	void printingVector(std::vector<int> vector);
	
};

Define_Module(Worker);

void Worker::initialize(){
	// TODO
	batchSize = par("batchSize").intValue();
	failureProbability = (par("failureProbability").intValue()) / 1000.0;
	numWorkers = par("numWorkers").intValue();
	failed = false;
	localFinish = false;
	data.resize(batchSize);
	timeout=5;
	changeKeyCtr = 0;
	changeKeyProbability = 0.1;
	currentBatch = 0;
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
				int destWorker = insertMsg->second->getDestID();
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

	PingMessage *pingMsg = dynamic_cast<PingMessage *>(msg);
	if(pingMsg != nullptr)
	{
		//replyPingMsg = pingMsg;
		// Generate random delay
		float delay = lognormal(PING_DELAY_AVG, PING_DELAY_VAR); // Log-normal to have always positive increments
		PingResEventMessage* pingResEvent = new PingResEventMessage();
		// Schedule response event
        scheduleAt(simTime() + delay , pingResEvent);
		return;
	}

	RestartMessage *restartMsg = dynamic_cast<RestartMessage *>(msg);
    if(restartMsg != nullptr) {
    	// Successfully cast to RestartMessage, handle it
    	handleRestartMessage(restartMsg);
    	return;
    }

	SetupMessage *setupMsg = dynamic_cast<SetupMessage *>(msg);
    if(setupMsg != nullptr){
        // Successfully cast to SetupMessage, handle it
        handleSetupMessage(setupMsg);
        return;
    }

	ScheduleMessage *scheduleMsg = dynamic_cast<ScheduleMessage *>(msg);
    if (scheduleMsg != nullptr) {
        // Successfully cast to ScheduleMessage, handle it
        handleScheduleMessage(scheduleMsg);
        return;
    }
	LocalExEventMessage *localExEvent = dynamic_cast<LocalExEventMessage *>(msg);
	if(localExEvent != nullptr){
		EV<<"\nLocalExEvent at worker: "<<workerId<<"\n\n";
		handleLocalExEvent(msg);
		return;
	}
	ChangeKeySelfMessage *changeKeySelf = dynamic_cast<ChangeKeySelfMessage *>(msg);
	if(changeKeySelf != nullptr){
		EV<<"\nChangeKeyExEvent at worker: "<<workerId<<"\n\n";
		handleChangeKeyExEvent(msg);
		return;
	}
	PingResEventMessage *pingResEvent = dynamic_cast<PingResEventMessage *>(msg);
	if(pingResEvent != nullptr)
	{
    	// EV<<"\nPingResEvent at worker: "<<workerId<<"\n\n";
    	handlePingMessage(msg);
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
		localFinish = true;
		EV<<"Start executing the remain schedule for the latecomers change key data\n";
		handleFinishLocalElaborationMessage(finishLocalMsg);
		return;
	}

    FinishSimMessage *finishSimMsg = dynamic_cast<FinishSimMessage *>(msg);
	if(finishSimMsg != nullptr)
	{
		handleFinishSimMessage(finishSimMsg);
		return;
	}
}

void Worker::handlePingMessage(cMessage *msg)
{
	EV << "WORKER "<< workerId << " -> Ping message received" << std::endl;

	PingMessage *replyPingMsg = new PingMessage();
	replyPingMsg -> setWorkerId(workerId);
	send(replyPingMsg, "out", LEADER_PORT);
	delete msg;

	return;
}

void Worker::handleRestartMessage(RestartMessage *msg){
	if(!failed)
	{
		EV << "Worker " << workerId << " received a RestartMessage, but has not failed: Restarting..." << std::endl;
		deallocatingMemory();
	}
	failed = false;
	workerId = msg->getWorkerID();
	initializeDataModules();
	// Re-Initialized worker and data modules, now wait for schedule and re-start processing
	
	float delay = lognormal(RESTART_DELAY_AVG, RESTART_DELAY_VAR);
	LocalExEventMessage *localExEvent = new LocalExEventMessage();
	scheduleAt(simTime() + delay, localExEvent);
	
	delete msg;
	return;
}

void Worker::handleSetupMessage(SetupMessage *msg){
	workerId = msg->getAssigned_id();

	if(par("id").intValue() == -1){ // If the ID was not previously set
		par("id") = workerId;
	}

	int dataSize = msg->getDataArraySize();
	iterations = (dataSize / batchSize) +1;
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

	delete msg;
}

void Worker::handleScheduleMessage(ScheduleMessage *msg){
	
    int scheduleSize = msg->getScheduleArraySize();

    for(int i=0; i<scheduleSize; i++){
        schedule.push_back(msg->getSchedule(i)) ;
		parameters.push_back(msg->getParameters(i));
    }
    LocalExEventMessage *localExEvent = new LocalExEventMessage();
	scheduleAt(simTime(), localExEvent);
	delete msg;
}

void Worker::handleLocalExEvent(cMessage *msg){
	delete msg;

	if(currentBatch < iterations){
		data = loader->loadBatch();	
		applySchedule(schedule, parameters, data);
		if(failed){
			return;
		}
		LocalExEventMessage *localExEvent = new LocalExEventMessage();
        scheduleAt(simTime() + BATCH_EXEC_TIME , localExEvent);
		currentBatch++;
    }else{
		EV<<"\nSENDING FINISHED LOCAL ELABORATION WORKER: "<<workerId<<"\n\n";
		FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
		finishLocalMsg->setWorkerId(workerId);
		send(finishLocalMsg, "out", 0);	
	}
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
	} else {
		// Insert new data point into data vector
		int gateIndex = msg->getArrivalGate()->getIndex();
		int senderID = getInboundWorkerID(gateIndex);

		insertManager->insertValue(senderID, msg->getReqID(), msg->getScheduleStep(), msg->getData());

		// Send back ACK after insertion
		DataInsertMessage* insertMsg = new DataInsertMessage();
		insertMsg->setReqID(msg->getReqID());
		insertMsg->setAck(true);

		send(insertMsg, "out", gateIndex);
	}
	ChangeKeySelfMessage* changeKeySelf = new ChangeKeySelfMessage();
	scheduleAt(simTime(), changeKeySelf);
	delete msg;
}

void Worker::handleChangeKeyExEvent(cMessage *msg){
	delete msg;

	std::map<int, std::vector<int>> changeKeyData;
	std::vector<std::string> remainingSchedule;
	std::vector<int> remainingParameters;

	changeKeyData = insertManager->getBatch();

	if(!changeKeyData.empty()){
		for(auto& pair : changeKeyData){
			EV<<"Change key batch: \n";
			printingVector(pair.second);
			EV<<"\n";
			if(schedule.size() == pair.first+1){
				EV<<"Changekey is the last operation, persisting the values\n";
				persistingResult(pair.second);
			}else{
				remainingSchedule = getRemainingSchedule(pair.first+1);
				remainingParameters = getRemainingParameters(pair.first+1);
				
				applySchedule(remainingSchedule, remainingParameters, pair.second);
			}
			if(failed){
				return;
			}
		}
		ChangeKeySelfMessage* changeKeySelf = new ChangeKeySelfMessage();
		scheduleAt(simTime() + BATCH_EXEC_TIME , changeKeySelf);
	}else{
		if(localFinish){
			CheckChangeKeyAckMessage* checkChangeKeyAckMsg = new CheckChangeKeyAckMessage();
			checkChangeKeyAckMsg->setWorkerId(workerId);
			send(checkChangeKeyAckMsg, "out", 0);
			EV<<"\nChangeKey checked at worker: "<<workerId<<"\n\n";
		}
	}
}

void Worker::handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg){
	ChangeKeySelfMessage* changeKeySelf = new ChangeKeySelfMessage();
	scheduleAt(simTime() + FINISH_EXEC_DELAY , changeKeySelf);
	delete msg;
}

void Worker::handleFinishSimMessage(FinishSimMessage *msg)
{
	EV << "Application finished at worker: " << workerId << std::endl;
	if(msg -> getWorkerId() == 3)
	{
	    endSimulation();
	}
	delete msg;
}

void Worker::initializeDataModules() {
	// Instantiate a BatchLoader
	fileProgressName = folder + "progress.txt";
	loader = new BatchLoader(fileName, fileProgressName, batchSize);

	// Instantiate an InsertManager
	std::string insertFilename = folder + "inserted.csv";
	std::string requestFilename = folder + "requests_log.csv";
	insertManager = new InsertManager(insertFilename, requestFilename, batchSize);
}

std::vector<std::string> Worker::getRemainingSchedule(int scheduleStep){
	std::vector<std::string> remainingSchedule;
	for(int i=scheduleStep; i<schedule.size(); i++){
		remainingSchedule.push_back(schedule[i]);
	}
	return remainingSchedule;
}

std::vector<int> Worker::getRemainingParameters(int scheduleStep){
	std::vector<int> remainingParameters;
	for(int i=scheduleStep; i<parameters.size(); i++){
		remainingParameters.push_back(parameters[i]);
	}
	return remainingParameters;
}

void Worker::applySchedule(std::vector<std::string> localSchedule, std::vector<int> localParameters, std::vector<int> currentData) {
    int scheduleSize = localSchedule.size();
    std::vector<int> discard;
	int reducedValue = 0;
    for (int i = 0; i < scheduleSize; i++) {
        for (int j = 0; j < currentData.size(); j++) {
			if(failureDetection()){
				failed = true;
				EV<<"FAILURE DETECTED AT WORKER: "<<workerId<<", deallocating memory\n";
				deallocatingMemory();
				return;
			}
            if (localSchedule[i] == "add" || localSchedule[i] == "sub" || localSchedule[i] == "mul" || localSchedule[i] == "div") {
				EV<<"Mapping: "<<currentData[j]<<" "<<localSchedule[i]<<" "<<localParameters[i];
                currentData[j] = map(localSchedule[i], localParameters[i], currentData[j]);
				EV<<" = "<<currentData[j]<<"  ";
            } else if (localSchedule[i] == "lt" || localSchedule[i] == "gt" || localSchedule[i] == "le" || localSchedule[i] == "ge") {
                discard = filter(localSchedule[i], discard, currentData[j], localParameters[i]);
                if (j == currentData.size() - 1) {
					EV<<"Filtering: ";
					printingVector(currentData);
					EV<<localSchedule[i]<<" "<<localParameters[i];
                    currentData = discardingData(discard, currentData);
					EV<<" = ";
					printingVector(currentData);
					discard.clear();
                } 
            } else if (localSchedule[i] == "reduce") {
				reducedValue = reduce(currentData[j], reducedValue, j);
				EV<<"Reduced value: "<<reducedValue<<"\n";
            } else if (localSchedule[i] == "changekey") {
            	int newKey = changeKey(currentData[j], changeKeyProbability);
                if(newKey != -1) {
                	EV << "Changing Key: " << workerId << " -> " << newKey << " for value: "<<currentData[j]<<"\n";
                	sendData(newKey, currentData[j], i); // 'i' == Schedule step
                }
            } else {
                EV << "Invalid operation: " << localSchedule[i] << "\n";
            }
        }
		if(currentData.size() > 0 && localSchedule[i] != "changekey"){
			EV<<"\n";
		}
		
    }
	if(schedule[scheduleSize-1] == "reduce" && currentData.size() > 0){
		persistingReduce(reducedValue);
	}else{
		persistingResult(currentData);
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

int Worker::changeKey(int data, float probability){
	int ckValue = data % (static_cast<int>(1/(probability))*numWorkers);
	if(ckValue == workerId || ckValue >= numWorkers || ckValue < 0) {
		return -1;
	}
	return ckValue;
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
	failureProbability = 0;
	workerId = 0;
	timeout = 0;
	iterations = 0;
	delete loader;
}

std::vector<int> Worker::discardingData(std::vector<int> discard, std::vector<int> data) {
    for (int i = discard.size() - 1; i >= 0; i--) {
        if (discard[i] == 1) {
            data.erase(data.begin() + i);
        }
    }
    return data;
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

	scheduleAt(simTime()+timeout, timeoutMsg);

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
