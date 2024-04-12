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

#define EXPERIMENT_NAME "Increasing_Batch_Size"

#define LEADER_PORT 0

// ----- Fast operations -----

#define PING_DELAY_AVG 0.0001
#define PING_DELAY_STD 0.001

#define FINISH_EXEC_DELAY_AVG 0.0002
#define FINISH_EXEC_DELAY_STD 0.001

#define MAP_EXEC_TIME_AVG 0.003
#define MAP_EXEC_TIME_STD 0.001

#define FILTER_EXEC_TIME_AVG 0.003
#define FILTER_EXEC_TIME_STD 0.001

// (Fast when local)
#define CHANGEKEY_EXEC_TIME_AVG 0.003
#define CHANGEKEY_EXEC_TIME_STD 0.001

// ----- Medium-duration operations -----

#define REDUCE_EXEC_TIME_AVG 0.03
#define REDUCE_EXEC_TIME_STD 0.01

// ----- Medium to slow-duration operations -----

#define BATCH_LOAD_TIME_AVG 0.3
#define BATCH_LOAD_TIME_STD 0.1

// ----- Slow operations -----
#define RESTART_DELAY_AVG 1
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
	bool failureDetection(int factor = 1);
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

/*
* Initializes base worker information, sets parameters for:
*	- Failure probability (x/1000)
*	- ChangeKey probability
*	- DataInsert timeout duration
*	- Conversion of parameters for lognormal distribution (Check function for more details)
*/
void Worker::initialize(){
	// Initializing variables to avoid segfaults
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
	failureProbability = (par("failureProbability").doubleValue()) / 1000.0;
	numWorkers = par("numWorkers").intValue();

	changeKeyProbability = 0.4;
	insertTimeout = 0.5; //500 ms
	localBatch = true;
	failed = false;

	convertParameters(); //Conversion of delay parameters for the lognormal distribution

	nextStepMsg = new NextStepMessage("NextStep");
	pingResEvent = new PingResMessage("PingRes");
	unstableMessage = new DataInsertMessage();
	insertTimeoutMsg = new cMessage("Timeout");
}

/*
* Deallocates worker variables, prints debug information and persists logged simulation data.
*/
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

/*
* Handles incoming messages/self-messages by casting to their respective type, and
* calling the function responsible for the corresponding task.
*/
void Worker::handleMessage(cMessage *msg){

	// Segment for data processing self-message
	if(msg == nextStepMsg) {
		processStep();
		return;
	}

    // Segment for ping response self-message
    if(msg == pingResEvent){
    	handlePingMessage(msg);
    	return;
    }

	/* 
	*  Timeout Message segment:
	*  Try re-sending an unstable DataInsert message and re-start a timeout.
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

	/*
	*	Ping Message segment:
	*	Schedule a delayed response to the ping message
	*/
	PingMessage *pingMsg = dynamic_cast<PingMessage *>(msg);
	if(pingMsg != nullptr){
		delete msg;		
		// If the worker has failed, do not reply
		if(failed){
			return;
		}
		
		// 
		if(pingResEvent != nullptr && pingResEvent->isScheduled()){
			cancelEvent(pingResEvent);
		}

		// Generate random delay
		double delay = calculateDelay("ping"); // Log-normal to have always positive increments
		// Schedule response event
        scheduleAt(simTime() + delay , pingResEvent);
		return;
	}

	// Setup Message segment
	SetupMessage *setupMsg = dynamic_cast<SetupMessage *>(msg);
    if(setupMsg != nullptr){
        // Successfully cast to SetupMessage, handle it
        handleSetupMessage(setupMsg);
    	delete msg;
        return;
    }

	// Schedule Message segment	
	ScheduleMessage *scheduleMsg = dynamic_cast<ScheduleMessage *>(msg);
    if (scheduleMsg != nullptr) {
        // Successfully cast to ScheduleMessage, handle it
        handleScheduleMessage(scheduleMsg);
    	delete msg;
        return;
    }
    
    // Data Insert Message segment (Insert/ACK)
    DataInsertMessage *dataInsertMsg = dynamic_cast<DataInsertMessage *>(msg);
    if(dataInsertMsg != nullptr){
    	//Successfully cast to DataInsertMessage, handle it
    	handleDataInsertMessage(dataInsertMsg);
    	return;
    }

	// FinishLocalElaboration Message segment (Check for local ChangeKeys)
	FinishLocalElaborationMessage *finishLocalMsg = dynamic_cast<FinishLocalElaborationMessage *>(msg);
	if(finishLocalMsg != nullptr){
		EV<<"Start executing the remain schedule for the latecomers change key data\n";
		handleFinishLocalElaborationMessage(finishLocalMsg);
    	delete msg;
		return;
	}

    // Finish Simulation message segment
    FinishSimMessage *finishSimMsg = dynamic_cast<FinishSimMessage *>(msg);
	if(finishSimMsg != nullptr){
		handleFinishSimMessage(finishSimMsg);
    	delete msg;
		return;
	}

	// Restart after failure message segment
	RestartMessage *restartMsg = dynamic_cast<RestartMessage *>(msg);
    if(restartMsg != nullptr) {
    	// Successfully cast to RestartMessage, handle it
    	handleRestartMessage(restartMsg);
    	delete msg;
    	return;
    }
}

/*
*	Function to reply to a ping message coming from the Leader node
*/
void Worker::handlePingMessage(cMessage *msg){
	if(failed) {
		return;
	}
	PingMessage *pingMsg = new PingMessage();
	pingMsg->setWorkerId(workerId);
	send(pingMsg, "out", LEADER_PORT);
	return;
}

/*
 * Handles the setup message received from the leader.
 * This function performs several tasks:
 *   - It sets the worker ID using the assigned ID from the message.
 *   - It initializes necessary data modules.
 *   - Data received with the setup message is persisted to a file.
 * 
 * Parameters:
 *   - msg: A pointer to the SetupMessage containing initialization data.
 */
void Worker::handleSetupMessage(SetupMessage *msg){
	workerId = msg->getAssigned_id(); // Set workerID from the message

	if(par("id").intValue() == -1){ // Set ID param
		par("id") = workerId;
	}

	int dataSize = msg->getDataArraySize(); // Get the number of data items

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

	// Initialize BatchLoader and InsertManager
	initializeDataModules();
}

/*
 * Handles the schedule message received from the leader.
 * This function performs several tasks:
 *   - Copy the schedule and parameters from the message
 *	 - Load the first batch of local data
 *	 - Persist initial info on type of batch
 *	 - Schedule the first nextStep message to begin elaboration
 * 
 * Parameters:
 *   - msg: A pointer to the ScheduleMessage containing schedule initialization data.
 */
void Worker::handleScheduleMessage(ScheduleMessage *msg){
	// Get size and copy operations and parameters
    int scheduleSize = msg->getScheduleArraySize();

    for(int i=0; i<scheduleSize; i++){
        schedule.push_back(msg->getSchedule(i)) ;
		parameters.push_back(msg->getParameters(i));
    }
    // Set helper flag
    reduceLast = (schedule.back() == "reduce");

    loadNextBatch(); // Load first batch
    
    persistCKCounter(); // Persist first batch info

    // Logging code (IGNORE)
    begin_elab = simTime();
    begin_batch = simTime();
    begin_op = simTime();
    // End of logging code

	// Schedule first message
	scheduleAt(simTime(), nextStepMsg);
}

/*
 * Handles a DataInsert message received from another Worker.
 * This function performs two tasks, based on the type of DataInsert (ACK/Insert):
 *  If the message is an ACK: 
 *	 - Cancel the scheduled timeout
 *	 - Increment changeKeySent and persist counters
 *	 - Schedule nextStep and unblock execution
 *
 *	If the message is an Insertion:
 *	 - Get information on the sender and pass it to InsertManager
 *	 - Reply with ACK
 *	 - Increment changeKeyReceived and persist counters
 * 
 * Parameters:
 *   - msg: A pointer to the DataInsertMessage containing a data point and info on the exchange.
 */
void Worker::handleDataInsertMessage(DataInsertMessage *msg){
	// Drop the message if the worker has failed
	if(failed){
		EV << "Received DataInsert - dropped" << "\n";
		delete msg;
		return;
	}
	// Check if it is an ACK or an insertion to me (workerID)
	if(msg->getAck()){
		// Cancel the timeout message
		if(insertTimeoutMsg != nullptr && insertTimeoutMsg->isScheduled()) {
			cancelEvent(insertTimeoutMsg);
		}
		// Delete clone of DataInsertMessage
		delete unstableMessage;
		
		// Unblock execution and re-schedule a nextStep
		if(nextStepMsg->isScheduled()) {
			cancelEvent(nextStepMsg);
		}

		scheduleAt(simTime(), nextStepMsg);
		waitingForInsert = false;
		
		// Increment sent counter and persist
		changeKeySent++;
		persistCKSentReceived();
	} else {
		// Handle data insertion
		// Get sender info from the arrival gate
		int gateIndex = msg->getArrivalGate()->getIndex();
		int senderID = getInboundWorkerID(gateIndex);
		
		// Try to insert this value into InsertManager
		insertManager->insertValue(senderID, msg->getReqID(), msg->getScheduleStep(), msg->getData());

		// Reset flag (If this new data was inserted, I need to elaborate it)
		finishedPartialCK = false;
		// Send back ACK after insertion
		DataInsertMessage* insertMsg = new DataInsertMessage();
		insertMsg->setReqID(msg->getReqID());
		insertMsg->setAck(true);

		send(insertMsg, "out", gateIndex);
		
		// Increment received counter and persist
		changeKeyReceived++;
		persistCKSentReceived();
	}
	delete msg;
}

/*
 * Handles a FinishLocalElaboration message received from the Leader.
 * This function sets the "checkChangeKeyReceived" flag to true and starts again the execution.
 * This is used to elaborate any received changeKey after this worker finished the previous batch, to
 * make sure all data is elaborated.
 * 
 * Parameters:
 *   - msg: A pointer to the FinishLocalElaborationMessage.
 */
void Worker::handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg){
	/*
	* Set checkChangeKeyReceived flag to true:
	* Used to recognize when to send the CheckChangeKeyACK message to the Leader
	*/
	checkChangeKeyReceived = true;
	// To continue execution
	finishedPartialCK = false;

	// If worker is waiting for a DataInsert to be ACKed, do not re-schedule a nextStep
	if(waitingForInsert && insertTimeoutMsg != nullptr && insertTimeoutMsg->isScheduled()) return;

	// Cancel any scheduled nextStep
	if(nextStepMsg != nullptr && nextStepMsg->isScheduled()) {
	cancelEvent(nextStepMsg);
	}
	
	// Logging
	if(idle) {
		begin_batch = simTime();
		begin_op = simTime();
	}
	// End of logging
	
	// Schedule a new nextStep with a small delay to account for the worker switching to the final phase of the elaboration.
	double delay = calculateDelay("finish");
	scheduleAt(simTime() + delay , nextStepMsg);
}

/*
 * Handles a RestartMessage received from the Leader.
 * This function performs several tasks:
 *	 - If the worker has not failed, but has failed to respond to a ping in time, it deallocates memory.
 *	 - Reset base worker information and data loading modules
 *	 - Copy schedule and parameter information
 *	 - Reload previous partial result (If the schedule ends with reduce)
 *	 - Reload changeKey counters
 *	 - Load one batch of data
 *	 - Schedule a nextStep with high delay to account for all these tasks
 * 
 * Parameters:
 *   - msg: A pointer to the RestartMessage.
 */
void Worker::handleRestartMessage(RestartMessage *msg){
	// If the worker has not failed, but didn't respond in time to a ping, it restarts.
	if(!failed){
		std::cout << "Worker " << workerId << " received a RestartMessage, but has not failed: Restarting..." << "\n";
		deallocatingMemory();
	}
	
	//Reload base worker information and data modules
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

	// Load ChangeKey counters and previous batch type information
	loadChangeKeyData();
	
	// Load one batch of data in memory
	loadNextBatch();

	// Cancel any pre-existing nextStep
	if(nextStepMsg != nullptr && nextStepMsg->isScheduled()) {
		cancelEvent(nextStepMsg);
	}

	// Calculate restart operation delay (high)
	double delay = calculateDelay("restart");

	// Logging
    begin_batch = simTime() + delay;
    begin_op = simTime() + delay;
	// End of logging
	
	// Schedule delayed nextStep
	scheduleAt(simTime() + delay, nextStepMsg);
	return;
}

/*
 * Handles a FinishSimMessage received from the Leader.
 * This message terminates the simulation.
 * 
 * Parameters:
 *   - msg: A pointer to the FinishSimMessage.
 */
void Worker::handleFinishSimMessage(FinishSimMessage *msg){
	EV<<"\nApplication finished at worker: "<<workerId<<"\n\n";
}

/*
 * Initializes the data modules needed to:
 *	 - Load batches of local data
 *	 - Load/Insert ChangeKey data
 */
void Worker::initializeDataModules() {
	// File for local data
	fileName = folder + "data.csv";

	// Instantiate a BatchLoader (For local data loading)
	fileProgressName = folder + "progress.txt";
	loader = new BatchLoader(fileName, fileProgressName, batchSize);
	
	// Instantiate an InsertManager
	std::string insertFilename = folder + "inserted.csv"; // File for inserted data
	std::string requestFilename = folder + "requests_log.csv"; // File to keep track of inserts from other workers
	std::string tempFilename = folder + "ck_batch.csv"; // Temp file to store previously loaded CK batch
	insertManager = new InsertManager(insertFilename, requestFilename, tempFilename, batchSize);
}
/*
 * This function performs several tasks related to the elaboration of data points:
 *	- Check if the current batch has been fully elaborated
 *		- Persist the result and load the next batch
 *		- Check if the local elaboration is finished and whether to send a 
 *		  FinishElaboration/CheckChangeKeyACK to the Leader.
 *		- Reschedule a nextStep with a delay for loading a batch of data.
 *	- Take one data point from the current schedule step and process the current operation
 *		- Decide between moving the data point to the next step, or dropping it (filtered/changed key)
 *		- Schedule nextStep with delay based on current operation (map/filter/changekey)
 *	- If data for current step is empty
 *		- Increment step
 *		- If we are at the last step and it is a reduce, move to the processReduce() function
 *		- Else, recursively call this function.
 */
void Worker::processStep()
{
	// If the worker has failed, do not do anything
	if(failed)
	{
	    EV << "Worker failed!\n";
	    return;
	}

	// If the batch is finished
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

		// Check whether to persist the reduce, or to just append the current result to the file
		if(reduceLast) {
			persistingReduce(tmpReduce);
		} else {
			// If a schedule has a ChangeKey at the last step, handle data points at schedule.size() + 1
			if(data.size() == (schedule.size() + 1) && !data[schedule.size()].empty()) {
				for(const auto& value : data[schedule.size()]) {
					tmpResult.push_back(value); // Push points in the result
				}
				data[schedule.size()].clear(); // Clear after elaborating
			}
			// Persist to file (append)
			persistingResult(tmpResult);
			// Clear tmp vector
			tmpResult.clear();
		} 

		// Save the progress in the elaboration of data
		if(previousLocal) {
			loader->saveProgress(); // Persists current lines read
		} else {
			insertManager->persistData(); // Clears tmp file
		}

		// Attempt to load another batch of data, if the elaboration is not finished (local/changekey)
		while(isScheduleEmpty() && (!finishedLocalElaboration || !finishedPartialCK)){
			loadNextBatch();
		}

		// Persist changeKeyCtr, changeKeySent, changeKeyReceived before next batch
		persistCKCounter();
		
		EV<<"Status - Worker " << workerId << " - FinishedLocal: " << finishedLocalElaboration << " - FinishedCK: " << finishedPartialCK << " - CheckCKReceived: " << checkChangeKeyReceived << "\n";
		
		// If the worker has finished both local and ChangeKey data (for now), send a FinishLocalElaboration message to the leader
		if(finishedLocalElaboration && finishedPartialCK && !finishNoticeSent) {
			EV<<"\nSENDING FINISHED LOCAL ELABORATION WORKER: "<<workerId<<"\n\n";
			FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
			finishLocalMsg->setWorkerId(workerId);
			finishLocalMsg->setChangeKeyReceived(changeKeyReceived);
			finishLocalMsg->setChangeKeySent(changeKeySent);
			send(finishLocalMsg, "out", 0);	
			finishNoticeSent = true;
		}

		// If the worker has sent the finish notice, and has finished all ChangeKey data, it can idle until:
		//	a) Receives more ChangeKey data
		//	b) The leader asks to check ChangeKey data
		if(finishNoticeSent && finishedPartialCK && !checkChangeKeyReceived) {
			std::cout<<"Worker " << workerId << " - Temporarily finished elaborating ChangeKeys - Status: Idle\n\n";
			idle = true;
			return;
		}

		// If the worker has sent the finish notice, finished ChangeKey data, and has received a FinishLocal from the leader
		// It must reply with its current partial result, for the leader to evaluate termination conditions
		if(finishNoticeSent && finishedPartialCK && checkChangeKeyReceived) {
			CheckChangeKeyAckMessage* checkChangeKeyAckMsg = new CheckChangeKeyAckMessage();
			checkChangeKeyAckMsg->setWorkerId(workerId);

			// Insert current partial result in the message
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
			// Set information on ChangeKeys received and sent
			checkChangeKeyAckMsg->setChangeKeyReceived(changeKeyReceived);
			checkChangeKeyAckMsg->setChangeKeySent(changeKeySent);
			
			// Send the message and idle
			send(checkChangeKeyAckMsg, "out", 0);
			EV<<"\nChangeKey checked at worker: "<<workerId<<"\n\n";
			idle = true;			
			return;
		}

		// Logging code (IGNORE)

		begin_batch = simTime();
		begin_op = simTime();

		// End of Logging code

		// Schedule a nextStep accounting for batch loading delay (mid-high delay)
		double delay = calculateDelay("load");
		scheduleAt(simTime() + delay, nextStepMsg);
		return;
	}

	// If there are data to be elaborated in the current schedule step
	if(!data[currentScheduleStep].empty()){
		// Take the first data point from the deque
		int value = data[currentScheduleStep].front();
		data[currentScheduleStep].pop_front();

		// Apply the current operation (Map/Filter/ChangeKey) to the extracted data point
		bool result = applyOperation(value);

		// If the operation made the worker crash, return
		if(failed) return;

		EV << "Result: " << value << "\n";

		/*
		* Decide what to do with the resuting data point:
		*	a) If there are more steps in the schedule, move it to the next step's queue
		*	b) If we are at the end of the schedule, and don't have a reduce, push it to the tmpResult vector
		*
		* Note: The 'result' flag is set to true if the data point must continue to be processed by this worker
		* Else, if it is filtered out, or its key changes, it is set to false
		*/
		if(result){
			if(currentScheduleStep + 1 < schedule.size()){
				data[currentScheduleStep + 1].push_back(value);
			} else if(!reduceLast){
				tmpResult.push_back(value);
			}
		}

		// If the current operation was a ChangeKey, and the data point was sent to another worker,
		// this worker must wait for the exchange to terminate (ACK from other worker)
		if(waitingForInsert) return;

		// Schedule the next step delayed based on the current operation
		double delay = calculateDelay(schedule[currentScheduleStep]);
		scheduleAt(simTime()+delay, nextStepMsg);
		EV<<"Scheduled next step\n";
	} else {
		// Current step is finished because the queue is empty
		EV << "Empty queue, finished current step\n\n";
		std::cout << "Worker " << workerId << " finished step " << currentScheduleStep << " - New step data: \n";
		
		// Logging code (IGNORE)
		simtime_t end_op = simTime();
		simtime_t duration = end_op - begin_op;

		if(duration > 0) {
			per_op_exec_times[getParentOperation(schedule[currentScheduleStep])].push_back(duration);
		}
		begin_op = simTime(); // Reset timer for next operation		
		// End of Logging code

		// Increment the schedule step
		currentScheduleStep++;

		// If this step is the last, and it is a reduce, we move to the processReduce() function
		if(reduceLast && currentScheduleStep == schedule.size() - 1) {
			EV << "Entering reduce\n\n";
			//std::cout << "Worker " << workerId << " reducing: ";
			processReduce();
			//std::cout<<"Returning after process reduce\n";
			return;
		}
		
		// Else, recursively call this function
		processStep();
	}
}

/*
 * Processes the reduce for the current batch of data
 *
 * Note on failure detection: Because the reduce function is called just once per batch, and per schedule,
 * we adjust the failure probability to be a bit higher to account for this, so, the new probability becomes:
 * 	batchSize * schedule.size()/4 * baseProbability
 * Account for each data point in the batch, and increment as if the reduce was distributed like the other operations
 */
void Worker::processReduce(){
	if(failureDetection(batchSize*schedule.size()/4)){ //Simulate as if it was distributed like the other 3 operations
		// Logging (ignore - adding artificial delay)
		double delay = calculateDelay(schedule[currentScheduleStep]);
		int reductionFactor = (rand() % (batchSize)) + 1;
		// We just count durations, this is just a hack to avoid changing the deallocatingMemory() function
		begin_op -= (delay/reductionFactor); // Divide delay by random number in [1, batchSize] to simulate failing in the middle of the operation
		// End of logging
		failed = true;
		std::cout<<"FAILURE DETECTED AT WORKER: "<<workerId<<", deallocating memory\n";
		deallocatingMemory();
		return;
	}
	
	// Call the reduce function on the current batch of data
	int batchRes = reduce({data[currentScheduleStep].begin(), data[currentScheduleStep].end()});
	tmpReduce = tmpReduce + batchRes; // Increment partial result

	data[currentScheduleStep].clear();

	// Schedule a delayed nextStep due to the Reduce operation
	double delay = calculateDelay(schedule[currentScheduleStep]);
	std::cout << "Reduce delay: " << delay << "\n";
	currentScheduleStep++;

	scheduleAt(simTime()+delay, nextStepMsg);
}

/*
 * Loads the next batch of data in memory.
 * Batches are loaded in order, first exhausting all local batches, then all ChangeKey batches.
 * Local batches are handled by BatchLoader
 * ChangeKey batches are handled by InsertManager
 */
void Worker::loadNextBatch(){
	// Clear previous data
	data.clear();

	// Load a local batch
	if(localBatch) {
		// For fault tolerance purpose
		previousLocal = true;
		std::cout << "Worker " << workerId << " - Loading local:\n";
		// Get a batch from BatchLoader
		std::vector<int> batch = loader->loadBatch();
		
		// If the loaded batch is empty, it means we reached the end of the file
		if(batch.empty()){
			// Update FinishedLocal flag
			finishedLocalElaboration = true;
			localBatch = false;
			//std::cout << "Finished local, switching to ck" << "\n";
		} else {
			// Else, insert data in the first step of the schedule
        	data[0].insert(data[0].end(), batch.begin(), batch.end());
		}
	} else {
		// Load a changeKey batch
		// For fault tolerance purpose
		previousLocal = false;
		EV << "Loading CK...\n";
		
		// Get a batch from InsertManager - Format is: <scheduleStep, [data]>
		std::map<int, std::vector<int>> ckBatch = insertManager->getBatch();

		// If the batch is empty, it means this worker currently finished elaboration
		if(ckBatch.empty()){
			finishedPartialCK = true;
			EV << "CK data empty" << "\n";
		} else {
			EV << "\n\nCK not empty";
			// Else, insert data in the corresponding schedule step
			for(int i=0; i < ckBatch.size(); i++) {
				data[i].insert(data[i].end(), ckBatch[i].begin(), ckBatch[i].end());
			}
		}
	}
	localBatch = !finishedLocalElaboration; // Switch to ChangeKey data only when local data is finished
	currentScheduleStep = 0; // Reset step
}

/*
* Applies the operation at the current schedule step to the data point passed.
*
* Parameters:
*   - value: A pointer to the data point to be elaborated.
*
* Returns:
*   - true if the elaborated data point must continue in the schedule.
*   - false if the data point is filtered, or sent to another worker via the ChangeKey function.
*/
bool Worker::applyOperation(int& value){
	// Simulate crash probability
	if(failureDetection()){
		failed = true;
		std::cout<<"FAILURE DETECTED AT WORKER: "<<workerId<<", deallocating memory\n";
		deallocatingMemory();
		return false;
	}
	
	// Get current operation and respective parameter
	const std::string& operation = schedule[currentScheduleStep];
	const int& parameter = parameters[currentScheduleStep];

	// MAP segment
	if(operation == "add" || operation == "sub" || operation == "mul" || operation == "div") {
		int res = map(operation, parameter, value);
		std::cout << "Map result: " << res << "\n";
		value = res;

		return true;
	} 
	// FILTER segment
	else if(operation == "lt" || operation == "gt" || operation == "le" || operation == "ge") {
		return filter(operation, parameter, value);

	} 
	// CHANGEKEY segment
	else if(operation == "changekey") {
		// Simulate probability of changing key
		int newKey = changeKey(value, changeKeyProbability);
		
		// ChangeKey is executed if the key returned by the function is valid
		if(newKey != -1) {
        	std::cout << "Changing Key: " << workerId << " -> " << newKey << " for value: "<<value<<"\n";
        	// Send the current data point to worker corresponding to 'newKey'
        	// Current data point will be inserted at schedule step + 1 to account for this current Changekey operation
        	sendData(newKey, value, currentScheduleStep + 1);

        	// Return false because this data point no longer belongs to this worker
        	return false;
        }
		
        // Return true because the data point did not receive a new key, and must continue elaboration in this worker.
        return true;
	}
	return false;
}

/*
* 
*/
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

bool Worker::failureDetection(int factor){
	int res = bernoulli(failureProbability * factor);
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
