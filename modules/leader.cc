#include <stdio.h>
#include <string.h>
#include <omnetpp.h>
#include <filesystem>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <numeric> // For std::accumulate
#include <fstream>

#include "setup_m.h"
#include "schedule_m.h"
#include "finishLocalElaboration_m.h"
#include "checkChangeKeyAck_m.h"
#include "ping_m.h"
#include "restart_m.h"
#include "finishSim_m.h"

#define EXPERIMENT_NAME "Increasing_Number_of_Data"

namespace fs = std::filesystem;
using namespace omnetpp;

class Leader : public cSimpleModule
{
    private:
        // Base simulation information
        int numWorkers;
        
        // Termination condition and result information
        bool finished;
        std::vector<int> finishedWorkers;
        std::vector<int> ckChecked;
        std::vector<int> ckReceived;
        std::vector<int> ckSent;
        std::vector<std::vector<int>> workerResult;

        // Schedule information
        int scheduleSize;
        bool reduceLast;
        std::vector<std::string> schedule;
        std::vector<int> parameters;
        
        // Data information
        int dataSize;
        std::vector<int> data;
        std::vector<std::vector<int>> data_clone;
        std::vector<std::vector<int>> dataMatrix;
        
        // Ping-related variables        
        bool stopPing;
        simtime_t interval;
        simtime_t timeout;
        cMessage *ping_msg;
        cMessage *check_msg;
        std::vector<int> pingWorkers;

        // Utils for plotting
        simtime_t startTime;
        double workerFailureProbability;
        int workerBatchSize;
    protected:
        virtual void initialize() override;
        virtual void finish() override;
        
        // Message handling
        virtual void handleMessage(cMessage *msg) override;
        void handleFinishElaborationMessage(FinishLocalElaborationMessage *msg);
        void handleCheckChangeKeyAckMessage(CheckChangeKeyAckMessage *msg);
        void handlePingMessage(cMessage *msg, int id);
        
        // Ping handling
        void checkPing();
        void sendPing();
        
        // Data, schedule handling
        void sendData(int id_dest);
        void sendSchedule();
        void sendCustomData(); // For custom testing
        void sendCustomSchedule(); // For custom testing
        
        // Final result calculation
        void calcResult();

        // Persistent storage handling
        void createWorkersDirectory();
        void removeWorkersDirectory();
        
        // Util functions
        int numberOfFilters(int scheduleSize);
        void getWorkerData();
        void logSimData();
        void printingVector(std::vector<int> vector);
        void printingStringVector(std::vector<std::string> vector);
        int counter(std::vector<int> vec);
};

Define_Module(Leader);

void Leader::initialize()
{
    // Basic initializations
    dataSize = 0;
    stopPing = false;

    // Remove all previous simulation data in './Data/'
    removeWorkersDirectory();
	//Create './Data/Worker_i' Directory for worker data
	createWorkersDirectory();
	
    // Initialize termination condition data structures
    numWorkers = par("numWorkers").intValue();
    for(int i = 0; i < numWorkers; i++)
    {
        finishedWorkers.push_back(0);
        ckChecked.push_back(0);
    }
    
    // Distribute data
    for(int i = 0; i < numWorkers; i++)
	{
	    // Re-initialize seed for more randomness
        srand((unsigned) time(NULL) + i);
        // Generate and send data to the worker i
	    sendData(i);
	}

    // Generate and send schedule to all workers
	sendSchedule();

	// Initialize structures based on numWorkers
	finishedWorkers.resize(numWorkers);
	pingWorkers.resize(numWorkers);
    workerResult.resize(numWorkers);
    ckSent.resize(numWorkers);
    ckReceived.resize(numWorkers);

    // Initialize helper flag
    reduceLast = (schedule[schedule.size() - 1] == "reduce");
    // If we expect one result per worker (reduce), resize the result vector
    if(reduceLast)
    {
        for(int i = 0; i < numWorkers; i++)
        {
            workerResult[i].resize(1);
        }
    }

	// Fix ping interval and schedule first ping
	interval = 2.5;
	ping_msg = new cMessage("sendPing");
	scheduleAt(simTime() + interval, ping_msg);

	// Fix ping timeout and schedule first check
	timeout = 2;
	check_msg = new cMessage("checkPing");
	scheduleAt(simTime() + interval + timeout, check_msg);

    // For logging
    startTime = simTime();
    getWorkerData();
}

void Leader::finish()
{
    // Calculate schedule result to check with workers'
    calcResult(); // Final result in data vector

    // Print final result
    std::cout << "\nResult should be: \n";
    if(reduceLast)
    {
        std::cout << data[0] << "\n";
    }
    else
    {
        // First sort the result and print
        std::sort(std::begin(data), std::end(data));
        printingVector(data);
    }
    
    std::cout << "\nAnd from the workers: \n";
    // Print result received by workers
    if(reduceLast)
    {
        int res = 0;
        for(int i = 0; i < numWorkers; i++)
        {
            res += workerResult[i][0];
        }
        // Sum and print reduced results
        std::cout << res << "\n" << "\n";
    }
    else
    {
        /*
        * Sort the received vectors
        * Compare with the result calculated
        * Print Correct/Incorrect
        */
        std::vector<int> workerRes;
        for(int i = 0; i < numWorkers; i++)
        {
            for(int j = 0; j < workerResult[i].size(); j++)
            {
                // Push all results
                workerRes.push_back(workerResult[i][j]);
            }
        }

        // Sort result vector
        std::sort(std::begin(workerRes), std::end(workerRes));
        printingVector(workerRes);

        std::cout << "\n\nThe result is: ";
        bool result;

        // Check if the elements contained are the same
        for(int i = 0; i < workerRes.size(); i++)
        {
            if(data[i] != workerRes[i])
            {
                result = false;
                break;
            }
        }

        // Check if the two vectors are of the same size
        if(data.size() != workerRes.size())
        {
            result = false;
        }
        else
        {
            result = true;
        }

        // Print final result
        if(result)
        {
            std::cout << "Correct\n\n";
        }
        else
        {
            std::cout << "Incorrect\n\n";
        }
    }

    // Debug prints (Ignore)
    std::cout << "For testing: " << "\n";
    std::cout << "dataMatrix: {";

    for(const auto& row : dataMatrix)
    {
        std::cout <<"{";
        printingVector(row);
        if(row != dataMatrix.back())
        {
            std::cout << "},\n";
        }
            else
            {
                std::cout << "}\n";
            }
    }

    std::cout << "};" << "\n";
    std::cout << "parameters = {";
    printingVector(parameters);
    std::cout << "};" << "\n";
    std::cout << "schedule = {";
    printingStringVector(schedule);
    std::cout << "};" << "\n";

    // Deallocating variables to avoid memory leaks
    if(ping_msg->isScheduled())
    {
        cancelEvent(ping_msg);
    }
    delete ping_msg;

    if(check_msg->isScheduled())
    {
        cancelEvent(check_msg);
    }
    delete check_msg;

    // Log data
    logSimData();
}

/*
* Calculates the result of the schedule based on parameters and data.
* The final result is stored in the data vector, in the first cell if
* the schedule has a final reduce
*/
void Leader::calcResult()
{
    std::vector<int> filteredData; // Tmp vector for filter operations

    for(size_t i = 0; i < schedule.size(); ++i)
    {
        const std::string& op = schedule[i];
        int param = parameters[i];
        filteredData.clear(); // Clear it before the next operation

        if(op == "add" || op == "sub" || op == "mul" || op == "div")
        {
            for(int& value : data)
            {
                if(op == "add") {
                    value += param;
                } else if(op == "sub") {
                    value -= param;
                } else if(op == "mul") {
                    value *= param;
                } else if(op == "div") {
                    value /= param;
                }
            }
        }
            else if(op == "gt" || op == "lt" || op == "ge" || op == "le")
            {
                for(int value : data)
                {
                    bool condition = false;
                    if(op == "gt") {
                        condition = (value > param);
                    } else if(op == "lt") {
                        condition = (value < param);
                    } else if(op == "ge") {
                        condition = (value >= param);
                    } else if(op == "le") {
                        condition = (value <= param);
                    }

                    // Push valid data points in filteredData vector
                    if(condition) {
                        filteredData.push_back(value);
                    }
                }
                data = filteredData; // Update data with filtered results.
            }
                else if(op == "reduce")
                {
                    int sum = 0;
                    for(int value : data)
                    {
                        sum += value;
                    }
                    data.clear();
                    data.push_back(sum); // Push in first cell
                    break; // No further operation is expected after reduce.
                }
        // Ignoring changekey op.
    }
}

/*
* - Testing function -
* Sends a custom data vector to each worker
* Used for debugging with specific data vectors
*/
void Leader::sendCustomData()
{
    dataMatrix = {{33, 74, 88, 39, 70, 75, 60, 62, 87, 48, 16, 71, 93, 92, 31, 87, 80, 92, 79, 50},
    {82, 38, 83, 85, 97, 87, 33, 73, 39, 100, 60, 42, 39, 16, 1, 5, 5, 23, 35, 65, 45, 39, 88},
    {62, 34, 78, 100, 55, 100, 5, 17, 59, 52, 4, 81, 17, 40, 71, 55, 62, 54, 92, 81},
    };
    
    // Update data vector for final calculation
    for(int i = 0; i < dataMatrix.size(); i++)
    {
        for(int j=0; j < dataMatrix[i].size(); j++)
        {
            data.push_back(dataMatrix[i][j]);
        }
        dataSize += dataMatrix[i].size();
    }

    // Send a setup message to each worker with custom data
    for(int i = 0; i < numWorkers; i++)
    {
        SetupMessage *msg = new SetupMessage();
        msg -> setAssigned_id(i);
        msg -> setDataArraySize(dataMatrix[i].size());
        for(int j = 0; j < dataMatrix[i].size(); j++)
        {
            msg -> setData(j, dataMatrix[i][j]);
        }
        send(msg, "out", i);
    }
}

/*
* - Testing function -
* Sends a custom schedule to each worker
* Used for debugging with specific schedules
*/
void Leader::sendCustomSchedule()
{
    // Define schedule and parameters
    schedule = {"gt", "changekey", "mul", "changekey", "changekey", "div", "le", "add", "changekey", "sub", "div", "reduce"};
    parameters = {15, 0, 4, 0, 0, 5, 84, 25, 0, 4, 3, 0};

    // Manually set values and send schedule to each worker
    scheduleSize = schedule.size();
    bool reduceFound = true;
    for(int i = 0; i < numWorkers; i++)
    {
        std::cout << "Schedule: ";
        ScheduleMessage *msg = new ScheduleMessage();
        msg -> setDestWorker(i);
        msg -> setScheduleArraySize(scheduleSize);
        msg -> setParametersArraySize(scheduleSize);

        for(int j = 0; j < scheduleSize; j++)
        {
            msg -> setSchedule(j, schedule[j].c_str());
            msg -> setParameters(j, parameters[j]);
            std::cout << schedule[j] << " ";
            std::cout << parameters[j] << " ";
        }
        send(msg, "out", i);
        std::cout << "\n";
    }
}

/*
* Handles incoming messages/self-messages by casting to their respective type, and
* calling the function responsible for the corresponding task.
*/
void Leader::handleMessage(cMessage *msg)
{
    /*
	*	FinishLocalElaboration Message segment:
	*	Update ChangeKey counters for this worker
	*/
    FinishLocalElaborationMessage *finishLocalMsg = dynamic_cast<FinishLocalElaborationMessage *>(msg);
    if(finishLocalMsg != nullptr)
    {
        handleFinishElaborationMessage(finishLocalMsg);
        delete msg;
        return;
    }

    /*
	*	CheckChangeKeyACK Message segment:
	*	Update ChangeKey counters
	*   Update Partial result
	*   Evaluate termination condition
	*/
    CheckChangeKeyAckMessage *checkChangeKeyAckMsg = dynamic_cast<CheckChangeKeyAckMessage *>(msg);
    if(checkChangeKeyAckMsg != nullptr)
    {
        handleCheckChangeKeyAckMessage(checkChangeKeyAckMsg);
        delete msg;
        return;
    }

    // Segment for workers pinging self-message
    if(msg == ping_msg)
    {
        if(stopPing) return;

        sendPing();
        return;
    }

    // Segment for ping timeout self-message
    if(msg == check_msg)
    {
        if(stopPing) return;

        checkPing();
        return;
    }

    /*
	*	Ping Message segment:
	*	Register a worker's response to the ping
	*/
    PingMessage *pingMsg = dynamic_cast<PingMessage *>(msg);
    if(pingMsg != nullptr)
    {
        handlePingMessage(pingMsg, pingMsg -> getWorkerId());
        delete msg;
        return;
    }

}

/*
*
*/
void Leader::handleFinishElaborationMessage(FinishLocalElaborationMessage *msg)
{
    int id = msg -> getWorkerId();
    finishedWorkers[id] = 1;
    finished = true;

    for(int i = 0; i < numWorkers; i++)
    {
        if(finishedWorkers[i] == 0)
        {
            finished = false;
            break;
        }
    }

    ckReceived[id] = msg -> getChangeKeyReceived();
    ckSent[id] = msg -> getChangeKeySent();

    //TODO: change the message type replacing this with a new message more understandable
    FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
    finishLocalMsg -> setWorkerId(id);
    send(finishLocalMsg, "out", id);
}

void Leader::handleCheckChangeKeyAckMessage(CheckChangeKeyAckMessage *msg)
{
    int id = msg -> getWorkerId();

    ckReceived[id] = msg -> getChangeKeyReceived();
    ckSent[id] = msg -> getChangeKeySent();
    if(reduceLast)
    {
        workerResult[id][0] = msg -> getPartialRes();
    }
        else
        {
            int resultSize = msg -> getPartialVectorArraySize();
            workerResult[id].clear();
            workerResult[id].resize(resultSize);
            for(int i = 0; i < resultSize; i++)
            {
                workerResult[id][i] = msg -> getPartialVector(i);
            }
        }
    ckChecked[id] = 1;
    bool allChecked = true;

    for(int i = 0; i < numWorkers; i++)
    {
        if(ckChecked[i] == 0)
        {
            allChecked = false;
            break;
        }
    }

    EV<<"ChangeKeyReceived: "<<counter(ckReceived)<<" ChangeKeySent: "<<counter(ckSent)<<"\n";
    EV<<"Finished: "<<finished<<"\n";
    if(counter(ckReceived) != counter(ckSent) && finished && allChecked)
    {
        for(int i = 0; i < numWorkers; i++)
        {
            ckChecked[i] = 0;   
            FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
            finishLocalMsg -> setWorkerId(i);
            send(finishLocalMsg, "out", i);
        }
    }
        else
        {
            if(finished && allChecked)
            {
                for(int i = 0; i < numWorkers; i++)
                {
                    FinishSimMessage* finishSimMsg = new FinishSimMessage();
                    finishSimMsg -> setWorkerId(i);
                    send(finishSimMsg, "out", i);
                    stopPing = true;
                }
            }
        }
}

void Leader::handlePingMessage(cMessage *msg, int id)
{
    EV << "Ping received from worker: " << id << "\n";
    pingWorkers[id] = 1;
}

void Leader::checkPing()
{
    for(int i = 0; i < numWorkers; i++)
    {
        if(pingWorkers[i] == 0)
        {
            EV << "Worker "<< i << " is dead. Sending Restart message" << "\n";
            RestartMessage* restartMsg = new RestartMessage();
            restartMsg -> setWorkerID(i);

            restartMsg -> setScheduleArraySize(scheduleSize);
            restartMsg -> setParametersArraySize(scheduleSize);

            for(int j = 0; j < scheduleSize; j++)
            {
                restartMsg -> setSchedule(j, schedule[j].c_str());
                restartMsg -> setParameters(j, parameters[j]);
            }
            send(restartMsg, "out", i);
        }
        pingWorkers[i] = 0;
    }

    scheduleAt(simTime() + interval, ping_msg);
    scheduleAt(simTime() + interval + timeout, check_msg);
}

void Leader::sendPing()
{
    for(int i = 0; i < numWorkers; i++)
    {
        PingMessage *pingMsg = new PingMessage();
        pingMsg -> setWorkerId(i);
        send(pingMsg, "out", i);
    }
}

void Leader::createWorkersDirectory()
{
    fs::path dirPath = "Data";
    if(!fs::exists(dirPath))
    {
        fs::create_directory(dirPath);
    }

    int numWorkers = par("numWorkers").intValue();
    fs::path subPath;
    for(int i = 0; i < numWorkers; i++)
    {
        subPath = "Worker_" + std::to_string(i);
        fs::create_directory(dirPath/subPath);
    }
}

void Leader::removeWorkersDirectory()
{
    fs::path dirPath = "Data";
    // Check if the directory exists before trying to remove it
    if(fs::exists(dirPath) && fs::is_directory(dirPath))
    {
        // Remove the directory and its contents recursively
        fs::remove_all(dirPath);
    }
}

void Leader::sendData(int idDest)
{
    SetupMessage *msg = new SetupMessage();
    std::vector<int> currentData;
    msg -> setAssigned_id(idDest);
    
    // Generate a random dimension for the array of values
    int minimum = 50;
    int maximum = 60;
    int numElements = minimum + rand() % (maximum - minimum + 1);

    dataSize += numElements;

    std::cout << "#elements: " << numElements << "\n";
    
    msg -> setDataArraySize(numElements);
    std::cout << "Array: ";
    for(int j = 0; j < numElements; j++)
    {
         // Generate a random int value starting from 1
        int value = (rand() % 100) + 1;
        std::cout << value << " ";
        msg -> setData(j, value);

        // Keep track of data for final check
        data.push_back(value);
        currentData.push_back(value);
    }
    dataMatrix.push_back(currentData);
    std::cout << "\n" << "\n";
    send(msg, "out", idDest);
}

void Leader::sendSchedule()
{
    int numWorkers = par("numWorkers").intValue();
    int changekeyPos, reducePos;
    int lowerBound = 0;
    int upperBound = 100;
    int maxFilter = 0;

    // Define the possible operations set
    std::vector<std::string> operations = {"add", "sub", "mul", "div", "gt", "lt", "ge", "le", "changekey", "reduce"};
    int op_size = operations.size();
    std::cout << "There are " << op_size << " operations"<< "\n";

    for(int i = 0; i < op_size; i++)
    {
        if(operations[i] == "changekey")
        {
            changekeyPos = i;
        }
        if(operations[i] == "reduce")
        {
            reducePos = i;
        }
    }

    bool reduceFound = false;

    // Generate a random number for the dimension of the schedule
    lowerBound = 8;
    upperBound = 20;
    scheduleSize = lowerBound + rand() % (upperBound - lowerBound + 1);
    std::cout << "\n" << "Schedule size: " << scheduleSize << "\n";
    maxFilter = numberOfFilters(scheduleSize);

    // Instantiate an empty array for the actual schedule and for parameters
    schedule.resize(scheduleSize);
    parameters.resize(scheduleSize);

    for(int i = 0; i < scheduleSize; ++i)
    {
        // Generate a random index to pick a random operation in the set
        int randomIndex = (rand() % (op_size));

        while((operations[randomIndex] == "reduce" && i != scheduleSize-1) || ((operations[randomIndex] == "le" || operations[randomIndex] == "lt"|| operations[randomIndex] == "ge" || operations[randomIndex] == "gt") && maxFilter == 0))    
        {
            reduceFound = true;
            randomIndex = (rand() % (op_size));
        }
        if(randomIndex == changekeyPos)
        {
            // Generate a random number between 1 e numWorkers for the changeKey operation
            int param = 0;
            parameters[i] = param;
        }
        else
        {
            // Generate a random number between 1 and 10 avoiding negative numbers due to the division operation
            if(operations[randomIndex] == "le" || operations[randomIndex] == "lt")
            {
                lowerBound = 60;
                upperBound = 100;
                int param = lowerBound + rand() % (upperBound - lowerBound + 1);
                parameters[i] = param;
            }
                else if(operations[randomIndex] == "ge" || operations[randomIndex] == "gt")
                {
                    lowerBound = 0;
                    upperBound = 40;
                    int param = lowerBound + rand() % (upperBound - lowerBound + 1);
                    parameters[i] = param;
                }
                    else
                    {
                        int param = (rand() % 10) + 1;
                        parameters[i] = param;
                    }
            
        }
        schedule[i] = operations[randomIndex];
        if(operations[randomIndex] == "le" || operations[randomIndex] == "lt" || operations[randomIndex] == "ge" || operations[randomIndex] == "gt")
        {
            maxFilter--;
        }
    }

    // Sending the schedule and the parameters
    for(int i = 0; i < numWorkers; i++)
    {
        std::cout << "Schedule: ";
        ScheduleMessage *msg = new ScheduleMessage();
        msg -> setDestWorker(i);
        msg -> setScheduleArraySize(scheduleSize);
        msg -> setParametersArraySize(scheduleSize);

        for(int j = 0; j < scheduleSize; j++)
        {
            msg -> setSchedule(j, schedule[j].c_str());
            msg -> setParameters(j, parameters[j]);
            if(reduceFound == true)
            {
                schedule[scheduleSize-1] = operations[reducePos];
                parameters[scheduleSize-1] = 0;
                msg -> setSchedule((scheduleSize-1), operations[op_size-1].c_str());
                msg -> setParameters((scheduleSize-1), 0);
            }
            std::cout << schedule[j] << " ";
            std::cout << parameters[j] << " ";
        }
        send(msg, "out", i);
        std::cout << "\n";
    }
}

int Leader::numberOfFilters(int scheduleSize)
{
    if(scheduleSize <= 10)
        return 2;
    else if(scheduleSize <= 15)
        return 3;
    else if(scheduleSize <= 20)
        return 4;
    else
        return 5;
}

void Leader::printingVector(std::vector<int> vector)
{
    for(int i = 0; i < vector.size(); i++)
    {
        if(i < vector.size()-1)
        {
            std::cout<<vector[i]<<", ";
        }
            else
            {
                std::cout<<vector[i];
            }
    }
}

void Leader::printingStringVector(std::vector<std::string> vector)
{
    for(int i = 0; i < vector.size(); i++)
    {
        if(i < vector.size()-1)
        {
            std::cout<< "\"" <<vector[i] << "\"" <<", ";
        }
            else
            {
                std::cout<< "\"" <<vector[i] << "\"";
            }
    }
}

int Leader::counter(std::vector<int> vec)
{
    int count = 0;
    for(int i=0; i < vec.size(); i++)
    {
        count += vec[i];
    }
    return count;
}

void Leader::getWorkerData()
{
    cModule *worker = getParentModule()->getSubmodule("worker", 0); // Access the first worker
    if(worker != nullptr)
    {
        // Accessing parameters
        workerFailureProbability = worker->par("failureProbability").doubleValue();
        workerBatchSize = worker->par("batchSize").intValue();
    }
}

void Leader::logSimData()
{
    // Capture the simulation end time
    simtime_t endTime = simTime();

    // Calculate duration
    simtime_t duration = endTime - startTime;

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
    for(const auto& entry : fs::directory_iterator(parentDir))
    {
        if(entry.is_directory())
        {
            std::string folderName = entry.path().filename().string();
            try
            {
                int folderId = std::stoi(folderName);
                maxId = std::max(maxId, folderId);
            }
                catch(const std::invalid_argument& e)
                {
                // Not a number-named folder, ignore
                }
        }
    }

    // Determine the folder name for the new simulation
    int newFolderId = maxId + 1;
    fs::path newFolderPath = parentDir / std::to_string(newFolderId);

    // Create the folder for the new simulation
    if(fs::create_directory(newFolderPath))
    {
        std::cout << "Created directory for simulation ID: " << newFolderId << "\n";
    }
        else
        {
            std::cerr << "Failed to create directory for new simulation.q";
            return; // Error
        }

    std::string fileName = newFolderPath.string() + "/SIM_" + std::to_string(numWorkers) + "_" + std::to_string(workerFailureProbability) + "_" + std::to_string(dataSize) + "_" + std::to_string(workerBatchSize) + "_" + std::to_string(scheduleSize) + ".log"; 
    std::ofstream outFile(fileName);
    if(outFile.is_open())
    {
        outFile << duration;
        outFile.close();
    }
        else
        {
            EV << "Error opening file for writing simulation duration.\n";
        }
}
