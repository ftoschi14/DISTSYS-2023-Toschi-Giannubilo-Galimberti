#include <stdio.h>
#include <string.h>
#include <omnetpp.h>
#include <filesystem>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <numeric> // For std::accumulate

#include "setup_m.h"
#include "schedule_m.h"
#include "finishLocalElaboration_m.h"
#include "checkChangeKeyAck_m.h"
#include "ping_m.h"
#include "restart_m.h"
#include "finishSim_m.h"

namespace fs = std::filesystem;
using namespace omnetpp;

class Leader : public cSimpleModule
{
    private:
        bool firstTime = true;
        bool finished;
        bool reallyFinished;
        int numWorkers;
        int scheduleSize;
        std::vector<int> ckReceived;
        std::vector<int> ckSent;
        std::vector<int> workerResult;
        simtime_t interval;
        simtime_t timeout;
        std::vector<std::string> schedule;
        std::vector<int> parameters;
        std::vector<int> finishedWorkers;
        std::vector<int> pingWorkers;
        cMessage *ping_msg;
        cMessage *check_msg;

        std::vector<int> data;
        std::vector<std::vector<int>> data_clone;
    protected:
        virtual void initialize() override;
        virtual void finish() override;
        virtual void handleMessage(cMessage *msg) override;
        void handleFinishElaborationMessage(FinishLocalElaborationMessage *msg);
        void handleCheckChangeKeyAckMessage(CheckChangeKeyAckMessage *msg);
        void handlePingMessage(cMessage *msg, int id);
        void checkPing();
        void sendPing();
        void createWorkersDirectory();
        void removeWorkersDirectory();
        void sendData(int id_dest);
        void sendCustomData();
        void sendSchedule();
        void sendCustomSchedule();
        int numberOfFilters(int scheduleSize);
        void calcResult();
        void calcResultFor();
        void printingVector(std::vector<int> vector);
        void printingStringVector(std::vector<std::string> vector);
        int counter(std::vector<int> vec);
};

Define_Module(Leader);

void Leader::initialize()
{
    // Remove all previous folders in './Data/'
    removeWorkersDirectory();
	//Create './Data/Worker_i' Directory for worker data
	createWorkersDirectory();
	
    numWorkers = par("numWorkers").intValue();
    for(int i = 0; i < numWorkers; i++)
    {
        finishedWorkers.push_back(0);
    }

	for(int i = 0; i < numWorkers; i++)
	{
        srand((unsigned) time(NULL) + i);
        // Call the function for sending the data
	    sendData(i);
	}

    // Call the function for sending the schedule
	sendSchedule();

	finishedWorkers.resize(numWorkers);
	pingWorkers.resize(numWorkers);
    workerResult.resize(numWorkers);
    ckSent.resize(numWorkers);
    ckReceived.resize(numWorkers);

	interval = 2.5;
	ping_msg = new cMessage("sendPing");
	scheduleAt(simTime() + interval, ping_msg);

	timeout = 2;
	check_msg = new cMessage("checkPing");
	scheduleAt(simTime() + interval + timeout, check_msg);
}

void Leader::finish() {
    calcResultFor();
    std::cout << "Result should be: \n";
    if(data.size() == 1) {
        std::cout << data[0] << "\n";
    } else {
        printingVector(data);
    }
    std::cout << "And from the workers: \n";

    int res = 0;
    for(int val : workerResult) {
        res += val;
    }

    std::cout << res << "\n" << "\n";

    std::cout << "For testing: " << "\n";
    std::cout << "Data: {\n";

    for(const auto& row : data_clone) {
        std::cout <<"{";
        printingVector(row);
        std::cout << "}\n";
    }
    std::cout << "};" << "\n";
    std::cout << "parameters: {";
    printingVector(parameters);
    std::cout << "};" << "\n";
    std::cout << "Schedule: {";
    printingStringVector(schedule);
    std::cout << "};" << "\n";

    if(ping_msg->isScheduled()){
        cancelEvent(ping_msg);
    }
    delete ping_msg;

    if(check_msg->isScheduled()){
        cancelEvent(check_msg);
    }
    delete check_msg;
}

void Leader::calcResultFor() {
    std::vector<int> filteredData; // Use this for operations that might change data size.

    for (size_t i = 0; i < schedule.size(); ++i) {
        const std::string& op = schedule[i];
        int param = parameters[i];
        filteredData.clear(); // Clear it for every operation that uses it.

        if (op == "add" || op == "sub" || op == "mul" || op == "div") {
            for (int& value : data) {
                if (op == "add") {
                    value += param;
                } else if (op == "sub") {
                    value -= param;
                } else if (op == "mul") {
                    value *= param;
                } else if (op == "div") {
                    value /= param; // Assuming param != 0
                }
            }
        } else if (op == "gt" || op == "lt" || op == "ge" || op == "le") {
            for (int value : data) {
                bool condition = false;
                if (op == "gt") {
                    condition = (value > param);
                } else if (op == "lt") {
                    condition = (value < param);
                } else if (op == "ge") {
                    condition = (value >= param);
                } else if (op == "le") {
                    condition = (value <= param);
                }

                if (condition) {
                    filteredData.push_back(value);
                }
            }
            data = filteredData; // Update data with filtered results.
        } else if (op == "reduce") {
            int sum = 0;
            for (int value : data) {
                sum += value;
            }
            data.clear();
            data.push_back(sum);
            break; // No further operation is expected after reduce.
        }
        // Ignoring 'changekey' as instructed.
    }
}

void Leader::calcResult() {
    for (size_t i = 0; i < schedule.size(); ++i) {
        const auto& op = schedule[i];
        
        if (op == "add") {
            std::transform(data.begin(), data.end(), data.begin(),
                           [param = parameters[i]](int x) { return x + param; });
        } else if (op == "sub") {
            std::transform(data.begin(), data.end(), data.begin(),
                           [param = parameters[i]](int x) { return x - param; });
        } else if (op == "mul") {
            std::transform(data.begin(), data.end(), data.begin(),
                           [param = parameters[i]](int x) { return x * param; });
        } else if (op == "div") {
            std::transform(data.begin(), data.end(), data.begin(),
                           [param = parameters[i]](int x) { return x / param; });
        } else if (op == "gt") {
            data.erase(std::remove_if(data.begin(), data.end(),
                                      [param = parameters[i]](int x) { return x <= param; }),
                       data.end());
        } else if (op == "lt") {
            data.erase(std::remove_if(data.begin(), data.end(),
                                      [param = parameters[i]](int x) { return x >= param; }),
                       data.end());
        } else if (op == "ge") {
            data.erase(std::remove_if(data.begin(), data.end(),
                                      [param = parameters[i]](int x) { return x < param; }),
                       data.end());
        } else if (op == "le") {
            data.erase(std::remove_if(data.begin(), data.end(),
                                      [param = parameters[i]](int x) { return x > param; }),
                       data.end());
        } else if (op == "reduce") {
            // Assuming 'reduce' means sum all elements and replace the vector with a single-element vector.
            int sum = std::accumulate(data.begin(), data.end(), 0);
            data.clear();
            data.push_back(sum);
            break; // Once reduced, no further operations make sense.
        }
        // 'changekey' operation is ignored as per instructions.
    }
}

void Leader::sendCustomData(){
    std::vector<int> data_1 = {18, 78, 25, 35, 75, 5, 53, 21, 36, 100, 81, 78, 59, 51, 57, 60, 32, 64, 100, 48, 46, 64, 28, 52, 70, 11, 41, 33, 3, 57, 63};
    std::vector<int> data_2 = {66, 42, 21, 50, 2, 17, 26, 33, 88, 52, 25, 48, 37, 75, 95, 10, 58, 95, 56, 64, 27, 10, 42, 87, 29, 20, 17, 58, 20, 90, 100, 32, 45, 60};
    
    data = {18, 78, 25, 35, 75, 5, 53, 21, 36, 100, 81, 78, 59, 51, 57, 60, 32, 64, 100, 48, 46, 64, 28, 52, 70, 11, 41, 33, 3, 57, 63, 66, 42, 21, 50, 2, 17, 26, 33, 88, 52, 25, 48, 37, 75, 95, 10, 58, 95, 56, 64, 27, 10, 42, 87, 29, 20, 17, 58, 20, 90, 100, 32, 45, 60};

    // Generate a random dimension for the array of values
    int numElements_1 = data_1.size();
    int numElements_2 = data_2.size();

    SetupMessage *msg_1 = new SetupMessage();
    msg_1 -> setAssigned_id(0);
    msg_1 -> setDataArraySize(numElements_1);
    for(int j = 0; j < numElements_1; j++)
    {
        msg_1 -> setData(j, data_1[j]);
    }
    send(msg_1, "out", 0);

    SetupMessage *msg_2 = new SetupMessage();
    msg_2 -> setAssigned_id(1);
    msg_2 -> setDataArraySize(numElements_2);
    for(int j = 0; j < numElements_2; j++)
    {
        msg_2 -> setData(j, data_2[j]);
    }
    send(msg_2, "out", 1);
}

void Leader::sendCustomSchedule()
{
    scheduleSize = 6;
    schedule = {"gt", "changekey", "add", "changekey", "add", "reduce"};
    parameters = {30,0,2,0,2,0};
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

void Leader::handleMessage(cMessage *msg)
{
    FinishLocalElaborationMessage *finishLocalMsg = dynamic_cast<FinishLocalElaborationMessage *>(msg);
    if(finishLocalMsg != nullptr)
    {
        handleFinishElaborationMessage(finishLocalMsg);
        delete msg;
        return;
    }

    CheckChangeKeyAckMessage *checkChangeKeyAckMsg = dynamic_cast<CheckChangeKeyAckMessage *>(msg);
    if(checkChangeKeyAckMsg != nullptr)
    {
        handleCheckChangeKeyAckMessage(checkChangeKeyAckMsg);
        delete msg;
        return;
    }

    // Self message to send ping to all worker nodes
    if(msg == ping_msg)
    {
        if(reallyFinished) return;

        sendPing();
        return;
    }

    // Self message to check who did not send a ping to the leader node
    if(msg == check_msg)
    {
        if(reallyFinished) return;

        checkPing();
        return;
    }

    // Received a ping message from worker node
    PingMessage *pingMsg = dynamic_cast<PingMessage *>(msg);
    if(pingMsg != nullptr)
    {
        handlePingMessage(pingMsg, pingMsg -> getWorkerId());
        delete msg;
        return;
    }

}

/*void Leader::handleFinishElaborationMessage(cMessage *msg, bool local, int id)
{   
    finishedWorkers[id] = 1;
    delete msg;
    finished = true;

    for(int i = 0; i < numWorkers; i++)
    {
        if(finishedWorkers[i] == 0)
        {
            finished = false;
            break;
        }
    }
    if(finished)
    {
        if(local)
        {
            EV<<"\nEVERYONE FINISHED ITS LOCAL ELABORATION\n";
        }
            else
            {
                EV << "Application terminated at Leader side\n";
            }
        for(int i = 0; i < numWorkers; i++)
        {
            if(local)
            {
                FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
                finishLocalMsg -> setWorkerId(i);
                
                send(finishLocalMsg, "out", i);
                for(int i = 0; i < numWorkers; i++)
                {
                    finishedWorkers[i] = 0;
                }
            }
                else
                {
                    FinishSimMessage* finishSimMsg = new FinishSimMessage();
                    finishSimMsg -> setWorkerId(i);
                    send(finishSimMsg, "out", i);
                }
        }
    }
}*/

void Leader::handleFinishElaborationMessage(FinishLocalElaborationMessage *msg){
    int id = msg -> getWorkerId();
    finishedWorkers[id] = 1;

    finished = true;
    for(int i = 0; i < numWorkers; i++){
        if(finishedWorkers[i] == 0){
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

void Leader::handleCheckChangeKeyAckMessage(CheckChangeKeyAckMessage *msg){
    int id = msg -> getWorkerId();

    ckReceived[id] = msg -> getChangeKeyReceived();
    ckSent[id] = msg -> getChangeKeySent();
    workerResult[id] = msg -> getPartialRes();

    EV<<"ChangeKeyReceived: "<<counter(ckReceived)<<" ChangeKeySent: "<<counter(ckSent)<<"\n";
    EV<<"Finished: "<<finished<<"\n";
    if(counter(ckReceived) != counter(ckSent) && finished){
        for(int i = 0; i < numWorkers; i++){
            
            FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
            finishLocalMsg -> setWorkerId(i);
            send(finishLocalMsg, "out", i);
            
        }
    }else{
        if(finished && firstTime){
            firstTime = false;
            for(int i = 0; i < numWorkers; i++){
                FinishSimMessage* finishSimMsg = new FinishSimMessage();
                finishSimMsg -> setWorkerId(i);
                send(finishSimMsg, "out", i);
                reallyFinished = true;
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
    int numElements = 20 + rand() % (40 - 20 + 1);
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
    data_clone.push_back(currentData);
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
    if (scheduleSize <= 10)
        return 2;
    else if (scheduleSize <= 15)
        return 3;
    else if (scheduleSize <= 20)
        return 4;
    else
        return 5;
}

void Leader::printingVector(std::vector<int> vector){
    for(int i=0; i<vector.size(); i++){
        if(i<vector.size()-1){
            std::cout<<vector[i]<<", ";
        }else{
            std::cout<<vector[i];
        }
    }
}

void Leader::printingStringVector(std::vector<std::string> vector){
    for(int i=0; i<vector.size(); i++){
        if(i<vector.size()-1){
            std::cout<< "\"" <<vector[i] << "\"" <<", ";
        }else{
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

