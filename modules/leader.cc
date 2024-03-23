#include <stdio.h>
#include <string.h>
#include <omnetpp.h>
#include <filesystem>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <numeric>

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
        bool finished;
        bool firstTime = true;
        int numWorkers;
        int scheduleSize;
        std::vector<int> ckReceived;
        std::vector<int> ckSent;
        std::vector<int> data;
        simtime_t interval;
        simtime_t timeout;
        std::vector<std::string> schedule;
        std::vector<int> parameters;
        std::vector<int> finishedWorkers;
        std::vector<int> pingWorkers;
        cMessage *ping_msg;
        cMessage *check_msg;
    protected:
        virtual void initialize() override;
        virtual void handleMessage(cMessage *msg) override;
        virtual void finish() override;
        void handleFinishElaborationMessage(FinishLocalElaborationMessage *msg);
        void handleCheckChangeKeyAckMessage(CheckChangeKeyAckMessage *msg);
        void handlePingMessage(cMessage *msg, int id);
        void checkPing();
        void sendPing();
        void createWorkersDirectory();
        void removeWorkersDirectory();
        int counter(std::vector<int> vec);
        void sendData(int id_dest);
        void sendSchedule();
        int numberOfFilters(int scheduleSize);
        void calcResult();
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
        ckReceived.push_back(0);
        ckSent.push_back(0);
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
    

	interval = 0.2;
	ping_msg = new cMessage("sendPing");
	scheduleAt(simTime() + interval, ping_msg);

	/*timeout = 3;
	check_msg = new cMessage("checkPing");
	scheduleAt(simTime()+timeout, check_msg);*/
}

void Leader::finish()
{
    calcResult();
    if(schedule[scheduleSize-1] == "reduce")
    {
        std::cout  << "Result: " << data[0] << std::endl;
    }
    else
    {
        EV << "Result: ";
        for(int i = 0; i < data.size(); i++)
        {
            std::cout << data[i] << " ";
        }
        std::cout << std::endl;
    }
}
void Leader::handleMessage(cMessage *msg)
{
    FinishLocalElaborationMessage *finishLocalMsg = dynamic_cast<FinishLocalElaborationMessage *>(msg);
    if(finishLocalMsg != nullptr)
    {
        handleFinishElaborationMessage(finishLocalMsg);
        return;
    }

    CheckChangeKeyAckMessage *checkChangeKeyAckMsg = dynamic_cast<CheckChangeKeyAckMessage *>(msg);
    if(checkChangeKeyAckMsg != nullptr)
    {
        handleCheckChangeKeyAckMessage(checkChangeKeyAckMsg);
        return;
    }

    // Self message to send ping to all worker nodes
    if(msg == ping_msg)
    {
        sendPing();
        return;
    }

    // Self message to check who did not send a ping to the leader node
    if(msg == check_msg)
    {
        if(!firstTime)
        {
            return;
        }
        checkPing();
        return;
    }

    // Received a ping message from worker node
    PingMessage *pingMsg = dynamic_cast<PingMessage *>(msg);
    if(pingMsg != nullptr)
    {
        handlePingMessage(pingMsg, pingMsg -> getWorkerId());
        return;
    }
}

/*void Leader::handleFinishElaborationMessage(cMessage *msg, bool local, int id)
{   
    finishedWorkers[id] = 1;
    delete msg;
    finished = true;

    for(int i = 0; i < numWorkers; i++){
        if(finishedWorkers[i] == 0){
            finished = false;
            break;
        }
    }
    if(finished){
        if(local){
            EV<<"\nEVERYONE FINISHED ITS LOCAL ELABORATION\n";
        }else{
            EV << "Application terminated at Leader side\n";
        }
        for(int i = 0; i < numWorkers; i++){
            if(local){
                FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
                finishLocalMsg -> setWorkerId(i);
                
                send(finishLocalMsg, "out", i);
                for(int i = 0; i < numWorkers; i++){
                    finishedWorkers[i] = 0;
                }
            }else{
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

    EV << "ChangeKeyReceived: "<< counter(ckReceived) <<" ChangeKeySent: "<< counter(ckSent) <<std::endl;
    EV << "Finished: "<< finished <<endl;
    if(counter(ckReceived) != counter(ckSent) && finished){
        for(int i = 0; i < numWorkers; i++){
            
            FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
            finishLocalMsg -> setWorkerId(i);
            send(finishLocalMsg, "out", i);
            
        }
    }else{
        if(finished && firstTime){
            firstTime = false;
            for(int i = 0; i < numWorkers; i++)
            {
                FinishSimMessage* finishSimMsg = new FinishSimMessage();
                finishSimMsg -> setWorkerId(i);
                send(finishSimMsg, "out", i);
            }
        }
    }
}

void Leader::handlePingMessage(cMessage *msg, int id)
{
    EV << "Ping received from worker: " << id << std::endl;
    pingWorkers[id] = 1;
}

void Leader::checkPing()
{
    for(int i = 0; i < numWorkers; i++)
    {
        if(pingWorkers[i] == 0)
        {
            EV << "Worker "<< i << " is dead. Sending Restart message" << std::endl;
            RestartMessage* resetMsg = new RestartMessage();
            resetMsg -> setScheduleArraySize(scheduleSize);
            resetMsg -> setParametersArraySize(scheduleSize);
            for(int i=0; i<scheduleSize; i++)
            {
                resetMsg -> setSchedule(i, schedule[i].c_str());
                resetMsg -> setParameters(i, parameters[i]);
            }
            resetMsg -> setWorkerID(i);
            send(resetMsg, "out", i);
        }
        pingWorkers[i] = 0;
    }

	ping_msg = new cMessage("sendPing");
	scheduleAt(simTime() + interval, ping_msg);
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

void Leader::sendPing()
{
    for(int i = 0; i < numWorkers; i++)
    {
        PingMessage *pingMsg = new PingMessage();
        pingMsg -> setWorkerId(i);
        send(pingMsg, "out", i);
    }
    timeout = 3;
	check_msg = new cMessage("checkPing");
	scheduleAt(simTime() + timeout, check_msg);
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

    msg -> setAssigned_id(idDest);
    
    // Generate a random dimension for the array of values
    int numElements = (rand () % 100) + 1;
    std::cout << "#elements: " << numElements << std::endl;
    
    msg -> setDataArraySize(numElements);
    std::cout << "Array: ";
    for(int j = 0; j < numElements; j++)
    {
         // Generate a random int value starting from 1
        int value = (rand() % 100) + 1;
        data.push_back(value);
        std::cout << value << " ";
        msg -> setData(j, value);
    }
    std::cout << endl << endl;
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
    std::cout << "There are " << op_size << " operations"<< endl;

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
    std::cout << std::endl << "Schedule size: " << scheduleSize << endl;
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
        std::cout << endl;
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
