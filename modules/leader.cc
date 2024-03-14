#include <stdio.h>
#include <string.h>
#include <omnetpp.h>
#include <filesystem>
#include <cstdlib>
#include <ctime>

#include "setup_m.h"
#include "schedule_m.h"
#include "finishLocalElaboration_m.h"
#include "checkChangeKeyAck_m.h"
#include "finishSim_m.h"

namespace fs = std::filesystem;
using namespace omnetpp;

class Leader : public cSimpleModule
{
    protected:
        virtual void initialize() override;
        virtual void handleMessage(cMessage *msg) override;
        void createWorkersDirectory();
        void removeWorkersDirectory();
        void sendData(int id_dest);
        void sendSchedule();
        void handleFinishElaborationMessage(cMessage *msg, bool local, int id);
    private:
        bool finished;
        int numWorkers;
        std::vector<int> finishedWorkers;
};

Define_Module(Leader);

void Leader::initialize()
{
    // Remove all previous folders in './Data/'
    removeWorkersDirectory();

	//Create './Data/Worker_i' directory for worker data
	createWorkersDirectory();
    
    numWorkers = par("numWorkers").intValue();
    for(int i = 0; i < numWorkers; i++)
    {
        finishedWorkers.push_back(0);
    }

	for(int i = 0; i < numWorkers; i++)
	{
	    srand((unsigned)time(NULL) + i);
	    // Call the function for sending the data
	    sendData(i);
	}
	// Call the function for sending the schedule
	sendSchedule();
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

void Leader::handleMessage(cMessage *msg)
{
    FinishLocalElaborationMessage *finishLocalMsg = dynamic_cast<FinishLocalElaborationMessage *>(msg);
    if(finishLocalMsg != nullptr)
    {
        handleFinishElaborationMessage(finishLocalMsg, true, finishLocalMsg -> getWorkerId());
        return;
    }
    CheckChangeKeyAckMessage *checkChangeKeyAckMsg = dynamic_cast<CheckChangeKeyAckMessage *>(msg);
    if(checkChangeKeyAckMsg != nullptr)
    {
        handleFinishElaborationMessage(finishLocalMsg, false, checkChangeKeyAckMsg -> getWorkerId());
        return;
    }
}

void Leader::handleFinishElaborationMessage(cMessage *msg, bool local, int id)
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
        for(int i = 0; i < numWorkers; i++)
        {
            if(local)
            {
                FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
                finishLocalMsg -> setWorkerId(i);
                EV << "Worker " << finishLocalMsg -> getWorkerId() << " has finished its local elaboration" << std::endl;
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
                    EV << "Application terminated at Leader side\n";
                    send(finishSimMsg, "out", i);
                }
        }
    }
}

void Leader::sendData(int idDest)
{
    SetupMessage *msg = new SetupMessage();
    msg -> setAssigned_id(idDest);

    // Generate a random dimension for the array of values
    int numElements = (rand () % 10) + 1;
    std::cout << "#elements: " << numElements << std::endl;

    msg -> setDataArraySize(numElements);
    std::cout << "Array: ";
    for(int j = 0; j < numElements; j++)
    {
        // Generate a random int value starting from 1
        int value = (rand() % 100) + 1;
        std::cout << value << " ";
        msg -> setData(j, value);
    }
    std::cout << endl << endl;
    send(msg, "out", idDest);
}

void Leader::sendSchedule()
{
    int numWorkers = par("numWorkers").intValue();
    int maxScheduleSize = 50;
    int changekeyPos, reducePos;

    // Define the possible operations set
    std::vector<std::string> operations = {"add", "sub", "mul", "div", "gt", "lt", "ge", "le", "changekey", "reduce"};
    int op_size = operations.size();
    std::cout << "There are " << op_size << " operations"<< endl;

    for(int i = 0; i < op_size; i++)
    {
        if(operations[i] == "changekey")
        {
            changekeyPos = i;
            std::cout << "ChangeKey Pos = "<< changekeyPos << std::endl;
        }
        if(operations[i] == "reduce")
        {
            reducePos = i;
            std::cout << "Reduce Pos = "<< reducePos << std::endl;
        }
    }

    bool reduceFound = false;

    // Generate a random number for the dimension of the schedule
    int scheduleSize = (rand() % maxScheduleSize) + 1;
    std::cout << std::endl << "Schedule size: " << scheduleSize << endl;

    // Instantiate an empty array for the actual schedule and for parameters
    std::vector<std::string> schedule(scheduleSize);
    std::vector<int> parameters(scheduleSize);

    for(int i = 0; i < scheduleSize; ++i)
    {
        // Generate a random index to pick a random operation in the set
        int randomIndex = (rand() % (op_size));
        while(operations[randomIndex] == "reduce" && i != scheduleSize-1)
        {
            reduceFound = true;
            randomIndex = (rand() % (op_size));
            std::cout << "Reduce found: " << reduceFound << endl;
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
                int param = (rand() % 10) + 1;
                parameters[i] = param;
            }
        schedule[i] = operations[randomIndex];
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
