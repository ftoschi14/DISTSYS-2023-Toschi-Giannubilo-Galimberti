#include <stdio.h>
#include <string.h>
#include <omnetpp.h>
#include <filesystem>
#include "setup_m.h"
#include "schedule_m.h"
#include "finishLocalElaboration_m.h"

namespace fs = std::filesystem;
using namespace omnetpp;

class Leader : public cSimpleModule
{
    public:
        ~Leader()
        {
            removeWorkersDirectory();
        }
    protected:
        virtual void initialize() override;
        virtual void handleMessage(cMessage *msg) override;
        //void sendTestSetupMessage(int gateIndex);
        void createWorkersDirectory();
        void sendData(int id_dest);
        void sendSchedule();
        void handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg);
    private:
        int numWorkers;
        std::vector<int> localFinishedWorkers;
        bool everyOneFinished;
        void removeWorkersDirectory()
        {
            fs::path dirPath = "Data";
            // Check if the directory exists before trying to remove it
            if (fs::exists(dirPath) && fs::is_directory(dirPath))
            {
                // Remove the directory and its contents recursively
                fs::remove_all(dirPath);
            }
        }
};

Define_Module(Leader);

void Leader::initialize()
{
	//Create './Data/Worker_i' Directory for worker data
	createWorkersDirectory();
    
	//sendTestSetupMessage(0);
    numWorkers = par("numWorkers").intValue();
    for(int i = 0; i < numWorkers; i++)
    {
        localFinishedWorkers.push_back(0);
    }

	for(int i = 0; i < numWorkers; i++)
	{
	    sendData(i);
	}
	sendSchedule();
}

void Leader::handleMessage(cMessage *msg)
{
    FinishLocalElaborationMessage *finishLocalMsg = dynamic_cast<FinishLocalElaborationMessage *>(msg);
    if(finishLocalMsg != nullptr)
    {
        handleFinishLocalElaborationMessage(finishLocalMsg);
    }
    
	
}

void Leader::handleFinishLocalElaborationMessage(FinishLocalElaborationMessage *msg)
{
    int id = msg -> getWorkerId();
    localFinishedWorkers[id] = 1;
    EV << "\nWORKER " << id << " FINISHED ITS LOCAL EXECUTION\n\n";
    delete msg;
    everyOneFinished = true;
    for(int i = 0; i < numWorkers; i++)
    {
        if(localFinishedWorkers[i] == 0)
        {
            everyOneFinished = false;
            break;
        }
    }
    if(everyOneFinished)
    {
        for(int i = 0; i < numWorkers; i++)
        {
            FinishLocalElaborationMessage* finishLocalMsg = new FinishLocalElaborationMessage();
            finishLocalMsg -> setWorkerId(i);
            send(finishLocalMsg, "out", i);
        }
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

/*
void Leader::sendTestSetupMessage(int gateIndex){
	SetupMessage *msg = new SetupMessage();
	msg->setAssigned_id(0);

	int numOfElements = 10;
	msg->setDataArraySize(numOfElements);

	for (int i = 0; i < numOfElements; ++i) {
    	msg->setData(i, i*2); // Fill with some values
	}
	send(msg, "out", gateIndex);
	return;
}
*/

void Leader::sendData(int idDest)
{
    SetupMessage *msg = new SetupMessage();
    msg -> setAssigned_id(idDest);

    // Genero un numero casuale che indica la dimensione del vettore da inviare
    //int numElements = intuniform(1, 100);
    int numElements = 30;
    std::cout << "Num elements: " << numElements << "\n";

    msg -> setDataArraySize(numElements);
    std::cout << "Array: ";
    for(int j = 0; j < numElements; j++)
    {
        // Genero un valore casuale tra 1 e 100
        int value = intuniform(1, 5);
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

    // Definisco l'insieme delle possibili operazioni
    std::vector<std::string> operations = {"add", "sub", "mul", "div", "lt", "gt", "le", "ge", "changekey", "reduce"};
    int op_size = operations.size();
    std::cout << "Abbiamo " << op_size-1 << " operazioni"<< endl;

    bool reduceFound = false;

    // Genero la dimensione della schedule tra 1 e 15
    //int scheduleSize = intuniform(1, 100);
    int scheduleSize = 5;
    std::cout << "Schedule size: " << scheduleSize << endl;


    // Creo un array di stringhe vuoto e un array per i parametri
    std::vector<std::string> schedule(scheduleSize);
    schedule = {"add", "mul", "gt", "sub", "reduce"};
    std::vector<int> parameters(scheduleSize);

    for(int i = 0; i < scheduleSize; ++i)
    {
        // Genero un indice casuale per poi trovare l'operazione corrispondente all'indice
        int randomIndex = intuniform(1, op_size - 1);
        while(operations[randomIndex] == "reduce" && i != scheduleSize-1)
        {
            reduceFound = true;
            randomIndex = intuniform(1, op_size) - 1;
            std::cout << "Found: " << reduceFound << endl;
        }
        if(randomIndex != 9)
        {
            // Evito numeri negativi per via della divisione
            int param = intuniform(1, 10);
            parameters[i] = param;
            //schedule[i] = operations[randomIndex];
        }
    }

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
                schedule[scheduleSize-1] = operations[9];
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
