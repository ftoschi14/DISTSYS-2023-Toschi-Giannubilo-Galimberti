#include <stdio.h>
#include <string.h>
#include <omnetpp.h>

using namespace omnetpp;

class Worker : public cSimpleModule{
protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
};

Define_Module(Worker);

void Worker::initialize(){
	// TODO
}

void Worker::handleMessage(cMessage *msg){
	// TODO
}