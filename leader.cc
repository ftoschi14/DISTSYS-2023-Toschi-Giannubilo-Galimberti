#include <stdio.h>
#include <string.h>
#include <omnetpp.h>

using namespace omnetpp;

class Leader : public cSimpleModule{
protected:
	virtual void initialize() override;
	virtual void handleMessage(cMessage *msg) override;
};

Define_Module(Leader);

void Leader::initialize(){
	// TODO
}

void Leader::handleMessage(cMessage *msg){
	// TODO
}
