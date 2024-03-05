message DataInsertMessage 
{
	int destWorker;
	int reqID;
	int value;
	bool ack;
}