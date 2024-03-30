message CheckChangeKeyAckMessage
{
    int workerId;
    
    int partialRes;

    int partialVector[];
    
    int changeKeySent;
    int changeKeyReceived;
}