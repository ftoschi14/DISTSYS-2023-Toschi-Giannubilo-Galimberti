simple Worker
{
    parameters:
        int numWorkers;
        int id = default(-1) @mutable;
        int batchSize;
        double failureProbability;
    gates:
        input in[];
        output out[];
}

simple Leader
{
    parameters:
        int numWorkers;
    gates:
        input in[];
        output out[];
}

network MapReduceNet
{
    parameters:
        int numWorkers;
    submodules:
        leader: Leader {
            numWorkers = default(parent.numWorkers);
        }

        worker[numWorkers]: Worker {
            numWorkers = default(parent.numWorkers);
        }
    connections allowunconnected:
        for i=0..numWorkers-1 {
            leader.in++ <-- {  delay = 100ms; } <-- worker[i].out++;
            leader.out++ --> {  delay = 100ms; } --> worker[i].in++;

        }
        for i=0..numWorkers-1, for j=0..numWorkers-1 {
            worker[i].out++ --> {  delay = 100ms; } --> worker[j].in++ if i!=j;
        }

}