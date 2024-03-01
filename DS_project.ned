simple Worker
{
    parameters:
        int numWorkers;
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
        int workers = 4;
        @display("bgb=261,224");
    submodules:
        leader: Leader {
            numWorkers = defualt(workers);
        }

        worker[numWorkers]: Worker {

            @display("p=147,154");
        }
    connections allowunconnected:
        for i=0..workers-1 {
            leader.in++ <-- {  delay = 100ms; } <-- worker[i].out++;
            leader.out++ --> {  delay = 100ms; } --> worker[i].in++;

        }
        for i=0..workers-1, for j=0..workers-1 {
            worker[i].out++ --> {  delay = 100ms; } --> worker[j].in++;
            worker[i].in++ <-- {  delay = 100ms; } <-- worker[j].out++;
        }

}


