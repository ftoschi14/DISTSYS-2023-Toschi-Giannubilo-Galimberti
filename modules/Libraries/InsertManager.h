#include <iostream>
#include <map>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <filesystem>

namespace fs = std::filesystem;

class InsertManager {
private:
    // Data-related structures
    std::map<int, std::vector<int>> previousData; // Structure with previously requested batch (step, [values])
    std::map<int, std::vector<int>> insertedData; // Structure with inserted data (step, [values])
    std::map<int, int> senderReqMap; // Keep track of elaborated reqIDs from different senders
    
    //Backup information
    std::string insertFilename;
    std::string previousBatchFilename;
    std::string requestFilename;
    
    // Auxiliary information
    int currentBatchSize;
    int batchSize;

    // Clears temporary file with previous batch information
    void clearTempFile() {
        std::ofstream tempFile(previousBatchFilename, std::ofstream::trunc);
        tempFile.close();
        currentBatchSize = 0;
    }

    /*
    * Saves the currently requested batch to a temporary file
    */
    void saveCurrentBatch() {
        std::ofstream tempFile(previousBatchFilename);
        if (!tempFile.is_open()) {
            std::cout << "Error opening temp file for writing." << std::endl;
            return;
        }

        std::cout << "[CK] Saving: ";

        for (const auto& stepPair : previousData) {
            for (const int value : stepPair.second) {
                tempFile << stepPair.first << ',' << value << std::endl;
                std::cout << stepPair.first << ", " << value << " | ";
            }
        }
        std::cout << "\n";
        tempFile.close();
    }

    /*
    * Updates the file keeping track of inserted data.
    * Called after a batch is loaded
    */
    void updateInsertFile() {
        std::ofstream insertFile(insertFilename, std::ofstream::trunc);
        if (!insertFile.is_open()) {
            std::cout << "Error opening insert file for writing.\n";
            return;
        }

        for (const auto& stepPair : insertedData) {
            for (const int value : stepPair.second) {
                insertFile << stepPair.first << ',' << value << "\n";
            }
        }
        insertFile.close();
    }

    /*
    * Appends inserted data at the end of the insertFile
    */
    void appendData(int step, int value) {
        std::ofstream insertFile(insertFilename, std::ios::app);
        if (!insertFile.is_open()) {
            std::cout << "Error opening insert file for writing.\n";
            return;
        }

        // Append the new key-value pair.
        insertFile << step << ',' << value << "\n";

        insertFile.close();
    }

    /*
    * Updates the requests file
    */
    void updateReqFile() {
        std::ofstream reqFile(requestFilename);
        if (!reqFile.is_open()) {
            std::cout << "Error opening request file for writing.\n";
            return;
        }

        for (const auto& reqPair : senderReqMap) {
            reqFile << reqPair.first << ',' << reqPair.second << "\n";
        }
        reqFile.close();
    }

    /*
    * Loads all data from the files:
    *  - Requests information
    *  - Inserted data map
    *  - Previous batch information
    */
    void loadData() {
        std::string line;

        // Previous batch data
        if(fs::exists(previousBatchFilename)) {
            std::ifstream previousDataFile(previousBatchFilename, std::ios::binary);
            if (!previousDataFile.is_open()) {
                std::cout << "Error opening temp data file for reading.\n";
                return;
            }

            while (std::getline(previousDataFile, line)) {
                std::istringstream iss(line);
                std::string part;
                std::vector<int> parts;

                while (std::getline(iss, part, ',')) {
                    parts.push_back(std::stoi(part));
                }

                // Line format: "scheduleStep,value"
                if (parts.size() == 2) {
                    int scheduleStep = parts[0];
                    int value = parts[1];
                    previousData[scheduleStep].push_back(value);
                }
            }
            previousDataFile.close();

            for(int i = 0; i < previousData.size(); i++) {
                currentBatchSize += previousData[i].size(); // This remains 0 if we load no data
            }
            std::cout << "Read previous file - size " << currentBatchSize << "\n";
            printScheduledData();
        }

        // Inserted Data
        std::ifstream insertFile(insertFilename, std::ios::binary);
        if (!insertFile.is_open()) {
            std::cout << "Error opening insert file for reading.\n";
            return;
        }

        while (std::getline(insertFile, line)) {
            std::istringstream iss(line);
            std::string part;
            std::vector<int> parts;

            while (std::getline(iss, part, ',')) {
                parts.push_back(std::stoi(part));
            }

            // Line format: "scheduleStep,value"
            if (parts.size() == 2) {
                int scheduleStep = parts[0];
                int value = parts[1];
                insertedData[scheduleStep].push_back(value);
            }
        }
        insertFile.close();

        std::cout << "Loaded:\n";
        printInsertedData();

        // Request Log File
        std::ifstream reqFile(requestFilename, std::ios::binary);
        if (!reqFile.is_open()) {
            std::cout << "Error opening request file for reading.\n";
            return;
        }

        while (std::getline(reqFile, line)) {
            std::istringstream iss(line);
            std::string part;
            std::vector<int> parts;

            while (std::getline(iss, part, ',')) {
                parts.push_back(std::stoi(part));
            }

            // Line format: "senderID,reqID"
            if (parts.size() == 2) {
                int senderID = parts[0];
                int reqID = parts[1];
                senderReqMap[senderID] = reqID;
            }
        }
        reqFile.close();
    }


public:
    InsertManager() : insertFilename(""), requestFilename(""), previousBatchFilename(""), batchSize(0) {
        std::cout << "WARNING: Using default InsertManager constructor - missing filenames and BatchSize\n";
    }

    InsertManager(const std::string& insertFilename, const std::string& requestFilename, const std::string& previousBatchFilename, int batchSize)
        : insertFilename(insertFilename), requestFilename(requestFilename), previousBatchFilename(previousBatchFilename), batchSize(batchSize) {
            currentBatchSize = 0;
        if (fs::exists(insertFilename) && fs::exists(requestFilename)) {
            loadData();
        } else {
            if (!fs::exists(insertFilename)) {
                std::cout << "Insert file does not exist, starting with an empty dataset.\n";
            }
            if (!fs::exists(requestFilename)) {
                std::cout << "Request file does not exist, starting with an empty request log.\n" ;
            }
        }
    }

    /*
    * Fetch a batch of inserted data of maximum size batchSize.
    *
    * Returns:
    *  - Map (step, [values]) containing one batch of inserted data, at the corresponding schedule steps
    */
    std::map<int, std::vector<int>> getBatch() {
        std::map<int, std::vector<int>> batch;

        // If a previous batch was reloaded, return this
        if(currentBatchSize > 0) {
            batch = previousData;
            std::cout << "Previous data: \n";
            printScheduledData();
            return batch;
        }

        //While currentBatchSize < batchSize, get as much data as possible
        auto it = insertedData.begin();
        while (it != insertedData.end() && currentBatchSize < batchSize) {
            const int scheduleStep = it->first;
            std::vector<int>& values = it->second;
            
            // Determine the number of values to add from this scheduleStep
            int valuesToAdd = std::min(static_cast<int>(values.size()), batchSize - currentBatchSize);

            // Add these values to the batch
            std::vector<int> stepBatch(values.begin(), values.begin() + valuesToAdd);
            batch.insert({scheduleStep, stepBatch});
            
            // Adjust the batch size
            currentBatchSize += valuesToAdd;
            
            // Remove the added values from the original data
            values.erase(values.begin(), values.begin() + valuesToAdd);

            // If all values for this scheduleStep have been processed, remove the scheduleStep from insertedData
            if (values.empty()) {
                it = insertedData.erase(it);
            } else {
                it++; // Move to the next scheduleStep if there are remaining values
            }
        }
        // Update previousData as current batch being returned
        previousData = batch;
        // Save current batch in temp file
        saveCurrentBatch();
        // Remove current batch by overwriting insert file
        updateInsertFile();

        std::cout << "Current data: \n";
        printScheduledData();
        return batch;
    }


    /*
    * Inserts the specified value, from the specified senderID, at the specified scheduleStep, with specified request ID.
    * If this worker has already received some value with a greater reqID, from this same sender, the request is dropped.
    * Else, the data point is inserted, the reqID value is updated for this sender, and the new data point is appended to
    * the insert file.
    *
    * Parameters:
    *  - senderID: ID of the worker that sent the data point
    *  - reqID: Request ID of the exchange, used to check for duplicate values
    *  - scheduleStep: The step of the schedule at which this data point should be inserted
    *  - value: Value of the data point to insert
    */
    void insertValue(int senderID, int reqID, int scheduleStep, int value) {
        auto map_it = senderReqMap.find(senderID);
        // If we have already received something from this sender, and the last request had ID greater than
        // the current reqID, we can reject this because it is a duplicate.
        if (map_it != senderReqMap.end() && map_it->second >= reqID) {
            std::cout << "DEBUG: Ignoring already inserted data: " << value << " From " << senderID << " With reqID: " << reqID << "\n";
            return;
        }
        
        // Update the last seen reqID to the current one
        senderReqMap[senderID] = reqID;
        // Push the value in the insertedData map
        insertedData[scheduleStep].push_back(value);
        std::cout << "DEBUG: Inserted " << value << " From " << senderID << " With reqID: " << reqID << " At scheduleStep: " << scheduleStep << "\n";

        // Update files by appending value and updating last request seen
        appendData(scheduleStep, value);
        updateReqFile();
    }

    // Clears the temp file after successfully elaborating the current batch of changekey data
    void persistData() {
        if(currentBatchSize > 0) {
            clearTempFile(); // Assuming result was persisted, we can get rid of data points elaborated
        }
    }

    /*
    * Returns whether the insertedData queues are empty
    *
    * Returns:
    *  - true if the insertedData queues are empty
    */
    bool isEmpty(){
        for (const auto& pair : insertedData) {
            if (!pair.second.empty()) {
                // Found a non-empty deque
                return false;
            }
        }
        // All deques are empty
        return true;
    }

    void printScheduledData(){
        for(int i = 0; i<previousData.size(); i++){
            std::cout << "Step " << i << ": ";
            for(int j = 0; j < previousData[i].size(); j++){
                std::cout << previousData[i][j] << " ";
            }
        std::cout << "\n";
        }
    }

    void printInsertedData(){
        for(int i = 0; i<insertedData.size(); i++){
            std::cout << "Step " << i << ": ";
            for(int j = 0; j < previousData[i].size(); j++){
                std::cout << previousData[i][j] << " ";
            }
        std::cout << "\n";
        }
    }
};