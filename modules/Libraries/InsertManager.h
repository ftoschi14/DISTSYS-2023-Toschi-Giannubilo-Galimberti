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
    std::map<int, std::vector<int>> insertedData; // Map of scheduleStep to list of new values
    std::map<int, int> senderReqMap; // Keep track of elaborated reqIDs from different senders
    std::string insertFilename;
    std::string requestFilename;
    int currentBatchSize;
    int batchSize;

    void saveData() {
        std::ofstream insertFile(insertFilename);
        if (!insertFile.is_open()) {
            std::cerr << "Error opening insert file for writing." << std::endl;
            return;
        }

        for (const auto& stepPair : insertedData) {
            for (const int value : stepPair.second) {
                insertFile << stepPair.first << ',' << value << std::endl;
            }
        }
        insertFile.close();

        std::ofstream reqFile(requestFilename);
        if (!reqFile.is_open()) {
            std::cerr << "Error opening request file for writing." << std::endl;
            return;
        }

        for (const auto& reqPair : senderReqMap) {
            reqFile << reqPair.first << ',' << reqPair.second << std::endl;
        }
        reqFile.close();
    }

    void loadData() {
        std::ifstream insertFile(insertFilename);
        if (!insertFile.is_open()) {
            std::cerr << "Error opening insert file for reading." << std::endl;
            return;
        }
        std::string line;
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

        std::ifstream reqFile(requestFilename);
        if (!reqFile.is_open()) {
            std::cerr << "Error opening request file for reading." << std::endl;
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
    InsertManager() : insertFilename(""), requestFilename(""), batchSize(0) {
        std::cout << "WARNING: Using default InsertManager constructor - missing filenames and BatchSize";
    }

    InsertManager(const std::string& insertFilename, const std::string& requestFilename, int batchSize)
        : insertFilename(insertFilename), requestFilename(requestFilename), batchSize(batchSize) {

        if (fs::exists(insertFilename) && fs::exists(requestFilename)) {
            loadData();
        } else {
            if (!fs::exists(insertFilename)) {
                std::cerr << "Insert file does not exist, starting with an empty dataset." << std::endl;
            }
            if (!fs::exists(requestFilename)) {
                std::cerr << "Request file does not exist, starting with an empty request log." << std::endl;
            }
        }
    }

    std::map<int, std::vector<int>> getBatch() {
    	if(currentBatchSize > 0) {
    		saveData(); // Assuming result was persisted, we can get rid of temporary data
    	}

        std::map<int, std::vector<int>> batch;
        currentBatchSize = 0;

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
    return batch;
    }


    void insertValue(int senderID, int reqID, int scheduleStep, int value) {
        auto map_it = senderReqMap.find(senderID);
        if (map_it != senderReqMap.end() && map_it->second >= reqID) {
            std::cout << "DEBUG: Ignoring already inserted data: " << value << " From " << senderID << " With reqID: " << reqID << "\n";
            return;
        }
        senderReqMap[senderID] = reqID;
        insertedData[scheduleStep].push_back(value);
        std::cout << "DEBUG: Inserted " << value << " From " << senderID << " With reqID: " << reqID << " At scheduleStep: " << scheduleStep << "\n";

        saveData();
    }
};
