#include <iostream>
#include <map>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>

class InsertManager {
private:
	std::vector<int> insertedData; // List of new values
	std::map<int, int> senderReqMap; // Keep track of elaborated reqIDs from different senders
	std::string insertFilename;
	std::string requestFilename;
	int batchSize;
	int processingSize;
	int saveFrequency;
	int unstableCount;

	void saveData(){
		std::ofstream insertFile(insertFilename);
		if(!insertFile.is_open()){
			std::cerr << "Error opening insert file for writing." << std::endl;
			return;
		}

		for(int i = 0; i < insertedData.size(); i++){
			insertFile << insertedData[i] << std::endl;
		}
		insertFile.close();

		std::ofstream reqFile(requestFilename);
		if(!reqFile.is_open()){
			std::cerr << "Error opening request file for writing." << std::endl;
			return;
		}

		for(const auto &reqPair : senderReqMap) {
			reqFile << reqPair.first << ',' << reqPair.second << std::endl;
		}
		reqFile.close();
	}

	void loadData(){
		std::ifstream insertFile(insertFilename);
		if(!insertFile.is_open()) {
			std::cerr << "Error opening insert file for reading." << std::endl;
			return;
		}
		std::string line;
		while(std::getline(insertFile, line)) {
			insertedData.push_back(std::stoi(line));
		}

		insertFile.close();

		std::ifstream reqFile(requestFilename);
		if(!reqFile.is_open()) {
			std::cerr << "Error opening request file for reading." << std::endl;
			return;
		}

		while(std::getline(reqFile, line)) {
			std::istringstream iss(line);

			int senderID;
			int reqID;

			if(std::getline(iss, line, ',')) {
				senderID = std::stoi(line);
				if(iss >> reqID) {
					senderReqMap.insert({senderID, reqID});
				}
			}
		}
		reqFile.close();
	}

public:
	InsertManager() : insertFilename(""), requestFilename(""), batchSize(0), processingSize(0), saveFrequency(0), unstableCount(0) {
		std::cout << "WARNING: Using default InsertManager constructor - missing filenames and BatchSize";
	}

	InsertManager(const std::string& insertFilename, const std::string& requestFilename, int batchSize, int saveFrequency)
	: insertFilename(insertFilename), requestFilename(requestFilename), batchSize(batchSize), saveFrequency(saveFrequency) {

	}

	std::vector<int> getBatch() {
		// If a previous batch was being processed
		if(processingSize > 0) {
			// Consider the data successfully elaborated, thus remove and re-persist
			insertedData.erase(insertedData.begin(), insertedData.begin() + processingSize);
			saveData();
			processingSize = 0;
		}
		processingSize = insertedData.size() > batchSize ? batchSize : insertedData.size();

		std::vector<int>::const_iterator batchStart = insertedData.begin();
		std::vector<int>::const_iterator batchEnd = insertedData.begin() + processingSize;

		std::vector<int> batch(batchStart, batchEnd);
		return batch;
	}

	void insertValue(int senderID, int reqID, int value) {
		auto map_it = senderReqMap.find(senderID);
		// If we have already processed previous changeKeys from this worker, check if this is new data
		if(map_it != senderReqMap.end() && map_it->second >= reqID){
			std::cout << "DEBUG: Ignoring already inserted data: " << value << "From " << senderID << "With reqID: " << reqID << "\n";
			return;
		}
		senderReqMap[senderID] = reqID;
		insertedData.push_back(value);
		std::cout << "DEBUG: Inserted " << value << "From " << senderID << "With reqID: " << reqID << "\n";

		unstableCount++;

		if(unstableCount >= saveFrequency) {
			saveData();
			unstableCount = 0;
		}
	}
};
