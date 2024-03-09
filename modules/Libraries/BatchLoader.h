#include <string>
#include <iostream>
#include <fstream>
#include <vector>

class BatchLoader {
private:
	std::string fileName;
	std::string fileProgressName;
	int batchSize;
	std::streampos filePosition;
	bool finished;

	void loadProgress() {
		//Load the last persisted file position from the progress file
		std::ifstream progressFile(fileProgressName, std::ios::binary);
		if(progressFile.is_open()){
			long long tmp;
			progressFile >> tmp;
			if(!progressFile.fail()){ //Successful read
				// Casting is needed due to compiler restrictions
				filePosition = static_cast<std::streampos>(tmp);
			}
			progressFile.close();
		}
	}

	void saveProgress() {
	    // Load Worker's progress file in binary mode
	    std::ofstream progressFile(fileProgressName, std::ios::binary);
	    if (progressFile.is_open()) {
	        // Update saved position
	        long long tmp = static_cast<long long>(filePosition);
	        progressFile << tmp;
	        progressFile.close();
	    }
	}

	int extractValue(const std::string& line){
		// Extract and store the value part of the key-value pair
        std::istringstream lineStream(line);
        std::string key, valueStr;
        int value = -1;
        if (std::getline(lineStream, key, ',') && std::getline(lineStream, valueStr)) { // Read up to the
            value = std::stoi(valueStr);
        }
        return value;
	}
public:
	BatchLoader() : fileName(""), fileProgressName(""), batchSize(0), filePosition(0) {
	}

	BatchLoader(const std::string& fileName, const std::string& fileProgressName, int batchSize) 
	: fileName(fileName), fileProgressName(fileProgressName), batchSize(batchSize), filePosition(0) {
		loadProgress(); // Load progress previous to crash
	}

	std::vector<int> loadBatch() {
		std::ifstream file(fileName);
		if(!file.is_open()){
			std::cerr << "Failed to open file: " << fileName << std::endl;
			return {};
		}
		saveProgress(); // Save current position before reading next N lines
		file.seekg(filePosition); // Move to the last processed batch

		if (!file) {
		    std::cerr << "Failed to seek to position: " << filePosition << std::endl;
		    return {};
		}

		std::vector<int> batchValues;
		std::string line;
		int linesRead = 0;

		while (linesRead < batchSize && std::getline(file, line, '\n')) {
	        // In binary mode, manually handle carriage returns if present
	        if (!line.empty() && line.back() == '\r') {
	            line.pop_back(); // Throw away \r
	        }

	        // Extract and store the value part of the key-value pair
	        int value = extractValue(line);
	        batchValues.push_back(value);

	        linesRead++;
	    }

		// After reading
		if (!file.eof() && file.fail()) {
		    file.clear(); // Clears the failbit
		}
		filePosition = file.tellg();

		file.close();
		return batchValues;
	}
};
