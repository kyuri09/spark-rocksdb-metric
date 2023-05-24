import sys
import pandas as pd

# python3 result_to_csv_addTimestamp.py RESULT_TEXT_FILE_NAME

if len(sys.argv) < 2:
    print("Need argument: File path to be parsed")
    sys.exit(1)
else:
    filePath = sys.argv[1]

resultDurationPerBatch = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}
resultLatencyPerBatch = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}
resultLatencyPerTrafficSet = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}
resultTimestamp =  {"rawData": [], "Q2": [], "Q3": [], "Q4": []}
resultNumOfRows = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}
resultNumRowsTotal = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}
resultNumRowsUpdate = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}
resultUsedMemoryBytes = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}

# added
resultStateSize = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}
resultUpdateTimeMS = {"rawData": [], "Q2": [], "Q3": [], "Q4": []}

duration=0
rocksdbLatency=0

with open(filePath, "r") as resultFile:
    for line in resultFile:
        if(line[0] == '*'):
            continue

        contents = line.split(' ')
        
        if "linear-road-rawData" in contents:
            query = "rawData"
            continue
        elif "linear-road-Q2" in contents:
            query = "Q2"
            continue
        elif "linear-road-Q3" in contents:
            query = "Q3"
            continue
        elif "linear-road-Q4" in contents:
            query = "Q4"
            continue

        if ',' in contents[0]:
            split = contents[0].split(",") # split[0] : duration, split[1] : rocksdbLatnecy
            duration=split[0]
            rocksdbLatency=split[1].replace("\n","")
            continue

        resultTimestamp[query].append(rocksdbLatency)
        resultDurationPerBatch[query].append(duration)
#        resultLatencyPerBatch[query].append(portion)
"""
        if "Duration(Ms):" in contents:
            duration = contents[2][18:-1]
            resultDurationPerBatch[query].append(duration)
            continue
        
        if "Sets" in contents:
            numTrafficSets = contents[-1]
            continue

        if "-" in contents:
            latencyPerTrafficSet = contents[10][:-2]
            resultLatencyPerTrafficSet[query].append(latencyPerTrafficSet)
            continue

        if "Average" in contents:
            latencyPerBatch = contents[5][:-1]
            resultLatencyPerBatch[query].append(latencyPerBatch)
            continue

        if "Event" in contents:
            eventTimeStamp = contents[2][:-1]
            resultTimestamp[query].append(eventTimeStamp)
            continue

        if "NumOf" in contents:
            numOfRows = contents[2][:-1]
            resultNumOfRows[query].append(numOfRows)
            continue

        if "Total" in contents:
            numRowsTotal = contents[2][:-1]
            resultNumRowsTotal[query].append(numRowsTotal)
            continue

        if "Update" in contents:
            numRowsUpdate = contents[2][:-1]
            resultNumRowsUpdate[query].append(numRowsUpdate)
            continue

        if "Used" in contents:
            usedMemoryBytes = contents[2][:-1]
            # print(usedMemoryBytes)
            resultUsedMemoryBytes[query].append(usedMemoryBytes)
            continue
        
        if "CacheHit" in contents:
            stateOnCurrentVersionsSizeBytes = contents[2][1:-1].split("=")[1]
            # print(contents[2][1:-1].split("=")[1])
            resultStateSize[query].append(stateOnCurrentVersionsSizeBytes)
            continue

        if "allUpdate" in contents:
            updateTimeMS = contents[2][:-1]
            resultUpdateTimeMS[query].append(updateTimeMS)
            continue
"""

durationPerBatchDF = pd.DataFrame({key: pd.Series(value) for key, value in resultDurationPerBatch.items()})
#latencyPerBatchDF = pd.DataFrame({key: pd.Series(value) for key, value in resultLatencyPerBatch.items()})
#latencyPerTrafficSetDF = pd.DataFrame({key: pd.Series(value) for key, value in resultLatencyPerTrafficSet.items()})
eventTimestampDF = pd.DataFrame({key: pd.Series(value) for key, value in resultTimestamp.items()})
#numOfRowsDF = pd.DataFrame({key: pd.Series(value) for key, value in resultNumOfRows.items()})
#numRowsTotalDF = pd.DataFrame({key: pd.Series(value) for key, value in resultNumRowsTotal.items()})
#numRowsUpdateDF = pd.DataFrame({key: pd.Series(value) for key, value in resultNumRowsUpdate.items()})
#usedMemoryBytesDF = pd.DataFrame({key: pd.Series(value) for key, value in resultUsedMemoryBytes.items()})

# added
#resultStateSizeDF = pd.DataFrame({key: pd.Series(value) for key, value in resultStateSize.items()})
#resultUpdateTimeMSDF = pd.DataFrame({key: pd.Series(value) for key, value in resultUpdateTimeMS.items()})

durationPerBatchDF.to_csv("./durationPerBatch.csv", sep=',', na_rep="NaN")
#latencyPerBatchDF.to_csv("./avglatencyPerBatch.csv", sep=',', na_rep="NaN")
#latencyPerTrafficSetDF.to_csv("./latencyPerTrafficSet.csv", sep=',', na_rep="NaN")
eventTimestampDF.to_csv("./rocksdbLatencyPerBatch.csv", sep=',', na_rep="NaN")
#numOfRowsDF.to_csv("./numOfRows.csv", sep=',', na_rep="NaN")
#numRowsTotalDF.to_csv("./numRowsTotal.csv", sep=',', na_rep="NaN")
#numRowsUpdateDF.to_csv("./numRowsUpdate.csv", sep=',', na_rep="NaN")
#usedMemoryBytesDF.to_csv("./usedMemoryBytes.csv", sep=',', na_rep="NaN")

# added
#resultStateSizeDF.to_csv("./stateSize.csv", sep=',', na_rep="NaN")
#resultUpdateTimeMSDF.to_csv("./allupdatetime.csv", sep=',', na_rep="NaN")
