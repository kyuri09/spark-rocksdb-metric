import sys
import pandas as pd

# python3 result_to_csv_addTimestamp.py RESULT_TEXT_FILE_NAME

if len(sys.argv) < 2:
    print("Need argument: File path to be parsed")
    sys.exit(1)
else:
    filePath = sys.argv[1]

resultDurationPerBatch = {"total_duration": []}
resultRocksdbGetLatency = {"get_latency": []}
resultTotalCommit = {"total_commit": []}
resultInputRows = {"input_rows": []}
with open(filePath, "r") as resultFile:
    for line in resultFile:
        if(line[0] == '*'):
            continue

        contents = line.split(' ')

        if "linear-road-Q2" in contents:
            query = "Q2"
            continue

        if ',' in contents[0]:
            split = contents[0].split(",")
            duration=split[0]
            rocksdbGetLatency=split[1].replace("\n","")
            resultDurationPerBatch["total_duration"].append(duration)
            continue

        if "TotalcommitTimeMs" in contents:
            totalCommit = contents[36]
            resultTotalCommit["total_commit"].append(totalCommit)

        if "rocksdbGetLatency" in contents:
            rocksdbGetLatency = contents[6]
            resultRocksdbGetLatency["get_latency"].append(rocksdbGetLatency)

        if "numInputRowSize" in contents:
            numInputRow = contents[48]
            resultInputRows["input_rows"].append(numInputRow)    	


durationPerBatchDF = pd.DataFrame({key: pd.Series(value) for key, value in resultDurationPerBatch.items()})
rocksdbGetLatencyDF = pd.DataFrame({key: pd.Series(value) for key, value in resultRocksdbGetLatency.items()})
totalCommitDF = pd.DataFrame({key: pd.Series(value) for key, value in resultTotalCommit.items()})
inputrowDF = pd.DataFrame({key: pd.Series(value) for key, value in resultInputRows.items()})

durationPerBatchDF.to_csv("./durationPerBatch.csv", sep=',', na_rep="NaN")
rocksdbGetLatencyDF.to_csv("./rocksdbGetLatency.csv", sep=',', na_rep="NaN")
totalCommitDF.to_csv("./totalCommit.csv", sep=',', na_rep="NaN")
inputrowDF.to_csv("./InputRows.csv", sep=',', na_rep="NaN")
