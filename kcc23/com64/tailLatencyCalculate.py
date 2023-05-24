import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sys
import os
from subprocess import PIPE, Popen

if len(sys.argv) < 2:
    print("Need File to parse!")
    sys.exit(1)
else:
    filename_FIFO = sys.argv[1]
    number_query = sys.argv[2] # Query[2] : 1, Query[3] : 2, Query[4] : 3
#    filename_EDF = sys.argv[3]
#    filename_RDAS = sys.argv[4]
array_seq=0

if number_query == "2":
    array_seq=1
elif number_query == "3":
    array_seq=2
elif number_query == "4":
    array_seq=3

path_FIFO = os.path.realpath(filename_FIFO)
#path_FAIR = os.path.realpath(filename_FAIR)
#path_EDF = os.path.realpath(filename_EDF)
#path_RDAS = os.path.realpath(filename_RDAS)
#parsed_fileName = filename_FIFO.split("-")
new_fileName = "Query"+number_query+"_tailLatency"
put = Popen(["touch", new_fileName], stdin=PIPE, bufsize=-1)
put.communicate()
path1 = os.path.realpath(new_fileName)

list_FIFO = []
latencies_FIFO = []

#list_FAIR = []
#latencies_FAIR = []

#list_EDF = []
#latencies_EDF = []

#list_RDAS = []
#latencies_RDAS = []

with open(path_FIFO,"r") as fp:
    next(fp) # Neglect some initial values
    next(fp)
    next(fp)
    next(fp)
    next(fp)
    next(fp)
    for line in fp:
        ls = line.replace(")","")
        ls = line.replace("(","")
        list_FIFO.append(line.split(","))

for i in list_FIFO:
    temp = float(i[array_seq])
    latencies_FIFO.append(temp)

cdfx_FIFO = np.sort(latencies_FIFO)
cdfy_FIFO = np.linspace(1 / len(latencies_FIFO), 1.0, len(latencies_FIFO))

#for latency, prob in zip(cdfx_FIFO, cdfy_FIFO):
#    print(str(latency) + "\t" + str(prob))

cdfx_under90_list = []
cdfx_upper90_list = []

cdfy_under90_list = []
cdfy_upper90_list = []

for latency, prob in zip(cdfx_FIFO, cdfy_FIFO):
    if(  prob < 0.9 ):
        cdfy_under90_list.append(prob)
        cdfx_under90_list.append(latency)
    else:
        cdfy_upper90_list.append(prob)
        cdfx_upper90_list.append(latency)
a = 0.1
total = 0.0
num = 0
under90_dict = {}
upper90_dict = {}

for latency, prob in zip(cdfx_upper90_list, cdfy_upper90_list):
    upper90_dict[prob] = latency

for latency, prob in zip(cdfx_under90_list, cdfy_under90_list):
    total += latency
    num += 1
    if prob > a:
        under90_dict[round(a,1)] = total/num
        a += 0.1
        total = 0.0
        num = 0


new_dict_list = list(zip(under90_dict.keys(),under90_dict.values()))
new_dict_list.sort(key=lambda x: x[0])
total_dict_list = new_dict_list + list(zip(upper90_dict.keys(), upper90_dict.values()))
total_dict_list.sort(key=lambda x: x[0])

with open(path1, "w") as fp_path:
    for prob, latency in zip(cdfy_FIFO, cdfx_FIFO):
        fp_path.write(str(prob)+ '\t' + str(latency) + '\n')
