import shlex, subprocess
import requests
from random import shuffle
import socket
from time import sleep
import optimiser

spark_submit = "/opt/spark/bin/spark-submit"
commonjar = "/opt/spark/BenchMark-1.0-SNAPSHOT.jar"
root_to_spark_bench = "/home/abrandon/spark-bench/"
root_to_hibench = "/home/abrandon/HiBench-master"

path_to_dfk = 'mydfk'
path_to_model = 'mymodel'

opt = optimiser.Optimiser(path_to_dfk,path_to_model)


def measure_mean_energy(start,end):
    r = requests.get('http://' + socket.gethostname() + ':8088/ws/v1/cluster/nodes')
    listOfNodes = ''
    for node in r.json.get('nodes').get('node'):
        if node.get('nodeHostName')!=socket.gethostname():
            listOfNodes = listOfNodes + node.get('nodeHostName') + ','
    listOfNodes = listOfNodes.replace('.lyon.grid5000.fr','')[:-1]
    tries = 0
    while True:
        r = requests.get('http://kwapi.lyon.grid5000.fr:12000/power/timeseries/?from=' + str(start) + '&to=' + str(end) + '&only=' + listOfNodes)
        print('http://kwapi.lyon.grid5000.fr:12000/power/timeseries/?from=' + str(start) + '&to=' + str(end) + '&only=' + listOfNodes)
        global_energy = []
        for item in r.json.get('items'):
            global_energy.extend(item.get('values'))
        result = sum(global_energy)
        if (result==0):
            print 'Grid 5000 Api error. Will sleep 2 secs and try again'
            sleep(2)
            tries = tries + 1
            if (tries == 60):
                result = 0
                break
            else:
                continue
        else:
            break
    return result

# def measure_mean_energy_old(start,end): deprecated
#     r = requests.get('http://' + socket.gethostname() + ':8088/ws/v1/cluster/nodes')
#     listOfNodes = ''
#     for node in r.json.get('nodes').get('node'):
#         if node.get('nodeHostName')!=socket.gethostname():
#             listOfNodes = listOfNodes + node.get('nodeHostName') + ','
#     listOfNodes = listOfNodes.replace('.lyon.grid5000.fr','')[:-1]
#     tries = 0
#     while True:
#         try:
#             r = requests.get('http://kwapi.lyon.grid5000.fr:12000/power/timeseries/?from=' + str(start) + '&to=' + str(end) + '&only=' + listOfNodes)
#             print('http://kwapi.lyon.grid5000.fr:12000/power/timeseries/?from=' + str(start) + '&to=' + str(end) + '&only=' + listOfNodes)
#             global_energy = []
#             for item in r.json.get('items'):
#                 global_energy.extend(item.get('values'))
#             result = sum(global_energy)/len(global_energy)
#         except ZeroDivisionError:
#             print 'Grid 5000 Api error. Will sleep 2 secs and try again'
#             sleep(2)
#             tries = tries + 1
#             if (tries == 60):
#                 result = 0
#                 break
#             else:
#                 continue
#         break
#     return result



def generate_random_conf():
    #exCores = ["1","2","3","4","5","6"]
    #exMem = ["1g","2g","3g","4g","5g","6g"]
    shuffleCompress = ["true","false"]
    shuffleManager = ["sort","hash"]
    compressionCodec = ["snappy","lz4","lzf"]
    sparkMemoryFrac = ["0.75","0.80","0.90"]
    sparkStorageFrac = ["0.5","0.4","0.25"]
    shuffleBuffer = ["32k","128k","512k"]  #Default: 32K Size of the in-memory buffer for each shuffle file output stream. These buffers reduce the number of disk seeks and system calls made in creating intermediate shuffle files.
    #nExecutors = ["8","10","12","14","16","18","20","22","24","26","70"]
    shuffleSpillCompress = ["true","false"]
    GC = ["-XX:+UseParallelGC","-XX:+UseG1GC","-XX:+UseSerialGC","-XX:+UseConcMarkSweepGC"]
    reducerMaxSize = ["24m","48m","128m","256m"] #spark.reducer.maxSizeInFlight: Maximum size of map outputs to fetch simultaneously from each reduce task. Since each output requires us to create a buffer to receive it, this represents a fixed memory overhead per reduce task,
    preferDirectBufs = ["true","false"]          # so keep it small unless you have a large amount of memory.
    broadcastCompress = ["true","false"]
    compressRDD = ["true","false"]
    localityWait = ["3","0"]
    serializer = ["org.apache.spark.serializer.JavaSerializer","org.apache.spark.serializer.KryoSerializer"]
    #especulation = ["true","false"]
    #espec_quantile = ["0.75","0.6","0.4"]
    #espec_multiplier = ["1.5","2","2.5"]
    #shuffle(exCores)
    #shuffle(exMem)
    shuffle(shuffleCompress)
    shuffle(compressionCodec)
    shuffle(sparkMemoryFrac)
    shuffle(sparkStorageFrac)
    shuffle(shuffleBuffer)
    #shuffle(nExecutors)
    shuffle(shuffleSpillCompress)
    shuffle(GC)
    shuffle(reducerMaxSize)
    shuffle(preferDirectBufs)
    shuffle(broadcastCompress)
    shuffle(localityWait)
    #shuffle(especulation)
    #shuffle(espec_quantile)
    #shuffle(espec_multiplier)
    shuffle(shuffleCompress)
    shuffle(compressRDD)
    shuffle(shuffleManager)
    shuffle(serializer)
    # if exMem[0]=="512m":
    #     exCores=["1"]
    # if exMem[0]=="1g":
    #     if int(exCores[0]) > 3:
    #         exCores=["3"]
    # if exMem[0] in ("4g","5g"):
    #     exCores[0]=str(int(exCores[0]) + 2)
    random_conf = [#["spark.executor.cores",exCores[0]],["spark.executor.memory",exMem[0]]]
                    ["spark.shuffle.compress",shuffleCompress[0]],["spark.io.compression.codec",compressionCodec[0]],
                   ["spark.memory.fraction",sparkMemoryFrac[0]],["spark.memory.storageFraction",sparkStorageFrac[0]],["spark.shuffle.file.buffer",shuffleBuffer[0]],#["executor.num",nExecutors[0]],
                   ["spark.shuffle.spill.compress",shuffleSpillCompress[0]],["spark.executor.extraJavaOptions",GC[0]],["spark.reducer.maxSizeInFlight",reducerMaxSize[0]],
                   ["spark.shuffle.io.preferDirectBufs",preferDirectBufs[0]],["spark.broadcast.compress",broadcastCompress[0]],["spark.locality.wait",localityWait[0]],#["spark.speculation",especulation[0]],
                   ["spark.shuffle.manager",shuffleManager[0]],["spark.rdd.compress",compressRDD[0]],
                    ["spark.serializer",serializer[0]]]#["spark.speculation.multiplier",espec_multiplier[0]],["spark.speculation.quantile",espec_quantile[0]],
    return random_conf



def generateConf(conf):
    finalConf = []
    for pair in conf:
        if (pair[0] == "executor.num"): ## Number of executors to be allocated for the appp
            finalConf.append("--num-executors")
            finalConf.append(pair[1])
        else:
            finalConf.append("--conf")
            finalConf.append(pair[0] + "=" + pair[1])
    return finalConf


def buildPopenArgs(app,conf,params,path_to_jar,monitor):
    args = ['timeout','--kill-after=3s','35m', spark_submit,"--class"]
    args.append(app)
    args.extend(["--master","yarn","--deploy-mode","client"])
    if monitor:
        args.extend(["--packages","org.hammerlab:spark-json-relay:2.0.0"])
    args.extend(generateConf(conf))
    args.append(path_to_jar)
    args.extend(params)
    return args


#
#
# ./bin/spark-submit \
#   --class org.apache.spark.examples.SparkPi \
#   --master yarn \
#   --deploy-mode cluster \  # can be client for client mode
#   --executor-memory 20G \
#   --num-executors 50 \
#   /path/to/examples.jar \
#   1000

### LAUNCH DATA GENERATORS ###

def launchGenerateKMeans(conf, params): ## ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_POINTS} ${NUM_OF_CLUSTERS} ${DIMENSIONS} ${SCALING} ${NUM_OF_PARTITIONS}
    args = buildPopenArgs("kmeans_min.src.main.scala.KmeansDataGen",conf,params,root_to_spark_bench + 'KMeans/target/KMeansApp-1.0.jar',False)
    print args
    p = subprocess.call(args)

def launchGenerateLogistic(conf,params):  ## ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_EXAMPLES} ${NUM_OF_FEATURES} ${EPS} ${ProbOne} ${NUM_OF_PARTITIONS}
    args = buildPopenArgs("LogisticRegression.src.main.java.LogisticRegressionDataGen",conf,params,root_to_spark_bench + 'LogisticRegression/target/LogisticRegressionApp-1.0.jar',False)
    print args
    p = subprocess.call(args)

def launchGenerateLinear(conf,params):  ## ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_EXAMPLES} ${NUM_OF_FEATURES} ${EPS} ${INTERCEPTS} ${NUM_OF_PARTITIONS}
    args = buildPopenArgs("LinearRegression.src.main.java.LinearRegressionDataGen",conf,params,root_to_spark_bench + 'LinearRegression/target/LinearRegressionApp-1.0.jar',False)
    print args
    p = subprocess.call(args)

def launchGeneratePCA(conf,params):  ## ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_SAMPLES} ${NUM_OF_FEATURES} ${NUM_OF_PARTITIONS}
    args = buildPopenArgs("PCA.src.main.scala.PCADataGen",conf,params,root_to_spark_bench + 'PCA/target/PCAApp-1.0.jar',False)
    print args
    p = subprocess.call(args)

def launchGenerateDecisionTree(conf,params): # ${SPARK_MASTER} ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_EXAMPLES} ${NUM_OF_FEATURES}  ${NUM_OF_PARTITIONS}
    args = buildPopenArgs("com.abrandon.upm.GenerateSVMData",conf,params,commonjar,False)
    print args
    p = subprocess.call(args)

def launchGenerateGraph(conf, params): ## ${INOUT_SCHEME}${INPUT_HDFS} ${numV} ${NUM_OF_PARTITIONS} ${mu} ${sigma}
    args = buildPopenArgs("DataGen.src.main.scala.GraphDataGen",conf,params,root_to_spark_bench + 'common/target/Common-1.0.jar',False)
    print args
    p = subprocess.call(args)

def launchGenerateRandomText(conf, params): ## parametros son  outputfile totaldatasize numparallel
    args = buildPopenArgs("com.abrandon.upm.GenerateRandomText",conf,params,commonjar,False)
    print args
    p = subprocess.call(args)

def launchGenerateSVMData(conf, params): # outputfile, nexamples, nfeatures, partitions
    args = buildPopenArgs("com.abrandon.upm.GenerateSVMData",conf,params,commonjar,False)
    print args
    p = subprocess.call(args)

def launchGenerateRDDRelational(conf,params):## nkeys savepath npartitions. This is from my benchmark
    args = buildPopenArgs("com.abrandon.upm.GenerateRDDRelation",conf,params,commonjar,False)
    print args
    p = subprocess.call(args)


### LAUNCH DATA GENERATORS ### {END}


def launchGrep(conf,params):
    args = buildPopenArgs("com.abrandon.upm.Grep",conf,params,commonjar,True)
    print args
    p = subprocess.call(args)

def launchKMeansBench(conf,params):
    args = buildPopenArgs("com.abrandon.upm.KMeansBench",conf,params,commonjar,True)
    p = subprocess.call(args)

def launchPageRank(conf,params):
    args = buildPopenArgs("com.abrandon.upm.PageRank",conf,params,commonjar,True)
    print args
    p = subprocess.call(args)

def launchSort(conf,params):
    args = buildPopenArgs("com.abrandon.upm.Sort",conf,params,commonjar,True)
    print args
    p = subprocess.call(args)

def launchSupportVectorMachine(conf,params):
    args = buildPopenArgs("com.abrandon.upm.SupportVectorMachine",conf,params,commonjar,True)
    print args
    p = subprocess.call(args)

def launchWordCount(conf,params):
    args = buildPopenArgs("com.abrandon.upm.WordCount",conf,params,commonjar,True)
    print args
    p = subprocess.call(args)

def launchGroupByTest(conf,params):
    args = buildPopenArgs("com.abrandon.upm.GroupByTest",conf,params,commonjar,True)
    print args
    p = subprocess.call(args)

def launchRDDRelational(conf,params):
    args = buildPopenArgs("com.abrandon.upm.RDDRelation",conf,params,commonjar,True)
    print args
    p = subprocess.call(args)

def launchNgrams(conf,params):
    args = buildPopenArgs("com.abrandon.upm.NGramsExample",conf,params,commonjar,True)
    print args
    p = subprocess.call(args)

#### HERE THE SPARK-BENCH BENCHMARK STARTS

def launchConnectedComponent(conf,params): ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} LAST PARAMETER IS USELESS
    args = buildPopenArgs("src.main.scala.ConnectedComponentApp",conf,params,root_to_spark_bench + "ConnectedComponent/target/ConnectedComponentApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchDecisionTrees(conf,params): ##${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS_Classification} ${NUM_OF_CLASS_C} ${impurityC} ${maxDepthC} ${maxBinsC} ${modeC}
    args = buildPopenArgs("DecisionTree.src.main.java.DecisionTreeApp",conf,params,root_to_spark_bench + "DecisionTree/target/DecisionTreeApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchKMeans(conf,params):
    args = buildPopenArgs("KmeansApp",conf,params,root_to_spark_bench + "KMeans/target/KMeansApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchPageRank(conf,params):  ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} ${MAX_ITERATION} ${TOLERANCE} ${RESET_PROB} ${STORAGE_LEVEL}
    args = buildPopenArgs("src.main.scala.pagerankApp",conf,params,root_to_spark_bench + "PageRank/target/PageRankApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchPCA(conf,params,optimise): ## ${INOUT_SCHEME}${INPUT_HDFS} ${DIMENSIONS}
    if optimise:
        conf = opt.get_best_conf('PCA',128)
    args = buildPopenArgs("PCA.src.main.scala.PCAApp",conf,params,root_to_spark_bench + "PCA/target/PCAApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchShortestPaths(conf,params): ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} ${numV}
    args = buildPopenArgs("src.main.scala.ShortestPathsApp",conf,params,root_to_spark_bench + "ShortestPaths/target/ShortestPathsApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchTriangleCount(conf,params): ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} ${STORAGE_LEVEL}
    args = buildPopenArgs("src.main.scala.triangleCountApp",conf,params,root_to_spark_bench + "TriangleCount/target/TriangleCountApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchSVDPlus(conf,params):  ##${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} ${NUM_ITERATION} ${RANK} ${MINVAL} ${MAXVAL} ${GAMMA1} ${GAMMA2} ${GAMMA6} ${GAMMA7} ${STORAGE_LEVEL}
    args = buildPopenArgs("src.main.scala.SVDPlusPlusApp",conf,params,root_to_spark_bench + "SVDPlusPlus/target/SVDPlusPlusApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchLinearRegression(conf,params): # ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${MAX_ITERATION}
    args = buildPopenArgs("LinearRegression.src.main.java.LinearRegressionApp",conf,params,root_to_spark_bench + "LinearRegression/target/LinearRegressionApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchLogisticRegression(conf,params):
    args = buildPopenArgs("LogisticRegression.src.main.java.LogisticRegressionApp",conf,params,root_to_spark_bench + "LogisticRegression/target/LogisticRegressionApp-1.0.jar",True)
    print args
    p = subprocess.call(args)

def launchStronglyConnnectedComponent(conf,params): #${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS}
    args = buildPopenArgs("src.main.scala.StronglyConnectedComponentApp",conf,params,root_to_spark_bench + "StronglyConnectedComponent/target/StronglyConnectedComponentApp-1.0.jar",True)
    print args
    p = subprocess.call(args)



#### THIS IS FROM THE HI-BENCH BENCHMARK

def launchScanSQL(conf,params):
    args = buildPopenArgs("")
    return 0

def launchJoinSQL(conf,params):
    return 0

def launchAggSQL(conf,params):
    return 0


