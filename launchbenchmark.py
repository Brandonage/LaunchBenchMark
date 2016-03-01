
import sparksubmitfunctions as sp




def generate_files():
    conf = [["spark.executor.cores","2"],["spark.executor.memory","2g"],["garbage.collector","UseG1GC"],["io.compression.codec","snappy"]]
    sp.launchGenerateKMeans(conf,["kMeans","500000000","4","5","0.9","16"])
    sp.launchGeneratePageRank(conf,["500000000","500000000","pageRank","16"])
    sp.launchGenerateRandomText(conf,["randomText","5000000000","16"])
    sp.launchGenerateSVMData(conf,["SVMdata","500000000","4","16"])

def launchBatch(conf,iteration):
    wordInput = "/files/words.txt"
    keyword = "pneumonalgia"
    kMeansInput = "/files/kmeans.txt"
    pageRankInput = "/files/pagerank.txt"
    svmInput = "/files/svm.txt"
    z # Grep <data_file> <keyword> <save_file>" + " [<slices>]
    sp.launchGenerateKMeans(conf,[kMeansInput,"4","20","16"]) # KMeans <data_file> <k> <iterations>" + " [<slices>]
    sp.launchPageRank(conf,[pageRankInput,"20","PageRankOutput.txt"+str(iteration),"16"]) # PageRank <file> <number_of_iterations> <save_path> [<slices>]
    sp.launchSort(conf, [wordInput,"SortOutput.txt"+str(iteration),"16"]) # Sort <data_file> <save_file>" + " [<slices>]
    sp.launchSupportVectorMachine(conf,[svmInput,"20","16"]) # SupportVectorMachine " + "<input_file> <num_iterations> <slices>
    sp.launchWordCount(conf,[wordInput,"WordCountOutput.txt"+str(iteration),"16"]) # WordCount <data_file> <save_file>" +" [<slices>]




def launchBenchmark():
    iteration = 0

    drCores = ["1","2","4","6"]
    exCores = ["1","2","4","6"]
    exMemOverhead =  [0.10, 0.05, 0.20]
    drMemOverhead = [0.10, 0.05, 0.20]
    exMem = ["1g","2g","4g","8g"]
    drMem = ["1g","2g","4g","8g"]
    reducerMaxSize = ["24m","48m","128m","240m"]
    shuffleCompress = ["true","false"]
    shuffleBuffer = ["32k","128k","512k","1m"]
    shuffleManager = ["sort","hash"]
    shuffleSpillCompress = ["true","false"]
    compressionCodec = ["snappy","lz4","lzf"]
    memFraction = ["0.75","0.60","0.85"]
    storageMemFraction = ["0.5","0.4","0.6"]
    exHeartBeat = ["10s","20s","25s"]
    taskCPU = ["1","2"]

    for drc in drCores:
        for exc in exCores:
            for emo in exMemOverhead:
                for dmo in drMemOverhead:
                    for em in exMem:
                        for dm in drMem:
                            for rMaxSize in reducerMaxSize:
                                for sComp in shuffleCompress:
                                    for sBuff in shuffleBuffer:
                                        for sMan in shuffleManager:
                                            for sSComp in shuffleSpillCompress:
                                                for cCod in compressionCodec:
                                                    for mFrac in memFraction:
                                                        for stoMfrac in storageMemFraction:
                                                            for eHB in exHeartBeat:
                                                                for cpu in taskCPU:
                                                                    ++iteration
                                                                    launchBatch([["spark.driver.cores",drc],["spark.executor.cores",exc],["spark.yarn.executor.memoryOverhead",emo],["spark.yarn.driver.memoryOverhead",dmo],
                                                                                ["spark.executor.memory",em],["spark.driver.memory",dm],["spark.reducer.maxSizeInFlight",rMaxSize],["spark.shuffle.compress",sComp],
                                                                                ["spark.shuffle.file.buffer",sBuff],["spark.shuffle.manager",sMan],["spark.shuffle.spill.compress",sSComp],["spark.io.compression.codec",cCod],
                                                                                ["spark.memory.fraction",mFrac],["spark.memory.storageFraction",stoMfrac],["spark.executor.heartbeatInterval",eHB],["spark.task.cpus",cpu]],iteration)




## A mock configuration to be launched in console mode
# conf = [["spark.executor.cores","2"],["spark.executor.memory","2g"],["spark.yarn.executor.memoryOverhead","512"],
# ["io.compression.codec","snappy"],["spark.reducer.maxSizeInFlight","128m"],["spark.shuffle.compress","false"],["spark.memory.fraction","0.85"]]
    

