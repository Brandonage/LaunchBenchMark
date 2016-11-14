
import sparksubmitfunctions as sp
import random
from random import shuffle
import time
import subprocess
import generatedata as generator

#### We keep track of the different inputs for the applications and their sizes
keyword = "pneumonalgia"
wordInput = {"medium": "/files/wordsm.txt", "big" : "/files/wordsb.txt", "extrasmall": "/files/wordsxs.txt", "small": "/files/wordss.txt"}
kMeansInput = {"medium": "/files/kmeansm.txt", "big": "/files/kmeansb.txt", "extrasmall": "/files/kmeansxs.txt", "small": "/files/kmeanss.txt"}
PCAInput = {"medium": "/files/pcam.txt", "big": "/files/pcab.txt", "extrasmall": "/files/pcaxs.txt", "small": "/files/pcas.txt"}
graphInput = {"medium": "/files/graphm.txt", "big": "/files/graphb.txt", "extrasmall": "/files/graphxs.txt", "small": "/files/graphs.txt"}
svmInput = {"medium": "/files/svmm.txt", "big": "/files/svmb.txt", "extrasmall": "/files/svmxs.txt", "small": "/files/svms.txt"}
decTreeInput = {"medium": "/files/decm.txt", "big": "/files/decb.txt", "extrasmall": "/files/decxs.txt", "small": "/files/decs.txt"}
linearInput = {"medium": "/files/linearm.txt", "big": "/files/linearb.txt", "extrasmall": "/files/linearxs.txt", "small": "/files/linears.txt"}
logisticInput = {"medium": "/files/logisticm.txt", "big": "/files/logisticb.txt", "extrasmall": "/files/logisticxs.txt", "small": "/files/logistics.txt"}
RDDInput = {"medium": "/files/rddm.txt", "big": "/files/rddb.txt", "extrasmall": "/files/rddxs.txt", "small": "/files/rdds.txt"}
GroupByInput =  {"medium": '7600000', "big":'15600000'}
GroupByPartitions = {"medium": '100', "big": '168'}

sizeingigas = {"medium":10,"big":22}
partitions_for_size = {"medium":"10","big":"16"}

### List of parameters for the spark-bench

defaultDecTree = [decTreeInput["medium"],'DecisionTreesOutput','10','gini','5','100','Classification'] ##${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS_Classification} ${NUM_OF_CLASS_C} ${impurityC} ${maxDepthC} ${maxBinsC} ${modeC}
defaultPageRank = [graphInput["medium"],'PageRankOutput','0','5','0.001','0.15','MEMORY_AND_DISK'] ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} ${MAX_ITERATION} ${TOLERANCE} ${RESET_PROB} ${STORAGE_LEVEL}
defaultPCA = [PCAInput["medium"],'50'] ## ${INOUT_SCHEME}${INPUT_HDFS} ${DIMENSIONS}
defaultShortest = [graphInput["medium"],'ShortestPathsOutput','0','10000'] ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} ${numV}
defaultTriangle = [graphInput["medium"],'TriangleCountOutput','0','MEMORY_AND_DISK'] ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} ${STORAGE_LEVEL}
defaultSVD = [graphInput["medium"],'SVDOutput','0','2','50','0.0','5.0','0.007','0.007','0.005','0.015','MEMORY_AND_DISK'] ##${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} ${NUM_ITERATION} ${RANK} ${MINVAL} ${MAXVAL} ${GAMMA1} ${GAMMA2} ${GAMMA6} ${GAMMA7} ${STORAGE_LEVEL}
defaultGrep = [wordInput["medium"],keyword,"grepOutput.txt","0"]
defaultWC = [wordInput["medium"],"WordCountOutput.txt","0"]
defaultSort = [wordInput["medium"],"SortOutput.txt","0"]
defaultConC = [graphInput["medium"],'ConnectedComponentOutput','0','0']  ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} LAST PARAMETER IS USELESS
defaultSVM = [svmInput["medium"],'3','0'] ##  sp.launchSupportVectorMachine  Input niterations y parallelism Este es el de mi Benchmark
defaultKmeans = [kMeansInput["medium"],'kMeansBenchOutput','10','4','1'] ##$ sp.launchKMeans() {INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_CLUSTERS} ${MAX_ITERATION} ${NUM_RUN}
defaultLinear = [linearInput["medium"],'LinearRegressionOutput','5'] ##${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${MAX_ITERATION}
defaultLogistic = [logisticInput["medium"],'LogisticOutput','5','MEMORY_AND_DISK']   ### ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS}  ${MAX_ITERATION} ${STORAGE_LEVEL}
defaultStronglyConnected = [graphInput["medium"],'ConnectedComponentOutput','0']
defaultGroupBy = [GroupByInput["medium"],'10000',GroupByPartitions["medium"]]   ### Three arguments: numKVPairs, valSize, numMappers
defaultRDDRelational = [RDDInput["medium"],"RDDOutput.txt"]
defaultNgrams = [wordInput["medium"],"NgramsOutput.txt"]

def change_defaults_to(size):
    defaultDecTree[0] = decTreeInput[size]
    defaultPageRank[0] = graphInput[size]
    defaultPCA[0] = PCAInput[size]
    defaultShortest[0] = graphInput[size]
    defaultTriangle[0] = graphInput[size]
    defaultSVD[0] = graphInput[size]
    defaultGrep[0] = wordInput[size]
    defaultWC[0] = wordInput[size]
    defaultSort[0] = wordInput[size]
    defaultConC[0] = graphInput[size]
    defaultSVM[0] = svmInput[size]
    defaultKmeans[0] = kMeansInput[size]
    defaultLinear[0] = linearInput[size]
    defaultLogistic[0] = logisticInput[size]
    defaultStronglyConnected[0] = graphInput[size]
    ### ppossibly define it separately defaultGroupBy[0] = defaultGroupBy[size]
    ###defaultGenDataRDD[0] = defaultGenDataRDD[size]
    defaultRDDRelational[0] = RDDInput[size]
    defaultNgrams[0] = wordInput[size]
    # defaultGroupBy[0] = GroupByInput[size]
    # defaultGroupBy[2] = GroupByPartitions[size]

def wipe_output():
    p = subprocess.Popen(['/opt/hadoop/bin/hdfs','dfs','-rm','-r','*Output.txt*'])

def wipe_files():
    p = subprocess.Popen(['/opt/hadoop/bin/hdfs','dfs','-rm','-r','/files/*'])


#### TILL HERE THEY ARE ALL AUXILIARY FUNCTIONS. HERE COMES THE EXPERIMENTS


def sla_experiment(): #### Launch three Apps at the same time. Let them run and then launch another one on top. Popen has to be changed
    conf = [["spark.executor.cores","1"],["spark.executor.memory","1g"],["executor.num","18"]] ## Also 1 executor has to fit in each node
    sp.launchSort(conf, [wordInput,"SortOutput.txt"+str(1),"32"])# Sort <data_file> <save_file>" + " [<slices>]
    sp.launchSort(conf, [wordInput,"SortOutput.txt"+str(2),"32"])
    sp.launchSort(conf, [wordInput,"SortOutput.txt"+str(3),"32"])
    time.sleep(10)
    sp.launchSort(conf, [wordInput,"SortOutput.txt"+str(4),"32"])




########################################################################################################################################################################
########################################################################################################################################################################
###ENERGY EXPERIMENTS###ENERGY EXPERIMENTS###ENERGY EXPERIMENTS###ENERGY EXPERIMENTS###ENERGY EXPERIMENTS###ENERGY EXPERIMENTS
########################################################################################################################################################################
########################################################################################################################################################################








########################################################################################################################################################################
########################################################################################################################################################################
###PARALELLISM EXPERIMENTS###PARALELLISM EXPERIMENTS###PARALELLISM EXPERIMENTS###PARALELLISM EXPERIMENTS###PARALELLISM EXPERIMENTS###PARALELLISM EXPERIMENTS
########################################################################################################################################################################
########################################################################################################################################################################


###CHANGING NPARTITIONS#################################################################################################################################################

def parallelism_concomponent(ntimes,start_part,increment):  ## for a 5.0GB it spans 50 partitions
    input = '/SparkBench/ConnectedComponent/Input' # A single file to be able to partition
    output = 'ConnectedComponent' # The output. Will be changed according to the iteration
    part = start_part # we start from the beginning point
    useless = '1000' ## Connected component doesn't use this parameter but we have to provide it
    conf = [] ## Empty conf. Everything in standard
    sp.launchConnectedComponent(conf,[input,output + str(-1),str(0),str(useless)])
    for iteration in xrange(ntimes):
        sp.launchConnectedComponent(conf,[input,output + str(iteration),str(part),str(useless)])  ## ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_PARTITIONS} LAST PARAMETER IS USELESS
        part += increment ## Increment the number of partitions to see the impact

def parallelism_SVM(ntimes,start_part,increment): # for a 19.2GB 153 partitions are spawned by default
    input = '/files/SVM.txt' ## have in mind that with 20GB it's 152 partitions with the standard block size. Meaning that we need at least more than 152 parts
    it = '3'
    part = start_part
    conf = []
    sp.launchSupportVectorMachine(conf,[input,it,str(0)]) ### first an execution by default
    for iteration in xrange(ntimes):
        sp.launchSupportVectorMachine(conf,[input,it,str(part)])
        part += increment


def parallelism_WC(ntimes,start_part,increment): # for a 18.6GB file it spans 149 partitions
    input = '/files/words8.txt' ## have in mind that with 20GB it's 152 partitions with the standard block size. Meaning that we need at least more than 152 parts
    part = start_part
    conf = []
    sp.launchWordCount(conf,[input,"WordCountOutput.txt"+str(-1),str(0)]) ### first an execution by default
    for iteration in xrange(ntimes):
        sp.launchWordCount(conf,[input,"WordCountOutput.txt"+str(iteration),str(part)])
        part += increment


def parallelism_sort(ntimes,start_part,increment): ## for a 18.6GB file it spans 149 partitions
    input = '/files/words8.txt' ## have in mind that with 20GB it's 152 partitions with the standard block size. Meaning that we need at least more than 152 parts
    part = start_part
    conf = []
    sp.launchSort(conf, [input,"SortOutput.txt"+str(-1),str(0)]) ### first an execution by default
    for iteration in xrange(ntimes):
        sp.launchSort(conf, [input,"SortOutput.txt"+str(iteration),str(part)])
        part += increment




def experiment_set_parallelism():
    ### We choose four apps: One graph(ConnectedComponents), one machine learning (SVM), one I/O (sort)
    ### We set different levels of parallelism for each one
    ### We check how it affects in terms of latency
    return 0


###CHANGING NUMBER OF CORES#####################################################################################################################################


def standard_configuration(): ## I want to see how applications behave with an standard configuration
    conf = []
    sp.launchSort(conf, defaultSort)
    sp.launchWordCount(conf,defaultWC)
    sp.launchGrep(conf,defaultGrep)
    sp.launchSupportVectorMachine(conf,defaultSVM)
    sp.launchKMeans(conf,defaultKmeans)
    sp.launchSVDPlus(conf,defaultSVD)
    sp.launchPageRank(conf,defaultPageRank)
    sp.launchShortestPaths(conf,defaultShortest)
    sp.launchLinearRegression(conf,defaultLinear)
    sp.launchLogisticRegression(conf,defaultLogistic)
    sp.launchPCA(conf,defaultPCA)


def cores_and_memory():
    sizes = [["big",17,"16","b"],["medium",10,"9","m"],["small",5,"5","s"]] ## Tuples of size of benchmark, gigas, partitions and prefix for the file names
    conf_list = [
                [['spark.executor.memory','1g'],['spark.executor.cores','1']],
                [['spark.executor.memory','2g'],['spark.executor.cores','1']],
                [['spark.executor.memory','2g'],['spark.executor.cores','2']],
                [['spark.executor.memory','2g'],['spark.executor.cores','3']],
                [['spark.executor.memory','3g'],['spark.executor.cores','1']],
                [['spark.executor.memory','3g'],['spark.executor.cores','4']],
                [['spark.executor.memory','4g'],['spark.executor.cores','1']],
                [['spark.executor.memory','4g'],['spark.executor.cores','3']],
                [['spark.executor.memory','4g'],['spark.executor.cores','6']]
                ]
    for s in sizes:
        change_defaults_to(s[0])
        print("benchmark for size " + s[0])
        generator.generate_batch(gigas=s[1],partitions=s[2],prefix=s[3])
        for conf in conf_list:
            sp.launchWordCount(conf,defaultWC)
            sp.launchSort(conf,defaultSort)
            sp.launchGrep(conf,defaultGrep)
            sp.launchLogisticRegression(conf,defaultLogistic)
            sp.launchLinearRegression(conf,defaultLinear)
            sp.launchKMeans(conf,defaultKmeans)
            sp.launchPCA(conf,defaultPCA)
            sp.launchSupportVectorMachine(conf,defaultSVM)
            sp.launchDecisionTrees(conf,defaultDecTree)
            sp.launchGroupByTest(conf,defaultGroupBy)
            sp.launchRDDRelational(conf,defaultRDDRelational)
            sp.launchNgrams(conf,defaultNgrams)
            wipe_output() ## clear the output so we don't fill the space
        wipe_files() ## Clear the files generated for the benchmark
        p = subprocess.Popen('/home/abrandon/ParallelismExperiment/export.sh')
    p = subprocess.Popen('/home/abrandon/ParallelismExperiment/export.sh')


def cores_and_memory_graphs():
    sizes = [["big",17,"20","b"],["medium",10,"15","m"],["small",5,"7","s"]] ## Tuples of size of benchmark, gigas, partitions and prefix for the file names
    conf_list_graph = [
            [['spark.executor.memory','1g'],['spark.executor.cores','1']],
            [['spark.executor.memory','2g'],['spark.executor.cores','1']],
            [['spark.executor.memory','3g'],['spark.executor.cores','4']],
            [['spark.executor.memory','4g'],['spark.executor.cores','1']],
            [['spark.executor.memory','4g'],['spark.executor.cores','3']],
            [['spark.executor.memory','6g'],['spark.executor.cores','1']],
            [['spark.executor.memory','7g'],['spark.executor.cores','2']],
            ]
    for s in sizes:
        change_defaults_to(s[0])
        print("benchmark for size " + s[0])
        generator.generateGraphFile(size=s[1],file="/files/graph" + s[3] + ".txt",nparallel=s[2])
        for conf in conf_list_graph:
            sp.launchPageRank(conf,defaultPageRank)
            sp.launchShortestPaths(conf,defaultShortest)
            sp.launchSVDPlus(conf,defaultSVD)
            sp.launchConnectedComponent(conf,defaultConC)
            sp.launchStronglyConnnectedComponent(conf,defaultStronglyConnected)
            sp.launchTriangleCount(conf,defaultTriangle)
            wipe_output() ## clear the output so we don't fill the space
        wipe_files() ## Clear the files generated for the benchmark
        p = subprocess.Popen('/home/abrandon/ParallelismExperiment/export.sh')
    p = subprocess.Popen('/home/abrandon/ParallelismExperiment/export.sh')

def experiment_for_paper(): ## this experiment has not been used
    conf = [['spark.executor.memory','1g'],['spark.executor.cores','1']]
    sizes = [["big",22,"30","b"],["medium",10,"16","m"],["small",5,"7","s"],["extrasmall",2.5,"3","xs"]]
    #generator.generateGraphFile(22,"/files/graphb.txt","30")
    for s in sizes:
        change_defaults_to(s[0])
        print("benchmark for size " + s[0])
        generator.generateGraphFile(size=s[1],file="/files/graph" + s[3] + ".txt",nparallel=s[2])
        generator.generateLinearRegressionFile(size=s[1],file="/files/linear" + s[3] + ".txt",nparallel=s[2])
        generator.generateTextFile(size=s[1],file="/files/words" + s[3] + ".txt",nparallel=s[2])
        sp.launchPageRank(conf,defaultPageRank)
        sp.launchLinearRegression(conf,defaultLinear)
        sp.launchWordCount(conf,defaultWC)
        wipe_output()
    for x in range(6):
        change_defaults_to("big")
        sp.launchWordCount(conf,defaultWC)
        wipe_output()
    p = subprocess.Popen('/home/abrandon/ParallelismExperiment/export.sh')

## in this experiment we want to prove that 1GB/1core is a good signature (Execute an application with more and more space in the executor.
# See that after some memory limit there is not a significant variation on the metrics).
#Also we want to see if for several executions of the same app the signature is similar
## we will use Sort, PCA, WordCount
def signature_consistency():
    generator.generateTextFile(size=10,file="/files/wordsm.txt",nparallel="16")
    generator.generatePCAFile(size=10,file="/files/pcam.txt",nparallel="16")
    conf_list = [[['spark.executor.memory','1g']],[['spark.executor.memory','2g']],[['spark.executor.memory','3g']],[['spark.executor.memory','4g']],[['spark.executor.memory','5g']]]
    for conf in conf_list:
        sp.launchPCA(conf,defaultPCA)
        sp.launchSort(conf,defaultSort)
        sp.launchWordCount(conf,defaultWC)
        wipe_output()
    conf = [['spark.executor.memory','1g']]
    for i in xrange(5):
        sp.launchPCA(conf,defaultPCA)
        sp.launchSort(conf,defaultSort)
        wipe_output()
    p = subprocess.Popen('/home/abrandon/ParallelismExperiment/export.sh')

### in this experiment we want to prove that the metrics of a sample size in a few nodes have the same kind of metrics
def sample_sizes():
    conf = [['spark.executor.memory','1g'],['spark.executor.cores','1']]
    sizes = [["big",16,"20","b"],["small",3,"4","s"]]
    for s in sizes:
        change_defaults_to(s[0])
        print("benchmark for size " + s[0])
        generator.generateGraphFile(size=s[1],file="/files/graph" + s[3] + ".txt",nparallel=s[2])
        generator.generateLinearRegressionFile(size=s[1],file="/files/linear" + s[3] + ".txt",nparallel=s[2])
        generator.generateTextFile(size=s[1],file="/files/words" + s[3] + ".txt",nparallel=s[2])
        generator.generatePCAFile(size=s[1],file="/files/pca" + s[3] + ".txt",nparallel=s[2])
        sp.launchPageRank(conf,defaultPageRank)
        sp.launchSVDPlus(conf,defaultSVD)
        sp.launchLinearRegression(conf,defaultLinear)
        sp.launchPCA(conf,defaultPCA)
        sp.launchWordCount(conf,defaultWC)
        sp.launchSort(conf,defaultSort)
        wipe_output()
        raw_input("Change the configuration of the number of nodes and press enter....")
    p = subprocess.Popen('/home/abrandon/ParallelismExperiment/export.sh')

# In this experiment we want to prove that the best configuration for one application holds. We will use only one graph, one text and one ML app.
def best_configuration_is_constant():
    conf_list = [
            [['spark.executor.memory','1g'],['spark.executor.cores','1']],
            [['spark.executor.memory','2g'],['spark.executor.cores','1']],
            [['spark.executor.memory','3g'],['spark.executor.cores','4']],
            [['spark.executor.memory','4g'],['spark.executor.cores','3']],
            [['spark.executor.memory','6g'],['spark.executor.cores','1']],
            [['spark.executor.memory','7g'],['spark.executor.cores','2']],
            ]
    sizes = [["big",12,"16","b"]]
    for s in sizes:
        change_defaults_to(s[0])
        print("benchmark for size " + s[0])
        generator.generateGraphFile(size=s[1],file="/files/graph" + s[3] + ".txt",nparallel=s[2])
        generator.generateLinearRegressionFile(size=s[1],file="/files/linear" + s[3] + ".txt",nparallel=s[2])
        generator.generateTextFile(size=s[1],file="/files/words" + s[3] + ".txt",nparallel=s[2])
        for iter in xrange(4):
            for conf in conf_list:
                sp.launchPageRank(conf,defaultPageRank)
                sp.launchLinearRegression(conf,defaultLinear)
                sp.launchGrep(conf,defaultGrep)
                sp.launchSort(conf,defaultSort)
                wipe_output()
    p = subprocess.Popen('/home/abrandon/ParallelismExperiment/export.sh')



def evaluation_benchmark_batch(optimise_switch,conf):
    sp.launchPCA(conf,defaultPCA,optimise=optimise_switch)
    sp.launchSVDPlus(conf,defaultSVD,optimse=optimise_switch)
    sp.launchSupportVectorMachine(conf,defaultSVM,optimise=optimise_switch)
    sp.launchGrep(conf,defaultGrep,optimise=optimise_switch)







    #print(sp.buildPopenArgs("kMeansBench",conf,[wordInput,keyword,"grepOutput.txt"]))




def launchRandomBatch(iteration):
    nApps = 3 ## Number of Apps we will launch
    order = [x for x in range(6)]#range(nApps)] ## Order in which we will launch the apps we will randomise this
    shuffle(order)
    for i in range(nApps):
        conf = sp.generate_random_conf()
        if order[i] == 0:
            sp.launchGrep(conf,[wordInput,keyword,"grepOutput.txt"+str(iteration),parallelism])
        elif order[i] == 1:
            sp.launchSort(conf,[wordInput,"SortOutput.txt"+str(iteration),parallelism])
        elif order[i] == 2:
            sp.launchWordCount(conf,[wordInput,"WordCountOutput.txt"+str(iteration),parallelism])
        elif order[i] == 3:
            sp.launchKMeansBench(conf,[kMeansInput,"4","3",parallelism])
        elif order[i] == 4:
            sp.launchSupportVectorMachine(conf,[svmInput,"3",parallelism])
        elif order[i] == 5:
            sp.launchPageRank(conf,[graphInput,"3","PageRankOutput.txt"+str(iteration),parallelism])
        time.sleep(random.randrange(30,60,2)) ## We sleep for some time before launching the next app


if __name__ == '__main__':
    cores_and_memory()




## A mock configuration to be launched in console mode
# conf = [["spark.executor.cores","2"],["spark.executor.memory","2g"],["spark.yarn.executor.memoryOverhead","512"],
# ["io.compression.codec","snappy"],["spark.reducer.maxSizeInFlight","128m"],["spark.shuffle.compress","false"],["spark.memory.fraction","0.85"]]
    

