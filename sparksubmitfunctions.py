import shlex, subprocess

spark_submit = "/opt/spark/bin/spark-submit"
path_to_jar = "/opt/spark/BenchMark-1.0-SNAPSHOT.jar"



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


def buildPopenArgs(app,conf,params):
    args = [spark_submit,"--class"]
    args.append("com.abrandon.upm." + app)
    args.extend(["--master","yarn","--deploy-mode","client"])
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

def launchGenerateKMeans(conf, params):
    args = buildPopenArgs("GenerateKMeansData",conf,params)
    print args
    p = subprocess.Popen(args)

def launchGeneratePageRank(conf, params):
    args = buildPopenArgs("GeneratePageRank",conf,params)
    print args
    p = subprocess.Popen(args)

def launchGenerateRandomText(conf, params):
    args = buildPopenArgs("GenerateRandomText",conf,params)
    print args
    p = subprocess.Popen(args)

def launchGenerateSVMData(conf, params):
    args = buildPopenArgs("GenerateSVMData",conf,params)
    print args
    p = subprocess.Popen(args)

def launchGrep(conf,params):
    args = buildPopenArgs("Grep",conf,params)
    print args
    p = subprocess.Popen(args)

def launchkMeansBench(conf,params):
    args = buildPopenArgs("kMeansBench",conf,params)
    p = subprocess.Popen(args)

def launchPageRank(conf,params):
    args = buildPopenArgs("PageRank",conf,params)
    print args
    p = subprocess.Popen(args)

def launchSort(conf,params):
    args = buildPopenArgs("Sort",conf,params)
    print args
    p = subprocess.Popen(args)

def launchSupportVectorMachine(conf,params):
    args = buildPopenArgs("SupportVectorMachine",conf,params)
    print args
    p = subprocess.Popen(args)

def launchWordCount(conf,params):
    args = buildPopenArgs("WordCount",conf,params)
    print args
    p = subprocess.Popen(args)






