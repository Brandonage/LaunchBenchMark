import sparksubmitfunctions as sp
import subprocess


conf = [["spark.executor.memory","7g"]]## The configuration for all text generators. Usually as big as possible

def calculatesizefor(app,size):  ## We are going to maintain a database of reference sizes for the different apps. Size has to be an int with the number of GB
    refpoints = 1000000 ## This are the reference number of points we used to infer the ratios in refsizedict
    refsizedict = {"words": 0.001, "SVM": 0.192, "KMeans":0.372, "Logistic":0.379, "Linear":0.135, "PCA":18.4,"DecisionTree":0.120,
                   "Relational":0.124,"Graph":1.9}
    refsize = refsizedict[app]
    npoints = (size*refpoints)/refsize
    return int(npoints)

def generateTextFile(size,file,nparallel):
    npoints = calculatesizefor("words",size)
    parametros = [file,str(npoints),nparallel] ## parametros son outputfile totaldatasize numparallel
    sp.launchGenerateRandomText(conf,parametros)

def generateSVMFile(size,file,nparallel): # # outputfile, nexamples, nfeatures, partitions
    npoints = calculatesizefor("SVM",size)
    parametros = [file, str(npoints), '10',nparallel]
    sp.launchGenerateSVMData(conf,parametros)

def generateKMeansFile(size,file,nparallel): # ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_POINTS} ${NUM_OF_CLUSTERS} ${DIMENSIONS} ${SCALING} ${NUM_OF_PARTITIONS}
    npoints = calculatesizefor("KMeans",size)
    parametros = [file, str(npoints), '10','20','0.6',nparallel]
    sp.launchGenerateKMeans(conf,parametros)

def generateLogisticFile(size,file,nparallel): ## ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_EXAMPLES} ${NUM_OF_FEATURES} ${EPS} ${ProbOne} ${NUM_OF_PARTITIONS}
    npoints = calculatesizefor("Logistic",size)
    parametros = [file,str(npoints),'20','0.5','0.2',nparallel]
    sp.launchGenerateLogistic(conf,parametros)

def generateLinearRegressionFile(size,file,nparallel):## ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_EXAMPLES} ${NUM_OF_FEATURES} ${EPS} ${INTERCEPTS} ${NUM_OF_PARTITIONS}
    npoints = calculatesizefor("Linear",size)
    parametros = [file,str(npoints),'6','0.5','0.1',nparallel]
    sp.launchGenerateLinear(conf,parametros)

def generatePCAFile(size,file,nparallel): ## ${INOUT_SCHEME}${INPUT_HDFS} ${NUM_OF_SAMPLES} ${NUM_OF_FEATURES} ${NUM_OF_PARTITIONS}
    npoints = calculatesizefor("PCA",size)
    parametros = [file,str(npoints),'1000',nparallel]
    sp.launchGeneratePCA(conf,parametros)

def generateDecisionTreeFile(size,file,nparallel): #  # outputfile, nexamples, nfeatures, partitions
    npoints = calculatesizefor("DecisionTree",size)
    parametros = [file,str(npoints),'6',nparallel]
    sp.launchGenerateDecisionTree(conf,parametros)

def generateRelationalFile(size,file,nparallel): ## nkeys savepath npartitions. This is from my benchmark
    npoints = calculatesizefor("Relational",size)
    parametros = [str(npoints),file,nparallel]
    sp.launchGenerateRDDRelational(conf,parametros)

def generateGraphFile(size,file,nparallel): ## ${INOUT_SCHEME}${INPUT_HDFS} ${numV} ${NUM_OF_PARTITIONS} ${mu} ${sigma}
    npoints = calculatesizefor("Graph",size)
    parametros = [file,str(npoints),nparallel,'4.0','1.3']
    sp.launchGenerateGraph(conf,parametros)


def generate_batch(gigas,partitions,prefix):
    generateDecisionTreeFile(gigas,"/files/dec" + prefix + ".txt",partitions)
    #generateGraphFile(gigas,"/files/graphb.txt",partitions)
    generateKMeansFile(gigas,"/files/kmeans" + prefix + ".txt",partitions)
    generateLinearRegressionFile(gigas,"/files/linear" + prefix + ".txt",partitions)
    generateLogisticFile(gigas,"/files/logistic" + prefix + ".txt",partitions)
    generatePCAFile(gigas,"/files/pca" + prefix + ".txt",partitions)
    generateRelationalFile(gigas,"/files/rdd" + prefix + ".txt",partitions)
    generateSVMFile(gigas,"/files/svm" + prefix + ".txt",partitions)
    generateTextFile(gigas,"/files/words" + prefix + ".txt",partitions)


if __name__ == '__main__':
    generate_batch(gigas=22,partitions='16',prefix="b")
    generate_batch(gigas=10,partitions='9',prefix="m")