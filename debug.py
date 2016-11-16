from pandas import read_pickle
from sklearn.externals import joblib
from math import ceil
import pandas as pd
import re
from optimiser import Optimiser


dfk_path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/dfk.pickle"
model_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/clf.pickleSVR'
nodes = 5
memory_node = 21504

if __name__ == '__main__':
    opt = Optimiser(dfk_path,model_path,nodes,memory_node)
    # sequence = [['Grep',81],['Grep',128],['Grep',40]]
    sequence = [['Spark PCA Example',81],['Spark PCA Example',144],['Spark PCA Example',40],
                ['Grep',81],['Grep',128],['Grep',40],
                ['SupporVectorMachine',81],['SupporVectorMachine',144],['SupporVectorMachine',40],
                ['Spark PageRank Application',90],['Spark PageRank Application',159]]
    optimalduration = []
    list_of_confs = []
    for s in sequence:
        conf = opt.get_best_conf(s[0],s[1]) ## application and ntasks
        list_of_confs.append((conf,s[0],s[1]))
        optimalduration.append(opt.get_duration_app_and_conf(s[0],s[1],conf))
    nonoptimalduration = []
    bestduration = []
    best_real_confs = []
    for s in sequence:
        nonoptimalduration.append(opt.get_duration_app_and_conf(s[0],s[1],('1g','1')))
        best_real_confs.append(opt.get_real_best_conf(s[0],s[1]))
    bestduration = zip(*best_real_confs)[2]
    sum(bestduration)/float(sum(nonoptimalduration))
    sum(optimalduration)/float(sum(nonoptimalduration))
