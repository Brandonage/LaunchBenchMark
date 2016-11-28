import optimiser
import pandas as pd


dfk_path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/dfk.pickle"
dfapps_path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/dfapps.pickle"
model_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/clf.pickle'
cluster_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/cluster.pickle'
normaliser_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/normaliser.pickle'
benchmark_apps = ['Spark PCA Example','SupporVectorMachine','Grep','Spark ShortestPath Application','RDDRelation','Spark ConnectedComponent Application']
nodes = 5
memory_node = 21504



if __name__ == '__main__':
    opt = optimiser.Optimiser(dfk_path,model_path,cluster_path, normaliser_path, nodes,memory_node)
    sequence = [['Spark PCA Example',81],['Spark PCA Example',144],['Spark PCA Example',40],
                ['Grep',81],['Grep',128],['Grep',40],
                ['SupporVectorMachine',81],['SupporVectorMachine',144],['SupporVectorMachine',40],
                #['Spark ShortestPath Application',90],['Spark ShortestPath Application',159],['Spark ShortestPath Application',65],
                ['RDDRelation',81],['RDDRelation',144],['RDDRelation',43]]
                #['Spark ConnectedComponent Application',90],['Spark ConnectedComponent Application',159],['Spark ConnectedComponent Application',65]]
    for s in sequence:
        conf = opt.get_best_conf(s[0],s[1])
    best_real_df = pd.DataFrame(opt.best_real_confs)
    best_conf_df = pd.DataFrame(opt.best_confs)
    non_optimal_df = pd.DataFrame(opt.non_optimal_confs)
    best_real_df.duration.sum()/float(non_optimal_df.duration.sum())
    best_conf_df.duration.sum()/float(non_optimal_df.duration.sum())