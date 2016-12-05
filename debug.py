import optimiser
import pandas as pd


dfk_path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/dfk.pickle"
dfapps_path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/dfapps.pickle"
model_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/clf.pickle'
cluster_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/cluster.pickle'
normaliser_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/normaliser.pickle'
benchmark_apps = ['Spark PCA Example','SupporVectorMachine','Grep','Spark ShortestPath Application','LogisticRegressionApp Example','Spark ConnectedComponent Application']
nodes = 5
memory_node = 21504



if __name__ == '__main__':
    opt = optimiser.Optimiser(dfk_path,model_path,cluster_path, normaliser_path, nodes,memory_node)
    sequence = [#['Spark PCA Example',20],
                #['Spark PCA Example',40],['Spark PCA Example',144],['Spark PCA Example',81],
                #['Grep',20], º
                #['Grep',40],['Grep',128],['Grep',81],
                #['SupporVectorMachine',20],
                #['SupporVectorMachine',40],['SupporVectorMachine',144],['SupporVectorMachine',81],
                ['Spark ShortestPath Application',90],['Spark ShortestPath Application',65],['Spark ShortestPath Application',65],['Spark ShortestPath Application',65]]
                #['LogisticRegressionApp Example',20],
                #['LogisticRegressionApp Example',40],['LogisticRegressionApp Example',81],['LogisticRegressionApp Example',144],
                #['Spark ConnectedComponent Application',159],['Spark ConnectedComponent Application',65],['Spark ConnectedComponent Application',90]]
    for s in sequence:
        conf = opt.get_best_conf(s[0],s[1])
    best_real_df = pd.DataFrame(opt.best_real_confs)
    #best_real_df = best_real_df[~best_real_df.index.isin([0,3,6,9,12,15])]
    best_conf_df = pd.DataFrame(opt.best_confs)
    #best_conf_df = best_conf_df[~best_conf_df.index.isin([0,3,6,9,12,15])]
    non_optimal_df = pd.DataFrame(opt.non_optimal_confs)
    #non_optimal_df = non_optimal_df[~non_optimal_df.index.isin([0,3,6,9,12,15])]
    worst_conf_df = pd.DataFrame(opt.worst_confs)
    #worst_conf_df = worst_conf_df[~worst_conf_df.index.isin([0,3,6,9,12,15])]
    non_optimal_df.loc[non_optimal_df['status']!=2,'duration']=0
    one_df = best_real_df.append(best_conf_df.append(non_optimal_df.append(worst_conf_df)))
    best_real_df.duration.sum()/float(non_optimal_df.duration.sum())
    best_conf_df.duration.sum()/float(non_optimal_df.duration.sum())
    #sns.barplot(x="application",y="duration",hue="type",data=one_df.loc[one_df['duration']>376212])
    #sns.barplot(x="application",y="duration",hue="type",data=one_df.loc[one_df["graph"]=="Non Graph Application"]) & (one_df["graph"]=="Non Graph Application"])