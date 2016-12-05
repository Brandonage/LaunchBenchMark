from pandas import read_pickle
from sklearn.externals import joblib
from math import ceil
import pandas as pd
import re
from sklearn import preprocessing
from sklearn.neighbors import NearestNeighbors

dfk_path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/dfk.pickle"
dfapps_path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/dfapps.pickle"
model_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/clf.pickle'
cluster_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/cluster.pickle'
normaliser_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/normaliser.pickle'
benchmark_apps = ['Spark PCA Example','SupporVectorMachine','Grep','Spark ShortestPath Application','LogisticRegressionApp Example','Spark ConnectedComponent Application']
nodes = 5
memory_node = 21504
graph_applications =  [u'Spark PageRank Application', u'Spark ShortestPath Application',
                       u'Spark SVDPlusPlus Application',
                       u'Spark ConnectedComponent Application',
                       u'Spark StronglyConnectedComponent Application']


class Optimiser:

    conf_list = [[['spark.executor.memory','1g'],['spark.executor.cores','1']],
                [['spark.executor.memory','2g'],['spark.executor.cores','1']],
                [['spark.executor.memory','2g'],['spark.executor.cores','2']],
                [['spark.executor.memory','2g'],['spark.executor.cores','3']],
                [['spark.executor.memory','3g'],['spark.executor.cores','1']],
                [['spark.executor.memory','3g'],['spark.executor.cores','4']],
                [['spark.executor.memory','4g'],['spark.executor.cores','1']],
                [['spark.executor.memory','4g'],['spark.executor.cores','3']],
                [['spark.executor.memory','4g'],['spark.executor.cores','6']],
                [['spark.executor.memory','6g'],['spark.executor.cores','1']],
                [['spark.executor.memory','7g'],['spark.executor.cores','2']]
                ]

    def __init__(self,dfk_path,model_path,cluster_path, normaliser_path, nod,mem):
        self.clf = joblib.load(model_path)
        self.normaliser = joblib.load(normaliser_path)
        self.cluster = joblib.load(cluster_path)
        self.dfk = read_pickle(dfk_path)
        self.dfapps = read_pickle(dfapps_path)
        self.dfkseen = self.dfk.loc[~self.dfk['spark.app.name'].isin(benchmark_apps)]
        self.dfappsseen = self.dfapps.loc[~self.dfapps['spark.app.name'].isin(benchmark_apps)]
        self.nodes = nod
        self.memory = mem
        self.best_real_confs = []
        self.best_confs = []
        self.non_optimal_confs = []
        self.worst_confs = []


    def get_status_app_and_conf(self,app,n,conf):
        appid = self.get_appid_for_ntasks_and_conf(app,n,conf)
        status = self.dfapps.loc[(self.dfapps['appId']==appid)]['status'].values[0]
        return status

    def isgraph(self,application):
        if application in graph_applications:
            return 'Graph Application'
        else:
            return 'Non Graph Application'


    def get_duration_app_and_conf(self,app,n,conf): ## conf is a tuple of spark.executor.memory and spark.executor.cores
        appid = self.get_appid_for_ntasks_and_conf(app,n,conf)
        stages = self.dfk.loc[(self.dfk['appId']==appid)]
        return stages.duration.sum()

    def get_worst_conf(self,app,n):
        list_apps = self.get_appid_for_ntasks(app,n)
        list_apps = self.dfapps[(self.dfapps["appId"].isin(list_apps)) & (self.dfapps["status"]==2)]["appId"]
        g = self.dfk[(self.dfk['spark.app.name']==app) & (self.dfk['appId'].isin(list_apps)) & (self.dfk['status']==2)].groupby('appId').sum()
        worstapp = g['duration'].idxmax()
        pair = self.dfk.loc[self.dfk['appId']==worstapp][['spark.executor.memory','spark.executor.cores']].head(1).values
        return pair[0][0], pair[0][1]

    def get_real_best_conf(self,app,n):
        list_apps = self.get_appid_for_ntasks(app,n)
        list_apps = self.dfapps[(self.dfapps["appId"].isin(list_apps)) & (self.dfapps["status"]==2)]["appId"]
        if list_apps.empty:
            return '6g','1'
        else:
            g = self.dfk[(self.dfk['spark.app.name']==app) & (self.dfk['appId'].isin(list_apps)) & (self.dfk['status']==2)].groupby('appId').sum()
            bestapp = g['duration'].idxmin()
            pair = self.dfk.loc[self.dfk['appId']==bestapp][['spark.executor.memory','spark.executor.cores']].head(1).values
            return pair[0][0], pair[0][1]

    def app_already_seen(self,application):
        return any((self.dfkseen['spark.app.name']==application))

    def get_appid_for_ntasks(self,application,ntasks):
        return self.dfk.loc[(self.dfk['spark.app.name']==application) & (self.dfk['taskCountsNum']==ntasks) ]['appId'].unique()

    def get_appid_for_ntasks_and_conf(self,application,ntasks,conf):
        try:
            return self.dfk.loc[(self.dfk['spark.app.name']==application) & (self.dfk['taskCountsNum']==ntasks) & (self.dfk['spark.executor.memory']==conf[0])
                            & (self.dfk['spark.executor.cores']==conf[1])]['appId'].unique()[0]
        except IndexError:
            return 'no app'

    def add_to_seen(self,application,ntasks,conf):
        appid = self.get_appid_for_ntasks_and_conf(application,ntasks,conf)  ## we get the appid of the default execution for that application with ntasks
        toadd = self.dfk.loc[(self.dfk['appId']==appid)] ## we get the stages that we have to add to the already seen metrics
        self.dfkseen = self.dfkseen.append(toadd)
        toadd = self.dfapps.loc[(self.dfapps['appId']==appid)]
        self.dfappsseen= self.dfappsseen.append(toadd)

    def predict_total_duration_ml(self,reference):
        drop_this_for_training = ['appId','id','jobId','name','stageId','spark.app.name','spark.executor.memory', 'taskCountsRunning' ,
                                  'taskCountsSucceeded','slotsInCluster','nExecutorsPerNode', 'tasksincluster'#'totalTaskDuration',
                                  ,'ExecutorRunTime', 'ResultSize','ResultSerializationTime','disk_read','disk_write','net_recv','net_send',
                                  'SchedulerDelayTime','ExecutorDeserializeTime','SchedulerDelayTime','status']
        input = reference.drop(drop_this_for_training,axis=1).drop('duration',axis=1)
        input = input.fillna(0)
        input = input.sort(axis=1)
        result = self.clf.predict(input) ## remember to always sort a pandas dataframe when passing it to a sklearn method
        return result.sum()

    def predict_conf_neighbours(self,application,for_ntasks):
        train = self.dfkseen.loc[(self.dfkseen['spark.executor.memory']=='1g') & (self.dfkseen['spark.executor.cores']=='1')]
        train = train.fillna(0)
        drop_for_clustering = ['appId','id','jobId','name','stageId','spark.executor.memory','spark.executor.cores', 'taskCountsRunning' , ## we have to drop parallelism features, noisy ones
                                  'taskCountsSucceeded','slotsInCluster','nExecutorsPerNode', 'tasksincluster'#'totalTaskDuration',     ## and all the identifiers (stageId, jobId and so on)
                                  , 'ResultSize','ResultSerializationTime','memoryPerTask','spark.executor.bytes','disk_read','disk_write','net_recv','net_send',
                                  'paging_in','paging_out','io_total_read','io_total_write','duration','tasksincluster','taskspernode','nWaves','spark.app.name']
        train_x = train.drop(drop_for_clustering,axis=1)
        scaler = preprocessing.StandardScaler().fit(train_x)
        X = scaler.transform(train_x)
        clf = NearestNeighbors(n_neighbors=4)
        clf.fit(X)
        input = self.dfkseen.loc[(self.dfkseen['spark.app.name']==application) & (self.dfkseen['status'].isin([3,1]))]
        input = input.fillna(0)
        input = input.drop(drop_for_clustering,axis=1)
        res = clf.kneighbors(self.normaliser.transform(input))
        neighbours = train.iloc[res[1][0]]
        names = neighbours.loc[neighbours['spark.app.name']!=application]['spark.app.name'].unique() ## I ignore the first row because it's the point itself
        tasks = neighbours.loc[neighbours['spark.app.name']!=application]['taskCountsNum'].unique()
        app = self.dfkseen.loc[(self.dfkseen['spark.app.name'].isin(names)) & (self.dfkseen['taskCountsNum'].isin(tasks))].groupby("appId")["stageId"].count().idxmax()
        memory = (self.dfapps[self.dfapps['appId']==app])['spark.executor.memory'].values[0]
        cores = (self.dfapps[self.dfapps['appId']==app])['spark.executor.cores'].values[0]
        return memory,cores





    def isinbenchmark(self,conf,application,for_ntasks):
        pair = [conf[0][1],conf[1][1]]
        res = self.get_appid_for_ntasks_and_conf(application,for_ntasks,pair)
        if res=='no app':
            return False
        else:
            return True


    def predict_conf(self,application,for_ntasks):
        def parse_mb_exec(string):
            m = re.search('(\d+)([a-zA-Z]{1})',string)
            unit = m.group(2) ## Are they megabytes or gigabytes
            if (unit=='m'): # if they are megabytes just leave the number as it is
                number = int(m.group(1))
            if (unit=='g'): # if they are gigabytes multiply by 1024
                number = int(m.group(1))*1024
            return number
        def calculate_task_per_host(yarnam,yarnminalloc,execmem,execcores,nNodes,available_mb,ntasks,newtasks): ## yarnminalloc is 1024 ## we add a new parameter newtasks to substitute it for the one we want to predict
            def calculate_allocation(containermem,alloc):
                return float(containermem+max([384,containermem*0.10]))/alloc
            exec_memory=ceil(calculate_allocation(execmem,yarnminalloc))*yarnminalloc
            nexec=int(available_mb/exec_memory)
            nslots_for_node=nexec*execcores
            nslots_for_cluster = nslots_for_node * nNodes
            if ntasks<25: ## If it's one of this stages that have few tasks then the number of tasks is going to be the older one.
                newtasks=ntasks
            if newtasks < nslots_for_cluster:
                ntasks_for_node=ceil(newtasks/float(nNodes)) # we need to do the float thing to get an int number
                ntasks_for_cluster=newtasks
            else:
                ntasks_for_node = nslots_for_node
                ntasks_for_cluster = nslots_for_cluster
            memoryPerTask = exec_memory/execcores
            nWaves = newtasks/float(nslots_for_cluster)
            return pd.Series([ntasks_for_node,nexec,ntasks_for_cluster,memoryPerTask,nslots_for_cluster,nWaves,newtasks,execmem,execcores],index=['taskspernode','nExecutorsPerNode','tasksincluster','memoryPerTask',
                                                                                                                        'slotsInCluster','nWaves','taskCountsNum','spark.executor.bytes','spark.executor.cores'])
        def get_parallelism_features(conf,ref,nodes,node_memory,tasks):
            memory = parse_mb_exec(conf[0][1])
            cores = int(conf[1][1])
            res = ref.apply(lambda row: calculate_task_per_host(512,1024,memory,cores,nodes,node_memory,row['taskCountsNum_ref'],tasks),axis=1)
            return res

        reference = self.dfkseen.loc[(self.dfkseen['spark.app.name']==application) & (self.dfkseen['status']!=4)] ## we get the stages for that app that we have seen
        listofconfs = []
        for conf in self.conf_list: ## we get the parallelism values for that conf
            if self.isinbenchmark(conf,application,for_ntasks):  # if we have the configuration in the benchmark we will try that configuration and predict its values
                reference = reference.rename(index=str,columns={"memoryPerTask":"memoryPerTask_ref","taskspernode":"taskspernode_ref","taskCountsNum":"taskCountsNum_ref",
                                                                "spark.executor.bytes":"spark.executor.bytes_ref","spark.executor.cores":"spark.executor.cores_ref"})
                input_parallelism_df = get_parallelism_features(conf,reference,nodes=self.nodes,node_memory=self.memory,tasks=for_ntasks)
                reference = reference.drop(['nExecutorsPerNode','tasksincluster','slotsInCluster','nWaves'],axis=1)
                reference[['taskspernode_if','nExecutorsPerNode','tasksincluster','memoryPerTask_if','slotsInCluster','nWaves','taskCountsNum_if','spark.executor.bytes_if','spark.executor.cores_if']]=input_parallelism_df # we set the slice
                predicted_duration =self.predict_total_duration_ml(reference) ## This duration is going to be predicted for all the stages and for signatures of different sizes (tasksCountThatRunned)
                listofconfs.append((conf[0][1],conf[1][1],predicted_duration)) ## The SUM of all the stages predicted with the different signatures gives us the shortes running configuration
        return listofconfs                                ## Across the observations we have

    def get_best_conf(self,application,ntasks):
        if self.app_already_seen(application): # if we have already seen the application
            status = self.dfappsseen.loc[(self.dfappsseen['spark.app.name']==application)]['status'] ## If it crashed with one gigabyte keep calling the kmeans
            if all(~status.isin([2])):
                best_tuple = self.predict_conf_neighbours(application,ntasks)
                self.add_to_seen(application,ntasks,best_tuple)
                self.best_confs.append({"conf":best_tuple,"duration":self.get_duration_app_and_conf(application,ntasks,best_tuple),"application":application + str(ntasks),
                                        "type":"Predicted Conf","status":self.get_status_app_and_conf(application,ntasks,best_tuple),"graph":self.isgraph(application)})
                best_real_tuple = self.get_real_best_conf(application,ntasks)
                self.best_real_confs.append({"conf":best_real_tuple,"duration":self.get_duration_app_and_conf(application,ntasks,best_real_tuple),"application":application+ str(ntasks),
                                             "type":"Best Real Conf","status":self.get_status_app_and_conf(application,ntasks,best_real_tuple),"graph":self.isgraph(application)})
                self.non_optimal_confs.append({"conf":("1g","1"),"duration":self.get_duration_app_and_conf(application,ntasks,('1g','1')),"application":application+ str(ntasks),
                                               "type":"Default Conf","status":self.get_status_app_and_conf(application,ntasks,("1g","1")),"graph":self.isgraph(application)})
                worst_tuple = self.get_worst_conf(application,ntasks)
                self.worst_confs.append({"conf":worst_tuple,"duration":self.get_duration_app_and_conf(application,ntasks,worst_tuple),"application":application+ str(ntasks),
                                             "type":"Worst Conf","status":self.get_status_app_and_conf(application,ntasks,worst_tuple),"graph":self.isgraph(application)})
                return best_tuple[0], best_tuple[1]
            else:
                list = self.predict_conf(application,ntasks) # we can optimise it with we've seen so far
                best_tuple = [item for item in list if item[2]==min(zip(*list)[2])] ##We get the tuple that has the minimum duration in the list of tuples
                best_tuple = (best_tuple[0][0],best_tuple[0][1])
                self.add_to_seen(application,ntasks,best_tuple)
                self.best_confs.append({"conf":best_tuple,"duration":self.get_duration_app_and_conf(application,ntasks,best_tuple),"application":application+ str(ntasks),
                                        "type":"Predicted Conf","status":self.get_status_app_and_conf(application,ntasks,best_tuple),"graph":self.isgraph(application)})
                best_real_tuple = self.get_real_best_conf(application,ntasks)
                self.best_real_confs.append({"conf":best_real_tuple,"duration":self.get_duration_app_and_conf(application,ntasks,best_real_tuple),"application":application+ str(ntasks),
                                             "type":"Best Real Conf","status":self.get_status_app_and_conf(application,ntasks,best_real_tuple),"graph":self.isgraph(application)})
                self.non_optimal_confs.append({"conf":("1g","1"),"duration":self.get_duration_app_and_conf(application,ntasks,('1g','1')),"application":application+ str(ntasks),
                                               "type":"Default Conf","status":self.get_status_app_and_conf(application,ntasks,('1g','1')),"graph":self.isgraph(application)})
                worst_tuple = self.get_worst_conf(application,ntasks)
                self.worst_confs.append({"conf":worst_tuple,"duration":self.get_duration_app_and_conf(application,ntasks,worst_tuple),"application":application+ str(ntasks),
                                             "type":"Worst Conf","status":self.get_status_app_and_conf(application,ntasks,worst_tuple),"graph":self.isgraph(application)})
                return best_tuple[0][0], best_tuple[0][1] ## best_tuple is a list of one triple [(memory,cores,duration)]
        else:
            self.add_to_seen(application,ntasks,('1g','1'))
            self.best_confs.append({"conf":("1g","1"),"duration":self.get_duration_app_and_conf(application,ntasks,('1g','1')),"application":application+ str(ntasks),
                                    "type" : "Predicted Conf","status":self.get_status_app_and_conf(application,ntasks,('1g','1')),"graph":self.isgraph(application)})
            self.best_real_confs.append({"conf":("1g","1"),"duration":self.get_duration_app_and_conf(application,ntasks,('1g','1')),"application":application+ str(ntasks),
                                         "type":"Best Real Conf","status":self.get_status_app_and_conf(application,ntasks,('1g','1')),"graph":self.isgraph(application)})
            self.non_optimal_confs.append({"conf":("1g","1"),"duration":self.get_duration_app_and_conf(application,ntasks,('1g','1')),"application":application+ str(ntasks),
                                           "type":"Default Conf","status":self.get_status_app_and_conf(application,ntasks,('1g','1')),"graph":self.isgraph(application)})
            self.worst_confs.append({"conf":("1g","1"),"duration":self.get_duration_app_and_conf(application,ntasks,("1g","1")),"application":application+ str(ntasks),
                                    "type":"Worst Conf","status":self.get_status_app_and_conf(application,ntasks,("1g","1")),"graph":self.isgraph(application)})
            return '1g', '1'



if __name__ == '__main__':
    opt = Optimiser(dfk_path,model_path,cluster_path, normaliser_path, nodes,memory_node)
    sequence = [['Spark PCA Example',81],['Spark PCA Example',144],['Spark PCA Example',40],
                ['BigDataBench Sort',81],['BigDataBench Sort',128],['BigDataBench Sort',40],
                ['SupporVectorMachine',81],['SupporVectorMachine',144],['SupporVectorMachine',40],
                ['Spark ShortestPath Application',90],['Spark ShortestPath Application',159],['Spark ShortestPath Application',65],
                ['TokenizerExample',81],['TokenizerExample',128],['TokenizerExample',40],
                ['Spark PageRank Application',90],['Spark PageRank Application',159],['Spark PageRank Application',65]]
    optimalduration = []
    list_of_confs = []
    row = {}
    for s in sequence:
        conf = opt.get_best_conf(s[0],s[1])
        best_real_conf = opt.get_real_best_conf(s[0],s[1])
        best_duration = opt.get_duration_app_and_conf(s[0],s[1],best_real_conf)
        non_optimal = opt.get_duration_app_and_conf(s[0],s[1],('1g','1'))
        row.update({"prediced_conf": conf, "application_name" : s[0], "ntasks" : s[1]})
        list_of_confs.append(row)
    bestduration = zip(*best_real_confs)[2]
    sum(bestduration)/float(sum(nonoptimalduration))
    sum(optimalduration)/float(sum(nonoptimalduration))



