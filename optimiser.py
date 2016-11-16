from pandas import read_pickle
from sklearn.externals import joblib
from math import ceil
import pandas as pd
import re

dfk_path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/dfk.pickle"
model_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/clf.pickleSVR'
nodes = 5
memory_node = 21504

class Optimiser:

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

    def __init__(self,dfk_path,model_path,nod,mem):
        self.clf = joblib.load(model_path)
        self.dfk = read_pickle(dfk_path)
        self.dfkseen = pd.DataFrame(columns=self.dfk.columns)
        self.nodes = nod
        self.memory = mem

    def get_duration_app_and_conf(self,app,n,conf): ## conf is a tuple of spark.executor.memory and spark.executor.cores
        appid = self.get_appid_for_ntasks_and_conf(app,n,conf)
        stages = self.dfk.loc[(self.dfk['appId']==appid)]
        return stages.duration.sum()

    def get_real_best_conf(self,app,n):
        list_apps = self.get_appid_for_ntasks(app,n)
        g = self.dfk[(self.dfk['spark.app.name']==app) & (self.dfk['appId'].isin(list_apps))].groupby('appId').sum()
        bestapp = g['duration'].idxmin()
        pair = self.dfk.loc[self.dfk['appId']==bestapp][['spark.executor.memory','spark.executor.cores']].head(1).values
        return pair[0][0], pair[0][1],g.duration.min()

    def app_already_seen(self,application):
        return any((self.dfkseen['spark.app.name']==application))

    def get_appid_for_ntasks(self,application,ntasks):
        return self.dfk.loc[(self.dfk['spark.app.name']==application) & (self.dfk['taskCountsNum']==ntasks) ]['appId'].unique()

    def get_appid_for_ntasks_and_conf(self,application,ntasks,conf):
        try:
            res = self.dfk.loc[(self.dfk['spark.app.name']==application) & (self.dfk['taskCountsNum']==ntasks) & (self.dfk['spark.executor.memory']==conf[0])
                            & (self.dfk['spark.executor.cores']==conf[1])]['appId'].unique()[0]
        except:
            res = 0
        return res
    def add_to_seen(self,application,ntasks,conf):
        appid = self.get_appid_for_ntasks_and_conf(application,ntasks,conf)  ## we get the appid of the default execution for that application with ntasks
        toadd = self.dfk.loc[(self.dfk['appId']==appid)] ## we get the stages that we have to add to the already seen metrics
        self.dfkseen = self.dfkseen.append(toadd)

    def predict_total_duration_ml(self,reference):
        drop_this_for_training_2 = ['appId','id','jobId','name','stageId','spark.app.name','spark.executor.memory','spark.executor.cores', 'taskCountsRunning',
                                  'taskCountsSucceeded','slotsInCluster','nExecutorsPerNode', 'tasksincluster'#'totalTaskDuration',
                                  ,'ExecutorRunTime', 'ResultSize','ResultSerializationTime','spark.executor.bytes']
        input = reference.drop(drop_this_for_training_2,axis=1).drop('duration',axis=1)
        input = input.fillna(0)
        result = self.clf.predict(input)
        return result.sum()



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
            return pd.Series([ntasks_for_node,memoryPerTask,nWaves,newtasks],index=['taskspernode_if','memoryPerTask_if','nWaves_if','taskCountsNum'])
        def get_parallelism_features(conf,ref,nodes,node_memory,tasks):
            memory = parse_mb_exec(conf[0][1]) ## For this ammount of memory
            cores = int(conf[1][1])  ### and this ammount of cores
            res = ref.apply(lambda row: calculate_task_per_host(512,1024,memory,cores,nodes,node_memory,row['taskCountsNum'],tasks),axis=1) ## how many tasks per node I get for that particular application (in fact we only use taskCountsNum)
            return res                                                                                                                      ## since some stages have less tasks than the file chunks

        # reference = self.dfkseen.loc[(self.dfkseen['spark.app.name']==application) & (self.dfkseen['status']!=4) & (self.dfkseen['spark.executor.memory']=='1g')
        #                                 & (self.dfkseen['spark.executor.cores']=='1')] ## we get the stages for that app that we have seen
        # listofconfs = []
        # for conf in self.conf_list: ## we get the parallelism values for that conf
        #     input_parallelism_df = get_parallelism_features(conf,reference,nodes=self.nodes,node_memory=self.memory,tasks=for_ntasks)
        #     reference[['taskspernode','nExecutorsPerNode','tasksincluster','memoryPerTask','slotsInCluster','nWaves','taskCountsNum']]=input_parallelism_df # we set the slice
        #     predicted_duration =  self.predict_total_duration_ml(reference) ## This duration is going to be predicted for all the stages and for signatures of different sizes (tasksCountThatRunned)
        #     listofconfs.append((conf[0][1],conf[1][1],predicted_duration)) ## The SUM of all the stages predicted with the different signatures gives us the shortes running configuration
        # return listofconfs                                ## Across the observations we have
        reference = self.dfkseen.loc[(self.dfkseen['spark.app.name']==application) & (self.dfkseen['status'].isin([3,2,1]))]## we get the stages for that app that we have seen
        reference = reference.rename(index=str,columns={"memoryPerTask":"memoryPerTask_ref","nWaves":"nWaves_ref","taskspernode":"taskspernode_ref"})
        reference['taskspernode_if']=""
        reference['memoryPerTask_if']=""
        reference["nWaves_if"]=""
        listofconfs = []
        for conf in self.conf_list: ## we get the parallelism values for that conf
            input_parallelism_df = get_parallelism_features(conf,reference,nodes=self.nodes,node_memory=self.memory,tasks=for_ntasks) ## Calculate the if parallelism metrics for this app and this conf
            reference[['taskspernode_if','memoryPerTask_if','nWaves_if','taskCountsNum']]=input_parallelism_df # we set the slice
            predicted_duration =  self.predict_total_duration_ml(reference) ## This duration is going to be predicted for all the stages and for signatures of different sizes (tasksCountThatRunned) <<<<<-----
            listofconfs.append((conf[0][1],conf[1][1],predicted_duration)) ## The SUM of all the stages predicted with the different signatures gives us the shortes running configuration
        return listofconfs                                ## Across the observations we have

    def get_best_conf(self,application,ntasks):
        if self.app_already_seen(application): # if we have already seen the application
            list = self.predict_conf(application,ntasks) # we can optimise it with we've seen so far
            best_tuple = [item for item in list if item[2]==min(zip(*list)[2])] ##We get the tuple that has the minimum duration in the list of tuples
            self.add_to_seen(application,ntasks,(best_tuple[0][0],best_tuple[0][1]))
            return best_tuple[0][0], best_tuple[0][1] ## best_tuple is a list of one triple [(memory,cores,duration)]
        else:
            self.add_to_seen(application,ntasks,('1g','1'))
            return '1g', '1'



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



