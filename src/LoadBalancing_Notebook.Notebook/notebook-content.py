# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8a28ed26-1929-46f0-bc9e-b4a1c06fdf7d",
# META       "default_lakehouse_name": "Logs",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "8a28ed26-1929-46f0-bc9e-b4a1c06fdf7d"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "0e0d9a3f-f509-a27b-45c3-82d0d34dbb56",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

clusterURI = "https://trd-7mb79sdg0xpkhbt4uc.z0.kusto.fabric.microsoft.com"
databaseName = "CapacityEvents"
#Workspace/Report ID
reportIDs = "500e0bd5-7c79-468e-9bb5-ac27de1dbad5"
##
WSName="LB_Workspace"
CapacityIdToDeploy="df45924e-97d8-48d0-a559-3bfddbfb5aac"
TargetWorkspace=""
CUusedPercentThreshold=0.1
RecoveryThreshold=0.05

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

livyid=spark.conf.get("trident.activity.id")
print(str(livyid))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils
import requests
import sempy_labs as labs
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp,lit
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(WSName)
print(CapacityIdToDeploy)
print(TargetWorkspace)
print(CUusedPercentThreshold)
print(RecoveryThreshold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS report_repoints (
# MAGIC   InitialDatasetID STRING,
# MAGIC   report_Workspace_id STRING,
# MAGIC   report_id STRING,
# MAGIC   trgdatasetID STRING,
# MAGIC   TargetWsID STRING,
# MAGIC   created_at STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS logs_table (
# MAGIC     timestamp TIMESTAMP,
# MAGIC     livyid STRING,
# MAGIC     message STRING
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reportIDsLst = reportIDs.split(",")
print(reportIDsLst)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#get tokens
accessTokenkusto = notebookutils.credentials.getToken(clusterURI)
accessTokenpbi = notebookutils.credentials.getToken("pbi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Input checks
if CapacityIdToDeploy != "" and TargetWorkspace != "":
    raise RuntimeError("You cannot set CapacityToDeploy and TargetWorkspace at the same time")
if WSName != "" and TargetWorkspace != "":
    raise RuntimeError("You cannot set WSName and TargetWorkspace at the same time")
if CapacityIdToDeploy != "" and  WSName == "":
    raise RuntimeError("You cannot set CapacityToDeploy and not set WSName at the same time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schemaRepoints = StructType([
    StructField("InitialDatasetID", StringType(), True),
    StructField("report_Workspace_id", StringType(), True),
    StructField("report_id", StringType(), True),
    StructField("trgdatasetID", StringType(), True),
    StructField("TargetWsID", StringType(), True)
])


def write_for_alerting(TargetWsCapID,TargetWorkspace,Op):
    # Create dummy data
    data = [(f"Target Workspace {TargetWorkspace} is in an overloaded capacity {TargetWsCapID} - please consider moving",)]

    # Create DataFrame
    df = spark.createDataFrame(data, ["Alert"])
    df = df.withColumn("timestamp", current_timestamp())

    df.write \
        .format("com.microsoft.kusto.spark.synapse.datasource") \
        .option("accessToken",accessTokenkusto) \
        .option("kustoCluster", clusterURI) \
        .option("kustoDatabase", databaseName) \
        .option("kustoTable", "alerts") \
        .option("tableCreateOptions", "CreateIfNotExist") \
        .mode("Append") \
        .save()

def write_log(message: str, livyid):
    data = [(message,)]

    df = spark.createDataFrame(data, ["message"]) \
              .withColumn("livyid",lit(str(livyid)))\
              .withColumn("timestamp", current_timestamp()) \
              .select("timestamp", "message")

    df.write.format("delta") \
        .mode("append") \
        .saveAsTable("logs_table")

def getWorkspace(WSName):
    url = f"https://api.powerbi.com/v1.0/myorg/groups"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    # Check status
    #print("Status Code:", response.status_code)

    # If successful, print JSON
    if response.status_code == 200:
        data = response.json()
    
    for ws in data["value"]:
        if ws["name"] == WSName:
            return str(ws["name"])
    return 0

def GetWorkspaceId(WSName):
    url = f"https://api.powerbi.com/v1.0/myorg/groups"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    # Check status
    #print("Status Code:", response.status_code)

    # If successful, print JSON
    if response.status_code == 200:
        data = response.json()
    
    for ws in data["value"]:
        if ws["name"] == WSName:
            return str(ws["id"])
    return 0

def deployWorkspaceInCapacity(capacityId):
    try:
        url = f"https://api.powerbi.com/v1.0/myorg/groups"
        headers = {
            "Authorization": f"Bearer {accessTokenpbi}",
            "Content-Type": "application/json"
        }
        payload = {
            "name": WSName #hardcoded
        }
        response = requests.post(url, headers=headers,json=payload)
        # Check status
        #print("Status Code:", response.status_code)

        # If successful, print JSON
        if response.status_code == 200:
            dataws = response.json()
        #print(dataws["id"])

        url = f"https://api.powerbi.com/v1.0/myorg/admin/capacities/AssignWorkspaces"
        headers = {
            "Authorization": f"Bearer {accessTokenpbi}",
            "Content-Type": "application/json"
        }
        payload = {
            "capacityMigrationAssignments":[
                {
                "targetCapacityObjectId": capacityId,
                "workspacesToAssign": [dataws["id"]]
                }
            ]
        }
        response = requests.post(url, headers=headers,json=payload)
        # Check status
        #print("Status Code: ", response.text)
        # If successful, print JSON
    except Exception as e:
        #print("E", e)
        write_log( "Could not create workspace: " + str(dataws["id"]) + " and attach it to capacity " + capacityId ,livyid)
        return "0"
    return str(dataws["id"])
#
def getCapacityConsumption(capacityId):
    kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(clusterURI,accessTokenkusto)
    client = KustoClient(kcsb)
    torebind=False
    query = f"""
    CapacityEvents
    | extend data = parse_json(data) 
    | where todatetime(["time"]) >= ago(5m)
    | where data.capacityId == '{capacityId}'
    | project ["time"],
    CUusedPercent = todouble(data.capacityUnitMs) / todouble(data.baseCapacityUnits * 1000 * 30),
    utilizationInteractivePercent = todouble(data.utilizationInteractive)/(todouble(data.utilizationBackground) +  todouble(data.utilizationInteractive))
    """

    # synchronous call — DO NOT await
    response = client.execute(databaseName, query)
    #print(response.primary_results[0])
    max = 0
    for row in response.primary_results[0]:
        if (row[1] > max):
            max = row[1]
    write_log("Max capacity consumption for the last 5 mins: " + " Cap ID: " + capacityId + " Percent: " + str(max),livyid)
    return max

def get_sem_model_name(datasetID):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{datasetID}"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)

    # If successful, print JSON
    if response.status_code == 200:
        data = response.json()
    #print(data)
    return (data["name"])

def get_sem_model_ID(datasetName,workspaceId):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/datasets"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)

    # If successful, print JSON
    if response.status_code == 200:
        data = response.json()
    for i in data["value"]:
        if i["name"]==datasetName:
            return i["id"]

def rebind_report(groupId,reportId,datasetID):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{groupId}/reports/{reportId}/Rebind"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    payload = {
        "datasetId": datasetID
    }
    response = requests.post(url, headers=headers,json=payload)

def get_workspaceId_from_dataset(datasetID):
    url = f"https://api.fabric.microsoft.com/v1/workspaces"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    # Check status
    #print("Status Code:", response.status_code)

    # If successful, print JSON
    if response.status_code == 200:
        data = response.json()
    for i in data["value"]:
        workspaceid= (i["id"])
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspaceid}/datasets"
        headers = {
            "Authorization": f"Bearer {accessTokenpbi}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        # If successful, print JSON
        if response.status_code == 200:
            data = response.json()
        for datasets in data["value"]:
            if str(datasets["id"]).strip() == datasetID:
                return (str(workspaceid))

def get_workspaceId_from_report(reportId):
    url = f"https://api.fabric.microsoft.com/v1/workspaces"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    # Check status
    #print("Status Code:", response.status_code)

    # If successful, print JSON
    if response.status_code == 200:
        data = response.json()
    for i in data["value"]:
        workspaceid= (i["id"])
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspaceid}/reports"
        headers = {
            "Authorization": f"Bearer {accessTokenpbi}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        # If successful, print JSON
        if response.status_code == 200:
            data = response.json()
        for reports in data["value"]:
            if str(reports["id"]).strip() == reportId:
                return (str(workspaceid))


def get_capacity_id_from_workspace(workspaceId):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    # Check status
    #print("Status Code:", response.status_code)

    # If successful, print JSON
    #print(response.text)
    if response.status_code == 200:
        data = response.json()
    return data["capacityId"]


def rebind_datasources(workspaceId,datasetID,datasourceId,connectivityType,path,type,gatewayId):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/semanticModels/{datasetID}/bindConnection"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    payload = {
    "connectionBinding": {
    "id": datasourceId,
    "connectivityType": connectivityType,
    "connectionDetails": {
        "path": path,
        "type": type
        }
      }
    }
    # ✅ Add gatewayId only if provided
    if gatewayId:
        payload["connectionBinding"]["gatewayId"] = gatewayId
    
    response = requests.post(url, headers=headers,json=payload)
    # Check status
    print("response.text")
    print(response.text)
    print("response.text")


def getConnectionDetailsFromDatasetandRebind(SRCworkspaceID,SRCdatasetId,workspaceID,datasetId):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{SRCworkspaceID}/datasets/{SRCdatasetId}/datasources"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    # Check status
    #print("Status Code:", response.status_code)
    #print(response.text)
    # If successful, print JSON
    if response.status_code == 200:
        data = response.json()
    for i in data["value"]:
        #print(i["datasourceId"])
        connectionId=str(i["datasourceId"]).strip()
        url = f"https://api.fabric.microsoft.com/v1/connections/{connectionId}"
        headers = {
            "Authorization": f"Bearer {accessTokenpbi}",
            "Content-Type": "application/json"
        }        
        response = requests.get(url, headers=headers)
        #print(response.text)
        if response.status_code == 200:
            data1 = response.json()
        datasourceId = i["datasourceId"]
        connectivityType = data1["connectivityType"]
        gatewayId = data1.get("gatewayId")
        path = data1["connectionDetails"]["path"]
        type = data1["connectionDetails"]["type"]
        write_log("Rebinding connection at dataset " + datasetId + " datasourceId " + datasourceId + " path: " + path ,livyid)
        rebind_datasources(workspaceID,datasetId,datasourceId,connectivityType,path,type,gatewayId)

def reportRebind(dictRebinds):
    #Rebind reports to target dataset 
    for report in dictRebinds:
        data = [(
        str(report["InitialDatasetID"]),
        str(report["report_Workspace_id"]),
        str(report["report_id"]),
        str(report["trgdatasetID"]),
        str(report["TargetWsID"]),
        )]
        df = spark.createDataFrame(data, schema=schemaRepoints)
        df = df.withColumn("created_at", current_timestamp())
        
        write_log("Initial DS ID "+ str(report["InitialDatasetID"]) +" Report WS: " +str(report["report_Workspace_id"]) + " REPORT: "+ str(report["report_id"]) + " repointed at "  + str(report["trgdatasetID"]) ,livyid)
        if report["ToRebind"]>0:
            rebind_report(str(report["report_Workspace_id"]).strip(), str(report["report_id"]).strip(), str(report["trgdatasetID"]).strip())
            print("!writing to table!")
            if report["InitialDatasetID"]!="CALLBACK":
                #in case there is an update it is because the report was rebind to two different workspaces.
                delta_table = DeltaTable.forName(spark, "report_repoints")
                (
                    delta_table.alias("t")
                    .merge(
                        df.alias("s"),
                        "t.report_id = s.report_id"
                    )
                    .whenMatchedUpdate(set={
                        #"InitialDatasetID": "s.InitialDatasetID",
                        "report_Workspace_id": col("s.report_Workspace_id"),
                        "trgdatasetID": col("s.trgdatasetID"),
                        "TargetWsID": col("s.TargetWsID"),
                        "created_at": current_timestamp()
                    })
                    .whenNotMatchedInsert(values={
                        "InitialDatasetID": col("s.InitialDatasetID"),
                        "report_Workspace_id": col("s.report_Workspace_id"),
                        "report_id": col("s.report_id"),
                        "trgdatasetID": col("s.trgdatasetID"),
                        "TargetWsID": col("s.TargetWsID"),
                        "created_at":  current_timestamp()
                    })
                    .execute()
                )

def update_refresh_schedule(sourceWS, sourceDatasetID, TargetWS,TargetDatasetID):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{sourceWS}/datasets/{sourceDatasetID}/refreshSchedule"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
            data1 = response.json()
    print(data1)
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    payload1 = {
        "value": {
            "enabled": True,
            "days": data1["days"],
            "times": data1["times"],
            "localTimeZoneId": data1["localTimeZoneId"]
        }
    }
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{TargetWS}/datasets/{TargetDatasetID}/refreshSchedule"
    response = requests.patch(url, headers=headers,json=payload1)
    print("##")
    print(response)
    print("##")

def get_refresh_schedule(sourceWS, sourceDatasetID):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{sourceWS}/datasets/{sourceDatasetID}/refreshSchedule"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
            data1 = response.json()
    print(data1)
    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#does ws exist already?
targetWSCapacityId=""
if WSName!="":
    existingwsid=GetWorkspaceId(WSName)
    if existingwsid!=0: #does workspace exist
        try:
            targetWSCapacityId=str(get_capacity_id_from_workspace(existingwsid)).strip()
        except Exception as e:
            print(e)
            raise Exception("Make sure workspace " + existingwsid + " is in a fabric capacity")
if TargetWorkspace!="":
    try:
        targetWSCapacityId=str(get_capacity_id_from_workspace(TargetWorkspace)).strip()
    except Exception as e:
        print(e)
        raise Exception("Make sure workspace " + TargetWorkspace + " is in a fabric capacity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Get report dependencies
if TargetWorkspace!="":
    TargetWorkspace = TargetWorkspace #no op
else: 
    if existingwsid==0: #does not exist
        if CapacityIdToDeploy != "":
            write_log("Deploying workspace at Capacity " + " workspace: " + WSName + " capacity: " + CapacityIdToDeploy,livyid)
            TargetWorkspace = deployWorkspaceInCapacity(CapacityIdToDeploy)
            time.sleep(10)
    else:
        TargetWorkspace = existingwsid
print("TargetWorkspace: " , TargetWorkspace)


if targetWSCapacityId=="":
    TargetWsCapID = str(get_capacity_id_from_workspace(TargetWorkspace)).strip()
else:
    TargetWsCapID=targetWSCapacityId

report_ids = (
    spark.table("report_repoints")
    .select("report_id")
    .distinct()
    .rdd
    .flatMap(lambda x: x)
    .collect()
)

dictRebinds=[]
ReportsRebindBack=[]
print(reportIDsLst)
for report_id in reportIDsLst:
    reportWS=get_workspaceId_from_report(report_id)
    write_log("CHECKING FOR REPORT " + str(report_id)+ " at workspace "+ reportWS,livyid)

    row = (
        spark.table("report_repoints")
        .filter(col("report_id") == report_id)
        .select("*")
        .limit(1)
        .collect()
    )
    
    if row:
        #what happens if the dataset is published again? does it update ID?
        #Posso testar no caso de fazer dois rebinds para dois workspaces diferentes?
        print("ROW " , row[0])
        initial_ds_id = row[0]["InitialDatasetID"]
        Init_Workspace_id = get_workspaceId_from_dataset(initial_ds_id)
        InitialCapId=get_capacity_id_from_workspace(str(Init_Workspace_id))
        InitCapCons=getCapacityConsumption(InitialCapId)
        if InitCapCons < RecoveryThreshold:
            #Dont copy as I assume the dataset is already updated via CICD in initial workspace (only rebind report)
            dictRebinds.append({
                "InitialDatasetID":"CALLBACK",
                "TargetWsID":Init_Workspace_id,
                "ToRebind":1,
                "report_Workspace_id": reportWS,
                "report_id": report_id,
                "trgdatasetID": initial_ds_id,
            })
            spark.sql(f"DELETE FROM report_repoints WHERE report_id = '{report_id}'")
            write_log("REPORT " + report_id + " WHICH WAS PREVIOUSLY REPOINTED AT DATASET " + str(row[0]["trgdatasetID"]) + " WILL BE REPOINTED BACK AT " + initial_ds_id ,livyid)
            ReportsRebindBack.append(report_id)
    else:
        write_log("Report: " + report_id + " not in cache for repoint to original" ,livyid)

reportRebind(dictRebinds)
#from now we only want to iterate over reports which were not repointed back already (as those ones are definitely healthy)
ReportsRemaining = [x for x in reportIDsLst if x not in ReportsRebindBack]
#clean up dictRebinds
dictRebinds=[]

print(ReportsRemaining)
for report_id in ReportsRemaining:
    #If target workspace is in an overloaded capacity we can raise this exception and exit
    Op = getCapacityConsumption(TargetWsCapID)
    if Op > CUusedPercentThreshold*0.8:
        write_log("NO OP - The target workspace for Load balancing is already in an overloaded capacity ( > 0.8 * CUusedPercentThreshold ) " + " CAPACITY: " + TargetWsCapID + " WORKSPACE: " + TargetWorkspace,livyid)
        write_for_alerting(TargetWsCapID,TargetWorkspace,Op)
        notebookutils.notebook.exit("NO OP - The target workspace for Load balancing is already in an overloaded capacity")
        #raise RuntimeError("NO OP - The target workspace for Load balancing is already in an overloaded capacity")
    ##############

    url = f"https://api.powerbi.com/v1.0/myorg/reports/{report_id}"
    headers = {
        "Authorization": f"Bearer {accessTokenpbi}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        datasetName=get_sem_model_name(data["datasetId"])
        Workspace_id = get_workspaceId_from_dataset(data["datasetId"])
        report_workspace_id=reportWS
        Capacity_id = str(get_capacity_id_from_workspace(str(Workspace_id))).strip()

        #for optimization only in case TargetWsCapID <= CUusedPercentThreshold - as it was already measured above dont need to run ToRebind=getCapacityConsumption(Capacity_id)
        if Capacity_id == TargetWsCapID:
            write_log("The report is in workspace Id " + str(Workspace_id) + " which is in the same capacity of the target workspace " + str(TargetWorkspace) ,livyid)
            continue

        ToRebind=getCapacityConsumption(Capacity_id)
        if ToRebind>CUusedPercentThreshold:
            try:
                labs.deploy_semantic_model(data["datasetId"],Workspace_id,target_dataset=datasetName+"_LB",target_workspace=TargetWorkspace, refresh_target_dataset =False, overwrite =True)
                trgdatasetID=get_sem_model_ID(datasetName+"_LB",TargetWorkspace)
            except Exception as e:
                #this following line is to get target dataset ID in case the refresh of the semantic model fails (due to datasource non-configurations)
                trgdatasetID=get_sem_model_ID(datasetName+"_LB",TargetWorkspace)
                print(f"Exception (may not be fatal): {e}")
            getConnectionDetailsFromDatasetandRebind(Workspace_id,str(data["datasetId"]).strip(),TargetWorkspace,trgdatasetID)
            update_refresh_schedule(Workspace_id, str(data["datasetId"]).strip(), TargetWorkspace ,trgdatasetID)
            labs.refresh_semantic_model(dataset=trgdatasetID, workspace=TargetWorkspace)
            dictRebinds.append({
                "InitialDatasetID":data["datasetId"],
                "TargetWsID":TargetWorkspace,
                "ToRebind":ToRebind,
                "report_Workspace_id": report_workspace_id,
                "report_id": report_id,
                "trgdatasetID": trgdatasetID,
            })
        else:
            write_log("NOT REBINDING REPORT: " + str(report_id) + " LIKELY BECAUSE CAPACITY UTILIZATION IS LOW",livyid)

reportRebind(dictRebinds)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
