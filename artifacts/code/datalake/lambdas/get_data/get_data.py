import boto3
import os

client = boto3.client("glue")

def lambda_handler(event, context):

    try:
        exe_process_id_list = event["exe_process_id"].split(",")
        if (str(event["own_process_id"]) not in exe_process_id_list and not event["redshift"]):
            return {
                "wait": False,
                "error": False,
                "ready": False,
                "table": event["table"],
                "job_name": event["job_name"],
                "exe_process_id": event["exe_process_id"],
                "own_process_id": event["own_process_id"],
                "INSTANCIAS": event["INSTANCIAS"],
                "COD_PAIS": event["COD_PAIS"],
            }

        response = client.get_job_runs(JobName=event["job_name"], MaxResults=50)
        
        if "ORIGIN" in event:
            ORIGIN = event['ORIGIN']
        else:
            ORIGIN = 'AJE'

        for run in response["JobRuns"]:
            if run["JobRunState"] not in ["STARTING", "RUNNING", "STOPPING"]:
                continue
            
            if run["Arguments"]["--COD_PAIS"] == event["COD_PAIS"]:
                return {
                    "wait": True,
                    "table": event["table"],
                    "job_name": event["job_name"],
                    "INSTANCIAS": event["INSTANCIAS"],
                    "COD_PAIS": event["COD_PAIS"],
                    "ORIGIN": ORIGIN,
                    #"PERIODO_INI": event["PERIODO_INI"],
                    #"PERIODO_FIN": event["PERIODO_FIN"],
                    #"PERIODOS": event["PERIODOS"],
                    "exe_process_id": event["exe_process_id"],
                    "own_process_id": event["own_process_id"],
                    "redshift": event["redshift"],
                }

        return {
            "wait": False,
            "error": False,
            "ready": True,
            "table": event["table"],
            "job_name": event["job_name"],
            "INSTANCIAS": event["INSTANCIAS"],
            "COD_PAIS": event["COD_PAIS"],
            "ORIGIN": ORIGIN,
            #"PERIODO_INI": event["PERIODO_INI"],
            #"PERIODO_FIN": event["PERIODO_FIN"],
            #"PERIODOS": event["PERIODOS"],
            "exe_process_id": event["exe_process_id"],
            "own_process_id": event["own_process_id"],
            "redshift": event["redshift"],
        }

    except Exception as e:
        return {"ready": False,"error": True, "msg": f"{str(e)}"}
