import boto3
import json
import time

# Instantiate the RDS client using the boto3 library
rds_client = boto3.client('rds-data')

# Database name, serverless cluster, secrets store
database_name = 'dockbotsql'
db_cluster_arn = 'arn:aws:rds:us-east-1:518464770440:cluster:dockbotdb'
db_credentials_secrets_store_arn = 'arn:aws:secretsmanager:us-east-1:518464770440:secret:rds-db-credentials/cluster-HQH6A6SFL74AQ3GS7KSIEA4QWU/admin-N7EUtU'

def lambda_handler(event, context):
# handler triggered by stream data
    # Parse stream data
    for record in event['Records']:
        # Pull the record data from the stream data, assign to x
        payload = record['body']
        x = json.loads(payload)
        # print(x)

        # Pull the inner message from x, assign to y
        y = json.loads(x["Message"])
        
        # Assign variables to inner message property values
        event_time = y["eventTime"]
        work_flow_action = y["workflowAction"]
        node_id = y["nodeId"]
        
        docking_location_name = y["dockingLocationName"]
        docking_location_name = docking_location_name.replace(" ", "")
        
        # Examine the vehicle_type
        vehicle_type = y["data"]["VehicleType"]
        # If TRAILER or TRAILER_SKIRTED, record the trailer number
        if vehicle_type == "TRAILER" or vehicle_type == "TRAILER_SKIRTED":
            vehicle_number = y["data"]["VehicleNumber"]
        else:
        # Otherwise vehicle_number defaults to "NONE"
            vehicle_number = "NONE"
        # We only want the first 20 characters of the vehicle number
        vehicle_number=vehicle_number[:20] 
        
        # Check for the presence of a VRID
        # If it's missing, vrid will equal "None"
        try:
            vrid = y["data"]["VRID"]
        except KeyError:
            vrid = "None"
        # We only want the first 10 characters of the VRID
        vrid = vrid[:10]
        
        try:
            owner = y["data"]["OwnerCode"]
        except KeyError:
            owner = "None"
        # We only want the first 10 characters of the OwnerCode
        owner = owner[:10] # We only want the fiest 10 characters
        
        # SQL to upsert values in tblTDRCurrentStatus.
        # If the node_id/docking_location_name pair exists, update it. Otherwise, 
        # insert the new values
        sql = 'INSERT INTO tblTDRCurrentStatus (event_time, work_flow_action, '
        sql = sql + 'node_id, docking_location_name, vehicle_number, owner_code, '
        sql = sql + 'vrid) values (:eventtime, :workflowaction, :nodeid, '
        sql = sql + ':dockinglocationname, :vehiclenumber, :ownercode, :vridval '
        sql = sql + ') ON DUPLICATE KEY UPDATE event_time=:eventtime, '
        sql = sql + 'work_flow_action=:workflowaction, node_id=:nodeid, '
        sql = sql + 'docking_location_name=:dockinglocationname, '
        sql = sql + 'vehicle_number=:vehiclenumber, owner_code=:ownercode, '
        sql = sql + 'vrid=:vridval'

        # Passing SQL parameters from extracted values above
        sql_parameters = [
        {'name':'eventtime', 'value':{'longValue': event_time}},
        {'name':'workflowaction', 'value':{'stringValue': work_flow_action}},
        {'name':'nodeid', 'value':{'stringValue': node_id}},
        {'name':'dockinglocationname', 'value':{'stringValue': docking_location_name}},
        {'name':'vehiclenumber', 'value':{'stringValue': vehicle_number}},
        {'name':'ownercode', 'value':{'stringValue': owner}},
        {'name':'vridval', 'value':{'stringValue': vrid}}
        ]
        
        response = execute_statement(sql, sql_parameters)

def execute_statement(sql, sql_parameters=[]):
    response = rds_client.execute_statement(
        secretArn=db_credentials_secrets_store_arn,
        database=database_name,
        resourceArn=db_cluster_arn,
        sql=sql,
        parameters=sql_parameters
    )
    return response