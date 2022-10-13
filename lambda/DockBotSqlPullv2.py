import json
import boto3
import datetime

# Create an RDS client
RDS_CLIENT = boto3.client('rds-data')

# Database/RDS endpoint/secrets store
DATABASE_NAME = 'dockbotsql'
DB_CLUSTER_ARN = 'arn:aws:rds:us-east-1:518464770440:cluster:dockbotdb'
DB_CREDENTIALS_SECRETS_STORE_ARN = 'arn:aws:secretsmanager:us-east-1:518464770440:secret:rds-db-credentials/cluster-HQH6A6SFL74AQ3GS7KSIEA4QWU/admin-N7EUtU'

# Query logic to get status
def CheckStatus(intent_request):
    session_attributes = get_session_attributes(intent_request)
    
    # Pull intent slot values using get method
    slots = get_slots(intent_request)
    req_type = get_slot(intent_request, 'request_type')
    fc = get_slot(intent_request, 'fc_id')
    door = get_slot(intent_request, 'dock_door')
    
    # ========================================================================
    # Query to pull TD Status
    # @fc   param IN - node_id value
    # @door param IN - dock door value
    # @returns: time_string, display_time, vehicle_number
    # ========================================================================
    query = f"""SELECT convert(event_time, char) as time_string, 
    concat(work_flow_action,' recorded at ',from_unixtime((event_time/1000),
    '%m/%d/%Y %h:%i:%s')) as display_time, vehicle_number, owner_code, vrid FROM 
    tblTDRCurrentStatus WHERE node_id='{fc}' AND docking_location_name='{door}'"""
    
    text = ""
    # Logic to call the query
    try:
        response = execute_statement(query)
        print(response)
        
        # Check if record returned. If it's three then GTG
        parsed_string = len(response['records'][0])
        if parsed_string == 5: #record found
            work_flow_action = response['records'][0][1]['stringValue']
            trailer_id = response['records'][0][2]['stringValue']
            owner_id = response['records'][0][3]['stringValue']
            vrid_id = response['records'][0][4]['stringValue']
            
            text = (
                f"TDR Status for {fc.upper()} {door.upper()} :\n" 
                f"\tStatus:\t{work_flow_action}\n"
                f"\tTrailer:\t {trailer_id}\n"
                f"\tOwner:\t{owner_id}\n"
                f"\tVRID:\t {vrid_id}"
            )
            
    except IndexError: # Empty recordset return, means no records
        text = f"The requested combination of {fc.upper()} {door.upper()} does " \
        "not exist. \nPlease try again."
        
    except Exception as error: #Any other error caught here and displayed to user
        print(error)
        text = f"An error occured while trying to retrieve the result from database"
    
    # Message packet to return    
    message =  {
            'contentType': 'PlainText',
            'content': text
        }
    fulfillment_state = "Fulfilled"
    
    # Message packet return to bot
    return close(intent_request, session_attributes, fulfillment_state, message)
    
# Clean up    
def close(intent_request, session_attributes, fulfillment_state, message):
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] 
        if 'requestAttributes' in intent_request else None
    }
    
def dispatch(intent_request):
    intent_name = intent_request['sessionState']['intent']['name']
    response = None
    
    # Traffic director for bot's intent handlers
    if intent_name == 'CheckStatus':
        return CheckStatus(intent_request) 
        # Other intent handlers to be listed below here
    # elif intent_name == 'CheckSomethingElse':
    #    return FollowupCheckSomethingElse(intent_request)
    raise Exception('Intent with name ' + intent_name + ' not supported')

def elicit_intent(intent_request, session_attributes, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitIntent'
            },
            'sessionAttributes': session_attributes
        },
        'messages': [ message ] if message != None else None,
        'requestAttributes': intent_request['requestAttributes'] 
        if 'requestAttributes' in intent_request else None
    }

# Handler to execute SQL queries
# @sql 
# ========================================================================
# Handler to execute SQL queries
# @sql  param IN - SQL string to execute
# *** Other internal parameters are global 
# @returns: response
# ========================================================================
def execute_statement(sql):
    response = RDS_CLIENT.execute_statement(
        secretArn=DB_CREDENTIALS_SECRETS_STORE_ARN,
        database=DATABASE_NAME,
        resourceArn=DB_CLUSTER_ARN,
        sql=sql
    )
    return response;
    
def get_slot(intent_request, slotName):
    slots = get_slots(intent_request)
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['value']['interpretedValue']
    else:
        return None    

def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']
    
def get_session_attributes(intent_request):
    sessionState = intent_request['sessionState']
    if 'sessionAttributes' in sessionState:
        return sessionState['sessionAttributes']
    return {}

def lambda_handler(event, context):
    response = dispatch(event)
    return response