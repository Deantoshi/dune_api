from requests import get, post
import pandas as pd
import time

API_KEY = ""
HEADER = {"x-dune-api-key" : API_KEY}

BASE_URL = "https://api.dune.com/api/v1/"

def make_api_url(module, action, ID):
    """
    We shall use this function to generate a URL to call the API.
    """

    url = BASE_URL + module + "/" + ID + "/" + action

    return url

def execute_query(query_id, engine="medium"):
    """
    Takes in the query ID and engine size.
    Specifying the engine size will change how quickly your query runs. 
    The default is "medium" which spends 10 credits, while "large" spends 20 credits.
    Calls the API to execute the query.
    Returns the execution ID of the instance which is executing the query.
    """

    url = make_api_url("query", "execute", query_id)
    params = {
        "performance": engine,
    }
    response = post(url, headers=HEADER, params=params)

    execution_id = response.json()['execution_id']

    return execution_id


def get_query_status(execution_id):
    """
    Takes in an execution ID.
    Fetches the status of query execution using the API
    Returns the status response object
    """

    url = make_api_url("execution", "status", execution_id)
    response = get(url, headers=HEADER)

    return response


def get_query_results(execution_id):
    """
    Takes in an execution ID.
    Fetches the results returned from the query using the API
    Returns the results response object
    """

    url = make_api_url("execution", "results", execution_id)
    response = get(url, headers=HEADER)

    return response


def cancel_query_execution(execution_id):
    """
    Takes in an execution ID.
    Cancels the ongoing execution of the query.
    Returns the response object.
    """

    url = make_api_url("execution", "cancel", execution_id)
    response = get(url, headers=HEADER)

    return response

#loops through our query untill it is completed
#returns a dataframe of our completed query results
def get_populated_results(response, execution_id):
    state = response.json()['state']

    while state != 'QUERY_STATE_COMPLETED':
        print('Waiting on Query Completion: ' + state)
        time.sleep(15)
        #gets our updated response

        response = get_query_results(execution_id)
        state = response.json()['state']

        #adds some time if our query needs time to wait before executing
        if state == 'QUERY_STATE_PENDING':
            time.sleep(120)
            state = response.json(['state'])
        
        #if our query has an issue then we cancel the query. Sleep. and we run everything again
        if state != 'QUERY_STATE_COMPLETED' and state != 'QUERY_STATE_EXECUTING':
            cancel_query_execution(execution_id)
            print('Query cancelled and trying again later')
            time.sleep(7200)
            run_everything()


        if state == 'QUERY_STATE_COMPLETED':
            print(state)
            break
    
    data = pd.DataFrame(response.json()['result']['rows'])

    return data

#runs our discord bot
def run_dune_query():

    #runs our query
    run_everything()
    
    # #Does not Query! Just reads populated data from test.csv for quicker testing
    # await test_run_everything()
    


    return

# will use execute our dune query, put the data in a dataframe, and save the data in a test.csv file
def run_everything():
    
    #the query ID  can be found if you click into a query and look at the URL
    #https://dune.com/queries/2991576/4964086
    #in this case the first set of numbers is our query ID we want to use: 2991576

    #gets our execution ID from our query ID, and sets the speed at which our query will execute: medium is cheapest we can do costing 10 credits
    execution_id = execute_query("2991576","medium")

    #makes our dataframe when our data is ready
    response = get_query_status(execution_id)

    response = get_query_results(execution_id)

    data = get_populated_results(response, execution_id)

    print('Query Results:')
    print(data[:50])
    #saves our results to a df that can be used by our test runner for quicker iteration
    data.to_csv('test.csv', index=False)

    

    # #Can be used to loop continous queries on a cooldown
    # time.sleep(720)
    # await run_everything()

    return data

#reads a csv_file instead of running our query
async def test_run_everything(client):
    
    data = pd.read_csv('test.csv')
    
    print('Query Results:')
    print(data[:50])
    

    return data