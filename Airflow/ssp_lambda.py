##### Aws lambda to trigger ssp function code #####
from botocore.session import Session
from botocore.config import Config


### cap factory ###
def cap_factory_lambda(): 
    
    # Connect aws lambda function #
    s = Session()
    client = s.create_client("lambda", config=Config(connect_timeout=900, read_timeout=800))    
    response = client.invoke(FunctionName='cap_factory', InvocationType='RequestResponse')
    ans = response['Payload'].read().decode("utf-8")[1:-1]
    
    if (ans == "skip") | (ans == "success"):
        return ans
    else:
        return "fail"  


### familymart ###
def familymart_lambda(): 
    
    # Connect aws lambda function #
    s = Session()
    client = s.create_client("lambda", config=Config(connect_timeout=900, read_timeout=800))    
    response = client.invoke(FunctionName='familyMart', InvocationType='RequestResponse')
    ans = response['Payload'].read().decode("utf-8")[1:-1]
    
    if (ans == "skip") | (ans == "success"):
        return ans
    else:
        return "fail"  


