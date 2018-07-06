

def get(key, aws_client):
    return aws_client.get_parameter(Name=key)['Parameter']['Value']
