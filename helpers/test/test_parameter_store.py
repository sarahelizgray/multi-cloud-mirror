import boto3
from moto import mock_ssm

from ..parameter_store import *

@mock_ssm
def test_get_returns_a_string():
    conn = boto3.client('ssm', region_name='us-east-1')

    conn.put_parameter(Name='awesome', Value='500', Type='String')

    assert get('awesome', conn) == '500'
