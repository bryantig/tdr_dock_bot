from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
)


class TdrDockBotStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        my_sql_lambda = _lambda.Function(
            self, 'SQLHandler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('lambda'),
            handler='DockBotSqlPullv2.lambda_handler',
        )