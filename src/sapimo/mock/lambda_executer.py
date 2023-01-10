import os
import importlib
import sys
import uuid
import datetime
from pathlib import Path
import json

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from sapimo.parser.config_parser import ConfigParser, ApiProps
from sapimo.utils import setup_logger

logger = setup_logger(__file__)


class LambdaExecuter:
    """
        - retain path config
        - setup s3 and dynamodb in local dir (if required)
        - setup and execute lambda python code
    """

    def __init__(self, path: Path):
        """
            - set config
            - setup s3 and dynamodb
        """
        self._config = ConfigParser(path)

    def _get_api_info(self, req: Request):
        path = req.scope["route"].path
        method = req.method.lower()
        try:
            return self._config.apis[path][method]
        except:
            logger.warning(f"{path}:{method} execute info is not found")
            return None

    async def run_by_trigger(self, updated: dict, deleted: dict):
        """
            lambda execution when s3 file is updated
            - interpret trigger rules
        """
        if not self._config.triggered:
            return
        raise NotImplementedError()

    async def run_by_api(self, req: Request):
        """
            lambda execution when called API
            - set(change) env
            - import required layer
            - execute lambda code
        """
        props: ApiProps = self._get_api_info(req)
        if not props:
            return None
        self._change_env(props.environ)
        with LayerImporter([*props.layers, props.code_uri]):
            # request to event
            try:
                event = await self.req_to_event(req)
            except Exception as e:
                logger.error("request convert error")
                return Response(status_code=400, content=str(e))

            # import lambda code
            try:
                app = importlib.import_module(props.import_path)
            except ModuleNotFoundError as e:
                err_msg = "lambda code import error: " + str(e) + "\n"\
                    "- check 'CodeUri' or 'Layers'"\
                    f" of {props.path}.{props.method} "\
                    " in mock_api/config.yaml\n"\
                    "- check if the required modules are installed\n"\
                    "- check import section in your code\n"
                logger.error(err_msg)
                return Response(status_code=500, content=err_msg)

            # check handler
            if not props.func in dir(app):
                err_msg = f"lambda entrypoint({props.func}) is\
                            not exist in {props.import_path}"
                return Response(status_code=500, content=err_msg)

            # lambda execution
            try:
                lambda_res = eval("app."+props.func)(event, None)
                return JSONResponse(json.loads(lambda_res["body"]))
            except Exception as e:
                logger.exception("lambda error")
                return Response(status_code=500, content=err_msg or str(e))

    async def req_to_event(self, req: Request):
        """
        convert request to lambda event
            isBase64Encoded, stageVariables, version etc. is invalid(dummy)
        """
        template_path = req.scope["route"].path
        body = await req.body()
        # headers
        header_dict = dict(req.headers)
        headers = {}
        multi_headers = {}
        for key, value in header_dict.items():
            k = "-".join([w.capitalize() for w in key.split("-")])
            if isinstance(value, list):
                value = value[0]
                values = value
            else:
                values = [value]
            headers[k] = value
            multi_headers[k] = values

        # query param
        query = {}
        multi_query = {}
        for key, value in req.query_params.items():
            if isinstance(value, list):
                value = value[0]
                values = value
            else:
                values = [value]
            query[key] = value
            multi_query[key] = values

        # dummy request context
        now = datetime.datetime.now(datetime.timezone.utc)
        protocol = "HTTP/" + req.scope["http_version"] \
            if req.scope["type"] == "http" else req.scope["type"]
        request_context = {
            "accountId": "123456789012",
            "apiId": "1234567890",
            "domainName": req.url.netloc,
            "extendedRequestId": None,
            "httpMethod": req.method,
            "identity": {
                "accountId": None,
                "apiKey": None,
                "caller": None,
                "cognitoAuthenticationProvider": None,
                "cognitoAuthenticationType": None,
                "cognitoIdentityPoolId": None,
                "sourceIp": req.scope["client"][0],
                "user": None,
                "userAgent": "Custom User Agent String",
                "userArn": None
            },
            "path": template_path,
            "protocol": protocol,
            "requestId": str(uuid.uuid4()),
            "requestTime": now.strftime("%d/%b/%Y:%H:%M:%S %z"),
            "requestTimeEpoch": int(now.timestamp()),
            "resourceId": "123456",
            "resourcePath": template_path,
            "stage": "Prod"
        }

        msg = {
            "body": body.decode("utf-8"),
            "headers": headers,
            "httpMethod": req.method,
            "isBase64Encoded": False,
            "multiValueHeaders": multi_headers,
            "multiValueQueryStringParameters": multi_query,
            "path": req.url.path,
            "pathParameters": dict(req.path_params),
            "queryStringParameters": query,
            "requestContext": request_context,
            "resource": template_path,
            "stageVariables": None,
            "version": "1.0"
        }
        return msg

    async def get_example(self, req: Request, status: int):
        props = self._get_api_info(req)
        if "responses" not in props:
            return None
        if status not in props["responses"]:
            return None
        props = props["responses"][status]

        def search_example(di: dict):
            for key, value in di.items():
                if key == "example":
                    return value
                elif isinstance(value, dict):
                    res = search_example(value)
                    if res:
                        return res
            else:
                return None
        return search_example(props)

    def _change_env(self, env: dict):
        def_env = {
            "HOSTNAME": "fae95fa3f3cb",  # dummy
            "AWS_LAMBDA_FUNCTION_VERSION": "$LATEST",
            "AWS_SAM_LOCAL": "false",
            "AWS_SESSION_TOKEN": "",
            "AWS_SECRET_ACCESS_KEY": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "LANG": "en_US.UTF-8",
            "AWS_ACCESS_KEY_ID": "AKIAXXXXXXXXXXXXXXXX",
            "SHLVL": "0",
            "HOME": "",
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
            # FIXME---
            # "LD_LIBRARY_PATH": "/var/lang/lib:/lib64:/usr/lib64:/var/runtime:/var/runtime/lib:/var/task:/var/task/lib:/opt/lib",
            # "PWD": "/var/task",
            # "LAMBDA_TASK_ROOT": "/var/task",
            # "LAMBDA_RUNTIME_DIR": "/var/runtime",
            # "TZ": ":/etc/localtime",
            # "AWS_ACCOUNT_ID": "123456789012",
            # "_HANDLER": "app.lambda_handler",
            # "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "128",
            # "PYTHONPATH": "/var/runtime",
            # "AWS_LAMBDA_FUNCTION_TIMEOUT": "3",
            # "AWS_LAMBDA_LOG_GROUP_NAME": "aws/lambda/dummy", # GreetingFunction",
            # "AWS_LAMBDA_RUNTIME_API": "127.0.0.1:9001",
            # "AWS_LAMBDA_LOG_STREAM_NAME": "$LATEST",
            # "AWS_EXECUTION_ENV": "AWS_Lambda_python3.9",
            # "AWS_LAMBDA_FUNCTION_NAME": "DummyFunction",
            # "PATH": "/var/lang/bin:/usr/local/bin:/usr/bin/:/bin:/opt/bin",
            # "AWS_LAMBDA_FUNCTION_HANDLER": "app.lambda_handler",
        }
        def_env.update(env)

        # delete one by one for avoid memory leak
        for k in os.environ.keys():
            del os.environ[k]
        for k, v in def_env.items():
            os.environ[k] = v

        # TODO:restore env: unnecessary?


class LayerImporter:
    """ append lambda layer's code uri to sys.path """

    def __init__(self, layers: list[str]):
        self._layers = layers
        self._count = len(layers)

    def __enter__(self):
        if self._count == 0:
            return

        for layer in self._layers:
            sys.path.append(layer)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._count == 0:
            return
        remove_targets = set(sys.path[-self._count:])
        if remove_targets != set(self._layers):
            logger.warning("sys.path is changed by lambda function. \
                            layers path can't be removed")
            return
        else:
            for _ in range(self._count):
                sys.path.pop()
