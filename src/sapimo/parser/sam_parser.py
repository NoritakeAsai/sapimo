
from copy import deepcopy
from pathlib import Path

from sapimo.utils import setup_logger, deep_update
from sapimo.parser.cf_resource_parser import CfResourceParser

logger = setup_logger(__file__)


class SamParser(CfResourceParser):
    def __init__(self, filepath: Path, region="us-east-1"):
        super().__init__(filepath, region)

    def _preprocess(self, filepath: Path, region: str):
        """
            override: extract global settings and declare additional member
        """
        super()._preprocess(filepath, region)
        # extract global settings and resolve Fn
        g_props = self._whole.get("Globals", {})
        self._function_globals = self._treat(g_props.get("Function", {}))
        # self._api_globals = self._treat(g_props.get("App", {}))
        # self._http_api_globals = self._treat(g_props.get("HttpApi", {}))
        # self._table_globals = self._treat(g_props.get("SimpleTable",{}))

        # additional member
        self._apis = {}  # key:api path,
        self._triggered = {}  # key:trigger bucket name

    def _classification(self, name: str, val: dict):
        """ override: Pick "serverless.function" and treat event """
        props: dict = deepcopy(val.get("Properties", {}))
        if val["Type"] == "AWS::Serverless::Function":
            deep_update(props, self._function_globals)
            events = props.pop("Events", {})
            for event in events.values():
                if not isinstance(event, dict):
                    continue
                event_type = event.get("Type", "")

                if event_type == "Api":
                    # api integration
                    api_path = event.get("Properties", {}).get("Path", "")
                    method = event.get("Properties", {}).get("Method", "")
                    if api_path and method:
                        if api_path in self._apis:
                            self._apis[api_path][method] = {
                                "Properties": props}
                        else:
                            self._apis[api_path] = {
                                method: {"Properties": props}}
                elif event_type == "S3":
                    # s3 trigger
                    ev_props = event.get("Properties", {})
                    if "ObjectCreated" in ev_props.get("Events", ""):
                        bucket = ev_props.get("Bucket", "")
                        filter_ = ev_props.get("Filter", None)
                        if bucket:
                            t_props = deepcopy(props)
                            if filter_:
                                t_props["Filter"] = filter_
                            self._triggered[bucket] = {"Properties": t_props}
                    else:
                        self._others[name] = val
                else:
                    # other event (unused)
                    self._others[name] = val
        else:
            super()._classification(name, val)

    def _get_config_dict(self) -> dict:
        """ override: add api paths """
        config = super()._get_config_dict()
        config["paths"] = self._apis
        return config

    def _get_ref_and_attr(self, name: str, resource: dict):
        """ override: for "AWS::Serverless::~~ """
        tp = resource["Type"]
        props = resource["Properties"]
        if tp == "AWS::Serverless::Function":
            return {"Ref": name, "Arn": self._arn_tmp.format("function", name)}
        elif tp == "AWS::Serverless::Api":
            return {"Ref": name,
                    "RootResourceId": "dummy"}
        elif tp == "AWS::Serverless::Application":
            return {"Ref": name,  # stack resource name
                    "Outputs.ApplicationOutputName": "dummyOutputName"}
        elif tp == "AWS::Serverless::HttpApi":
            return {"Ref": name}  # resource ip id
        elif tp == "AWS::Serverless::LayerVersion":
            return {"Ref": props.get("ContentUri", name)}  # original
        elif tp == "AWS::Serverless::SimpleTable":
            return {"Ref": props.get("TableName", name)}
        elif tp == "AWS::Serverless::StateMachine":
            return {"Ref": self._arn_tmp.format("stateMachine", name)}
        else:
            return super()._get_ref_and_attr(name, resource)
