from pathlib import Path
import json

import yaml

from sapimo.utils import setup_logger
logger = setup_logger(__file__)


class ConfigParser:
    """
        read config.yaml and convert to useful form
    """

    def __init__(self, path: Path):
        try:
            if not path.exists():
                raise FileNotFoundError(f"{path.name} is not found")

            with open(path) as f:
                if path.name.endswith(".json"):
                    obj = json.load(f)
                elif path.name.endswith(".yaml") or path.name.endswith(".yml"):
                    obj = yaml.safe_load(f)
                else:
                    raise Exception("config file must be json or yaml")

            if "paths" not in obj:
                raise Exception("paths key dose not exist in config file")
            # paths = {}
            self.apis: dict[str, dict[str, ApiProps]] = {}
            for path, val in obj["paths"].items():
                # method_props = {}
                self.apis[path] = {}
                for k, v in val.items():
                    method = k.lower()
                    self.apis[path][method] = ApiProps(path, method, v)
            self.triggered = obj.get("triggered", {})
        except:
            logger.exception("config parse error")
            raise Exception("config parse error")

        self.all_resource = obj

    def get_service_config(self, service: str):
        return self.all_resource.get(service, {})


class ApiProps:
    def __init__(self, path: str, method: str, src: dict):
        self.path = path
        self.method = method

        props = src["Properties"]
        dirs = [d for d in props["CodeUri"].split("/") if d]
        handler_prefix = ".".join(dirs)
        handler = handler_prefix + "." + props["Handler"]
        self.code_uri = props["CodeUri"]
        self.import_path = ".".join(handler.split(".")[:-1])
        self.func = handler.split(".")[-1]
        self.layers = props.get("Layers", [])
        self.runtime = props.get("Runtime", "")
        self.environ = props.get("Environment", {}).get("Variables", {})

        responses = src.get("responses", {})
        self.responses = {}
        succeed = None
        redirection = None
        client_error = None
        server_error = None
        for k, v in responses.items():
            res = ApiResponse(k, v)
            self.responses[k] = res
            if not succeed and str(k).startswith("20"):
                succeed = res
            elif not redirection and str(k).startswith("30"):
                redirection = res
            elif not client_error and str(k).startswith("40"):
                client_error = res
            elif not server_error and str(k).startswith("50"):
                server_error = res
        self.responses.setdefault(200, succeed or ApiResponse(200, {}))
        self.responses.setdefault(300, redirection or ApiResponse(300, {}))
        self.responses.setdefault(400, client_error or ApiResponse(400, {}))
        self.responses.setdefault(500, server_error or ApiResponse(500, {}))


class ApiResponse:
    def __init__(self, code: int, src: dict):
        self.code = code
        self._example = self._dig_out(src, "example")

    def example(self):
        return {
            "statusCode": self.code,
            "body": json.dumps(self._example),
        }

    def _dig_out(self, d: dict, key: str) -> dict:
        for k, v in d.items():
            if k == key:
                return v
            elif isinstance(v, dict):
                return self._dig_out(v)
        else:
            return {}
