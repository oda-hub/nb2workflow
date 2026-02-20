from typing import Any
from flask.json.provider import DefaultJSONProvider
import json

from json import JSONEncoder

try:
    from oda_api.json import CustomJSONEncoder as MMODAJSONEncoder
    oda_encoder = True
except (ModuleNotFoundError, ImportError):
    oda_encoder = False

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, type):
                return dict(type_object=repr(obj))
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        
        if oda_encoder:
            try:
                return MMODAJSONEncoder.default(self, obj)
            except (TypeError, RuntimeError): 
                pass
        return super().default(obj)
    
class CustomJSONProvider(DefaultJSONProvider):
    def dumps(self, obj: Any, **kwargs: Any) -> str:
        kwargs['cls'] = CustomJSONEncoder
        return json.dumps(obj, **kwargs)