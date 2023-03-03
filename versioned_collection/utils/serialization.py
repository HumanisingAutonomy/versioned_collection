import json
import re
from typing import Dict, Any

from bson import ObjectId, json_util
from colorama import Fore


def stringify_object_id(oid: ObjectId) -> str:
    return f"ObjectId('{str(oid)}')"


def stringify_document(document: Dict[str, Any]) -> str:
    class ObjectIdEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, ObjectId):
                return stringify_object_id(obj)
            return json.JSONEncoder.default(self, obj)

    if document is None:
        raise ValueError("The `document` parameter must not be `None`!")
    return json.dumps(document, indent=2, cls=ObjectIdEncoder)


def parse_json_document(json_doc: str) -> Dict[str, Any]:
    object_ids = re.findall(r'"ObjectId\(\S*\)"', json_doc)
    for object_id in object_ids:
        hex_id = re.findall(r"'(\S*)'", object_id)[0]
        new_id = json.dumps({"$oid": hex_id})
        json_doc = json_doc.replace(object_id, new_id)
    return json.loads(json_doc, object_hook=json_util.object_hook)


def colour_diff(diff_str: str) -> str:
    lines = diff_str.splitlines()
    coloured_lines = []
    for line in lines:
        if line.startswith('+'):
            coloured_lines.append(Fore.GREEN + line + Fore.RESET)
        elif line.startswith('-'):
            coloured_lines.append(Fore.RED + line + Fore.RESET)
        else:
            coloured_lines.append(line)
    return '\n'.join(coloured_lines)
