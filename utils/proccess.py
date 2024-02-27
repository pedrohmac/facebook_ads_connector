import logging

logging.basicConfig(level=logging.INFO)

def normalize_keys(record, custom_fields):
    logging.info("Normalizing record keys.")
    actions = record.get("actions", None)
    
    if actions is not None:
        actions_dict = {action["action_type"]: action["value"] for action in actions}

        for key, value in actions_dict.items():
            for name, field_id in custom_fields.items():
                if field_id in key:
                    record[name] = value

    return record

def record_maker(record, keys_list):
    logging.info("Creating record.")
    handler = {}
    for key in keys_list:
        handler[key] = record.get(key, None)

    return handler
