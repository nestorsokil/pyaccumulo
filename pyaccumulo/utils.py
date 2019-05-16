from accumulo.ttypes import ScanColumn


def _get_scan_columns(cols):
    if not cols:
        return []
    return [ScanColumn(colFamily=col.get('cf'), colQualifier=col.get('cq')) for col in cols]


def _following_array(val):
    if val:
        return val + b'\0'
    return None


def _following_key(key):
    """
    Returns the key immediately following the input key - based on the Java implementation found in
    org.apache.accumulo.core.data.Key, function followingKey(PartialKey part)
    :param key: the key to be followed
    :return: a key that immediately follows the input key
    """
    if key.timestamp is not None:
        key.timestamp -= 1
    elif key.colVisibility is not None:
        key.colVisibility = _following_array(key.colVisibility)
    elif key.colQualifier is not None:
        key.colQualifier = _following_array(key.colQualifier)
    elif key.colFamily is not None:
        key.colFamily = _following_array(key.colFamily)
    elif key.row is not None:
        key.row = _following_array(key.row)
    return key
