def insertCell(table_id, userid, appid, ymd, column_family_id, col, val):
    import os
    import sys
    import pip
    import site
    reload(site)
    import settings
    reload(settings)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.credentials
    from google.cloud import bigtable
    project_id = settings.bt_project_id
    instance_id = settings.bt_instance_id
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    value = 0
    if(column_family_id == 'OneTag'):
        col_val = val
    else:
        col_val = '{}'.format(val).encode('utf-8')
    column_id = '{}'.format(col).encode('utf-8')
    cf_val_id ='{}'.format(column_family_id).encode('utf-8')
    try:
        import hashlib
  	import time
        row_key = hashlib.md5('{}{}'.format(str(userid).strip(),str(ymd).strip()).encode('utf-8')).hexdigest() + (str(appid)).encode('utf-8')
        row = table.row(row_key)
        row.set_cell(
                cf_val_id,
                column_id,
                col_val)
        row.commit()
        value = 1
        return value
    except (KeyError,AttributeError):
        return value
 
    return value


def readCell(table_id, userid,appid,ymd,column_family_id,col):
    import os
    import sys
    import pip
    from datetime import datetime, timedelta
    import time
    import site
    reload(site)
    import settings
    reload(settings)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.credentials
    from google.cloud import bigtable
    project_id = settings.bt_project_id
    instance_id = settings.bt_instance_id
    value = None
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    try:
        cf_val_id ='{}'.format(column_family_id).encode('utf-8')
        table = instance.table(table_id)
        col_val = '{}'.format(col).encode('utf-8')
        import hashlib
        import time
        row_key = hashlib.md5('{}{}'.format(str(userid).strip(),str(ymd).strip()).encode('utf-8')).hexdigest()+(str(appid)).encode('utf-8')
        row = table.read_row(row_key)
        value= row.cells[cf_val_id][col_val][0].value
        return value	
    except (AttributeError,KeyError):	
        return value

    return value
