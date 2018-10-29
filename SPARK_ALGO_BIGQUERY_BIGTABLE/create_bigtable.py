
def createBigtable(table_id,column_family_id):
    import os
    import sys
    import pip
 
    import settings
    reload(settings)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.credentials
    from google.cloud import bigtable
    project_id = settings.bt_project_id
    instance_id = settings.bt_instance_id
    
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    print('Creating the {} table.'.format(table_id))
    table = instance.table(table_id)
    try:
        table.create()
        for cf in column_family_id:
           cf1 = table.column_family(cf)
           cf1.create()
           print('Created the {} table.'.format(table_id))
    except (AttributeError,KeyError):	
        table.create()
        for cf in column_family_id:
           cf1 = table.column_family(cf)
           cf1.create()


if __name__ == '__main__':
    import sys
    import settings
    reload(settings)
    table_name = settings.tablename
    column_family_id = settings.column_family_id  #['daily', 'weekly','onetag' ]
    '''
    - please mention all possible values of column families here
    - please create last 3 months instances of bigtable name relative to month in which you are starting daily summary script.
    '''
    tbl_name = str(table_name) + sys.argv[1] #'201703'
    createBigtable(tbl_name, column_family_id)
    tbl_name = str(table_name) + sys.argv[2] #'201702'
    createBigtable(tbl_name, column_family_id)
    tbl_name = str(table_name) + sys.argv[3] #'201701'
    createBigtable(tbl_name, column_family_id)
