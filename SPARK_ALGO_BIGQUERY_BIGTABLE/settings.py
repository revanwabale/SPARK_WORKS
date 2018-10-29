#  'service provider id' : 'shared password'
servicePrPass = {'xx':'xx','xx':'xx-VwgUu'}

credentials="/home/revan/account.json"
bucket = "xx-dev"
bt_project_id = 'xx-prd'
bt_instance_id = 'xx'
time_units = 20

thresholds = {
    '2' : [10,300,3600],
    '30' : [10,300,3600],
    '25' : [10,600,600],
    '31' : [10,600,3600],
    '5' : [10,600,600],
    '300' : [10,0,600],
    '301' : [10,0,3600],
    '20' : [10,0,60],
    '37' : [10,0,3600],
    '300301': [10,0,600]
    }

project='xx-143401'

tablename = 's_k_usr_profile'

imgateEstimate_url="https://xx_data"

imdb_url="jdbc:mysql://xx"

user="xx"
password="xx"
dbtable="xx.user"
column="id"
numPartitions="5"

STD_MULTIPLE = 3

dataset_prefix="bbq_dev_sp" 

bbq_table = 'xx_tbl'

datasetid = "xx_bbq"

special_user = {'xx_xx':[-7200,18000]}

n_weeks = 8

firstday = 6 

window_size = 4   

param_th_top = 1.5

param_th_bottom = 0.5

column_family_id = ['Daily', 'Weekly','OneTag']
