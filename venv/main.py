import pandas as pd
from sqlalchemy import create_engine
import progressbar
import datetime
import time

'''Проблемы: 1) Отказ от создания дампа базы данных(создание таблицы по 0 строке в csv). 2) Использование потоков для
отображения прогресса закгрузки. 3) Нативный лог-файл'''


##########
chunksize = 100000
n = len(open('police-department-calls-for-service.csv').readlines())
percent = 0
one = 100//((n-1)//chunksize)
filename = 'police-department-calls-for-service.csv'
tablename = 'testtable'
curr_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace("-","").replace(" ","_").replace(":","")
start_datetime = datetime.datetime.now()
##########


#widgets = ['Test: ', progressbar.Percentage(), ' ', progressbar.Bar(marker='#',left='[',right=']'),
#          ' ', progressbar.ETA()] #see docs for other options
#pbar = progressbar.ProgressBar(widgets=widgets, maxval=n)
#pbar.start()



engine = create_engine('sqlite:///testdb.db')

for df in pd.read_csv(filename, chunksize=chunksize, header = 0, iterator=True):
    df.to_sql(tablename, engine, if_exists='append', index=False)
    print('Test:  ' + str(percent) + ' % complete')
    percent += one
    #time.sleep(0.1)
    #pbar.update(percent)
    #percent += chunksize


#pbar.finish()


end_datetime = datetime.datetime.now()
time_delta = end_datetime - start_datetime

with open('log_' + curr_datetime + '.txt', 'w') as log_file:
    log_file.write("The script started at " + str(start_datetime) + ".\n")
    log_file.write("The script ended at " + str(end_datetime) + ".\n")
    log_file.write("The running time was " + str(time_delta.seconds) + " seconds.\n")
    log_file.write("The number of added records " + str(n-1) + ".\n")
    log_file.write("\n")
    log_file.close()