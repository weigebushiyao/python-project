import datetime
import time

t=datetime.datetime.strptime('2019-05-15 16:44:01',"%Y-%m-%d %H:%M:%S").strftime("%H")
print(type(t))