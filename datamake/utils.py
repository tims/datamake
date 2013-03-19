import datetime

def date_now():  
  dt = datetime.datetime.now()
  return dt.strftime("%Y-%m-%d")

def date_days_delta(datestring, days_delta):
  dt = datetime.datetime.strptime(datestring, "%Y-%m-%d")
  delta = datetime.timedelta(int(days_delta))
  return (dt + delta).strftime("%Y-%m-%d")
