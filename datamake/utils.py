import itertools
import datetime

def days_range(a, b, fmt="%Y-%m-%d"):
  a,b = int(a),int(b)
  for i in range(a,b):
    dt = datetime.datetime.now()
    yield (dt + datetime.timedelta(i)).strftime(fmt)

def date_now(fmt="%Y-%m-%d"):  
  dt = datetime.datetime.now()
  return dt.strftime(fmt)

def date_days_delta(datestring, days_delta):
  dt = datetime.datetime.strptime(datestring, "%Y-%m-%d")
  delta = datetime.timedelta(int(days_delta))
  return (dt + delta).strftime("%Y-%m-%d")

def evalulate_parameters(parameters):
  evaled_params = {}
  for key, value in parameters.items():
    evaled_value = eval(value)
    if hasattr(evaled_value, "__iter__") and not isinstance(evaled_value, basestring):
      evaled_params[key] = set(evaled_value)
    else:
      evaled_params[key] = set([evaled_value])

  for point in itertools.product(*evaled_params.values()):
    params = dict(zip(evaled_params.keys(), point))
    yield params
