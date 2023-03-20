import datetime
from dateutil.relativedelta import relativedelta


now = datetime.datetime.now().astimezone()


intervals = [
    {"interval": 60, "interval_name": "1m", "max_history_days": 6},
    {"interval": 300, "interval_name": "5m", "max_history_days": 59},
    {"interval": 3600, "interval_name": "1h", "max_history_days": 500},
    {"interval": 86400, "interval_name": "1d", "max_history_days": 2000},
]

for i in intervals:
    out_date = now
    out_date = out_date.replace(second=0)
    out_date = out_date.replace(microsecond=0)
    if i["interval_name"] == "1m":
        ...
    elif i["interval_name"] == "5m":
        mod_minute = out_date.minute % 5
        out_date = out_date.replace(minute=out_date.minute - mod_minute)
    elif i["interval_name"] == "1h":
        out_date = out_date.replace(minute=0)
    elif i["interval_name"] == "1d":
        out_date = out_date.replace(minute=0, hour=0)

    print(out_date)

# interval_in_units = relativedelta(seconds=interval)


print("a")
#    mod_min = date_obj.minute % interval_settings.interval
#    date_obj_aligned_minutes = date_obj - relativedelta(minutes=mod_min)
#    date_obj_aligned = date_obj_aligned_minutes.replace(microsecond=0).replace(second=0)
