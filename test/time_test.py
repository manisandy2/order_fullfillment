from datetime import datetime
import pytz

now_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
print(now_ist.strftime("%Y-%m-%d %H:%M:%S"))