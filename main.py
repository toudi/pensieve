import sys
from datetime import datetime, timedelta
from decimal import Decimal
from random import randint, randrange, choice

import pytz

from schemas.weather import Weather, Description
from storage import Storage

now = datetime.utcnow().replace(tzinfo=pytz.utc)

CITY = "Sao Paulo"

with Storage() as storage:
    if "-generate" in sys.argv:
        for row in range(20):
            timestamp = now - timedelta(seconds=-1 * randint(0, 24 * 3600))

            storage.add(
                Weather(
                    timestamp=timestamp,
                    city=CITY,
                    temperature=Decimal(str(randrange(-300, 500) / 10)),
                    rainfall=randrange(0, 200),
                    description=choice(list(Description)),
                )
            )

    for measurement in storage.query(
        Weather,
        dimensions={"city": CITY},
        start_time=datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc),
        # end_time=datetime(2023, 4, 1, 19, 50, tzinfo=pytz.utc),
    ):
        print(measurement)
