import asyncio
import logging

logger = logging.getLogger(__name__)

try:
    import uvloop
except ModuleNotFoundError:
    logger.warning('uvloop not found, defaulting to asyncio')
else:
    uvloop.install()

EVENT_LOOP = None

def obtain_event_loop():
    global EVENT_LOOP
    if EVENT_LOOP is None:
        EVENT_LOOP = asyncio.get_event_loop()

    return EVENT_LOOP
