
# Optimized broadcast forwarding for faster speed (Pyrogram)
# Replaces slow per-message sequential forwarding with batched forwards + concurrency control
# Keep original imports needed by other parts of the bot.

from pyrogram.errors import InputUserDeactivated, UserNotParticipant, FloodWait, UserIsBlocked, PeerIdInvalid, RPCError
from database import db
from pyrogram import Client, filters
from config import Config
import asyncio
import datetime
import time
import logging
import math

LOG = logging.getLogger(__name__)

# Recommended: configure these in Config or env
BATCH_SIZE = getattr(Config, "FORWARD_BATCH_SIZE", 20)  # number of message_ids to forward in one API call
CONCURRENT_TARGETS = getattr(Config, "FORWARD_CONCURRENCY", 8)  # how many targets to process concurrently
STATUS_UPDATE_INTERVAL = 20  # update status every N targets

async def broadcast_messages(client: Client, source_chat_id: int, message_ids: list, targets: list, status_message=None):
    """
    Fast broadcast: forward message_ids (from source_chat_id) to a list of targets.
    - Groups message_ids into batches and forwards each batch in a single API call per target.
    - Processes multiple targets concurrently with a semaphore to avoid FloodWait.
    - Catches FloodWait and sleeps for required duration.
    """
    start_time = time.time()
    total = len(targets)
    success = blocked = deleted = failed = 0

    sem = asyncio.Semaphore(CONCURRENT_TARGETS)

    # prepare batches of message_ids (Pyrogram supports forwarding multiple ids at once)
    batches = [message_ids[i:i+BATCH_SIZE] for i in range(0, len(message_ids), BATCH_SIZE)]

    async def send_to_target(target):
        nonlocal success, blocked, deleted, failed
        async with sem:
            try:
                for batch in batches:
                    # forward the whole batch in a single API call
                    await client.forward_messages(chat_id=target, from_chat_id=source_chat_id, message_ids=batch)
                success += 1
                return True
            except FloodWait as e:
                wait_for = int(e.x) if hasattr(e, "x") else int(getattr(e, "value", 5))
                LOG.warning("FloodWait %s seconds while forwarding to %s — sleeping", wait_for, target)
                await asyncio.sleep(wait_for + 1)
                # retry once after sleeping
                try:
                    for batch in batches:
                        await client.forward_messages(chat_id=target, from_chat_id=source_chat_id, message_ids=batch)
                    success += 1
                    return True
                except Exception as e2:
                    LOG.exception("Failed after FloodWait retry for %s: %s", target, e2)
                    failed += 1
                    return False
            except UserIsBlocked:
                blocked += 1
                return False
            except PeerIdInvalid:
                deleted += 1
                return False
            except Exception as e:
                LOG.exception("Unexpected error forwarding to %s: %s", target, e)
                failed += 1
                return False

    # create tasks in controlled groups to avoid creating thousands of tasks at once
    tasks = []
    for idx, tgt in enumerate(targets, 1):
        tasks.append(asyncio.create_task(send_to_target(tgt)))
        # throttle task creation in bursts to keep memory reasonable
        if len(tasks) >= 200:
            results = await asyncio.gather(*tasks)
            tasks = []
        # update status occasionally
        if status_message and idx % STATUS_UPDATE_INTERVAL == 0:
            try:
                await status_message.edit(f"Broadcast progress: {idx}/{total} — Success: {success} Blocked: {blocked} Deleted: {deleted} Failed: {failed}")
            except Exception:
                pass

    if tasks:
        await asyncio.gather(*tasks)

    time_taken = datetime.timedelta(seconds=int(time.time()-start_time))
    if status_message:
        try:
            await status_message.edit(f"Broadcast Completed:\nCompleted in {time_taken}\nTotal: {total}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}\nFailed: {failed}")
        except:
            pass

    return {"total": total, "success": success, "blocked": blocked, "deleted": deleted, "failed": failed}
