#!/usr/bin/env python3

import os
import json
import queue
import asyncio
import aiohttp
import aiofiles
import pyinotify

from nxtools import *

settings = {
    "target_urls" : [],
    "cache_dir" : "cache.dir",
    "record_dir" : "record.dir",
    "remote_dir" : "events",
    "recording" : False,
    "segment_exts" : ["ts"],
    "manifest_exts" : ["m3u8"]
}

try:
    settings.update(json.load(open("settings.json")))
except:
    critical_error("Unable to open settings.json")


if not os.path.exists(settings["cache_dir"]):
    os.makedirs(settings["cache_dir"])

if settings["recording"] and (not os.path.exists("record_dir")):
    os.makedirs(settings["record_dir"])





file_queue = queue.Queue()


def enqueue_waiting():
    #TODO: sort by mtime?
    for f in get_files(settings["cache_dir"], exts=settings["segment_exts"] + settings["manifest_exts"]):
        file_queue.put(os.path.abspath(f.path))


async def upload_queued():
    async with aiohttp.ClientSession() as session:
        while True:
            if file_queue.empty():
                await asyncio.sleep(.1)
                continue

            path = file_queue.get()
            basename = os.path.basename(path)

            async with aiofiles.open(path, "rb") as f:
                payload = await f.read()

            for url in settings["target_urls"]:
                f.seek(0)
                response = await session.post(
                        url,
                        data=payload,
                        headers={
                            "X-Pushkin-Filename" : basename,
                            "X-Pushkin-Directory" : settings["remote_dir"],
                        }
                )
                try:
                    msg = await response.json()
                except:
                    msg = await response.text()
                    logging.error("Non-json response. something is wrong")
                    file_queue.put(path)
                    break
                else:
                    if response.status == 201:
                        logging.info("Uploaded {} to {}".format(basename, url))
                    else:
                        logging.error(msg["message"])
                        file_queue.put(path)
                        break
            else:
                if os.path.splitext(basename)[1][1:] in settings["segment_exts"]:
                    if settings["recoding"]:
                        try:
                            target_path = os.path.join(settings["record_dir"], basename)
                            os.rename(path, target_path)
                        except:
                            pass
                    else:
                        try:
                            os.remove(path)
                        except:
                            pass

            qs = file_queue.qsize()
            if qs:
                logging.debug("{} files still enqueued".format(qs))




class EventHandler(pyinotify.ProcessEvent):
    """inotify handler, which puts new files into the global queue"""
    def process_default(self, event):
        if event.dir:
            return
        if os.path.splitext(event.pathname)[1][1:] not in settings["segment_exts"] + settings["manifest_exts"]:
            return
        if event.maskname not in ["IN_CLOSE_WRITE", "IN_MOVED_TO"]:
            return
        logging.debug("Got new file: ", event.pathname, event.maskname)
        file_queue.put(event.pathname)


def watch(loop, directory:str, handler:pyinotify.ProcessEvent) -> pyinotify.AsyncioNotifier:
    """Watch selecected directory using inotify"""
    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_MOVED_TO | pyinotify.IN_CLOSE_WRITE
    notifier = pyinotify.AsyncioNotifier(wm, loop, default_proc_fun=handler())
    wdd = wm.add_watch(
        directory,
        mask,
        rec=True,
        auto_add=True
    )
    return notifier



if __name__ == "__main__":
    enqueue_waiting()

    loop = asyncio.get_event_loop()
    notifier = watch(loop, settings["cache_dir"], EventHandler)


    logging.goodnews("Starting Pushkin, watching '{}'".format(settings["cache_dir"]))
    try:
        loop.run_until_complete(upload_queued())
    except Exception:
        log_traceback()
        print()
        logging.info("Shutting_down")

    loop.stop()
    notifier.stop()
