#!/usr/bin/env python
# encoding: utf-8


"""
    This is a module of crawler for Tumbler.
    The project is fork from https://github.com/dixudx/tumblr-crawler
"""

"""
@version    :   1.0
@author     :   MoMo
@license    :   Apache Licence 
@contact    :   yuanhongbin9090@gmail.com
@site       :   http://yuanmomo.net
@software   :   PyCharm
@file       :   crawler.py
@time       :   26/07/2017 15:35
"""

import sys
import os
import requests
import xmltodict
import json
from six.moves import queue as Queue
from threading import Thread
import re

# Debug switch
DEBUG = False

# Location of proxies json config
PROXIES_JSON = "./proxies.json"

# Location of proxies json config
SITE_CONFIG = "./sites.txt"

# num of concurrent download threads
DOWNLOAD_THREADS = 10

# Medium Index Number that Starts from
START = 0

# Directory to save
SAVE_DIRECTORY_PARENT = "/Volumes/momo/tumblr"

# Fetch num per page
PAGE_SIZE = 50

# Setting timeout
TIMEOUT = 10

# Retry times
RETRY = 5


def readFile(file_to_read, type="string"):
    """
        Read the file content. If type is json, then use json.load or store the lines into a list.
        :param file_to_read:
        :param type:
        :return:
    """
    if os.path.exists(file_to_read):
        with open(file_to_read, "r") as ftr:
            try:
                content = None;
                if "string" == type:
                    content = ftr.read().splitlines()
                elif "json" == type:
                    content = json.load(ftr);
                return content
            except Exception, e:
                print "Read the type [%s] file [%s] ERROR [%s]." % (type, file_to_read, str(e))
                sys.exit(1)


def video_hd_match():
    hd_pattern = re.compile(r'.*"hdUrl":("([^\s,]*)"|false),')

    def match(video_player):
        hd_match = hd_pattern.match(video_player)
        try:
            if hd_match is not None and hd_match.group(1) != 'false':
                return hd_match.group(2).replace('\\', '')
        except:
            return None

    return match


def video_default_match():
    default_pattern = re.compile(r'.*src="(\S*)" ', re.DOTALL)

    def match(video_player):
        default_match = default_pattern.match(video_player)
        if default_match is not None:
            try:
                return default_match.group(1)
            except:
                return None

    return match


class CrawlerDownloader(Thread):
    """

    """

    def __init__(self, id, queue, proxies=None):
        super(CrawlerDownloader, self).__init__()
        self.id = id
        self.queue = queue
        self.proxies = proxies
        self._register_regex_match_rules()

    def run(self):
        while True:
            mediaType, post, targetDirectory = self.queue.get()
            self.download(mediaType, post, targetDirectory)
            self.queue.task_done()

    # can register differnet regex match rules
    def _register_regex_match_rules(self):
        # will iterate all the rules
        # the first matched result will be returned
        self.regex_rules = [video_hd_match(), video_default_match()]

    def download(self, mediaType, post, targetDirectory):
        try:
            medium_url = self._handle_medium_url(mediaType, post)
            if medium_url is not None:
                self._download(mediaType, medium_url, targetDirectory)
        except TypeError:
            pass

    def _handle_medium_url(self, medium_type, post):
        try:
            if medium_type == "photo":
                return post["photo-url"][0]["#text"]

            if medium_type == "video":
                video_player = post["video-player"][1]["#text"]
                for regex_rule in self.regex_rules:
                    matched_url = regex_rule(video_player)
                    if matched_url is not None:
                        return matched_url
                else:
                    raise Exception
        except:
            raise TypeError("Unable to find the right url for downloading. "
                            "Please open a new issue on "
                            "https://github.com/dixudx/tumblr-crawler/"
                            "issues/new attached with below information:\n\n"
                            "%s" % post)

    def _download(self, medium_type, medium_url, target_folder):
        medium_name = medium_url.split("/")[-1].split("?")[0]
        if medium_type == "video":
            if not medium_name.startswith("tumblr"):
                medium_name = "_".join([medium_url.split("/")[-2],
                                        medium_name])

            medium_name += ".mp4"

        file_path = os.path.join(target_folder, medium_name)
        if not os.path.isfile(file_path):
            print("Downloading %s from %s.\n" % (medium_name,
                                                 medium_url))
            retry_times = 0
            while retry_times < RETRY:
                try:
                    resp = requests.get(medium_url,
                                        stream=True,
                                        proxies=self.proxies,
                                        timeout=TIMEOUT)
                    if resp.status_code == 403:
                        retry_times = RETRY
                        print("Access Denied when retrieve %s.\n" % medium_url)
                        raise Exception("Access Denied")
                    with open(file_path, 'wb') as fh:
                        for chunk in resp.iter_content(chunk_size=1024):
                            fh.write(chunk)
                    break
                except:
                    # try again
                    pass
                retry_times += 1
            else:
                try:
                    os.remove(file_path)
                except OSError:
                    pass
                print("Failed to retrieve %s from %s.\n" % (medium_type,
                                                            medium_url))


class Crawler(object):
    """
        Craw the links of photos and videos of each site and put them into a queue.
    """

    def __init__(self, sites, proxies=None, directory="."):
        self.sites = sites
        self.proxies = proxies
        self.queue = Queue.Queue();
        self.saveDirectoryParent = directory;
        self.start();

    def start(self):
        # init the download threads
        for i in range(DOWNLOAD_THREADS):
            worker = CrawlerDownloader(i, self.queue, proxies=self.proxies)
            # Setting daemon to True will let the main thread exit
            # even though the workers are blocking
            worker.daemon = True
            worker.start()

        # start to get the download links
        for site in self.sites:
            self.fetchLinksForSite(site)

    def fetchLinksForSite(self, site):
        """"""
        self.fetchVideoLinks(site)
        # self.fetchPhotoLinks(site)

    def fetchVideoLinks(self, site):
        self._fetchLinks(site, "video")
        # wait for the queue to finish processing all the tasks from one
        # single site
        self.queue.join()
        print("Finish Downloading All the videos from %s" % site)

    def fetchPhotoLinks(self, site):
        self._fetchLinks(site, "photo")
        # wait for the queue to finish processing all the tasks from one
        # single site
        self.queue.join()
        print("Finish Downloading All the photos from %s" % site)

    def _fetchLinks(self, site, mediaType="photo"):
        targetDirectory = self.saveDirectoryParent + os.path.sep + site
        if not os.path.isdir(targetDirectory):
            os.mkdir(targetDirectory)

        # tumblr URL
        FETCH_LINK_URL = "http://{0}.tumblr.com/api/read?type={1}&num={2}&start={3}"

        start = START
        while True:
            fetchUrl = FETCH_LINK_URL.format(site, mediaType, PAGE_SIZE, start);
            print "Fetch links from  [%s] " % fetchUrl
            response = requests.get(fetchUrl, proxies=self.proxies)
            if response.status_code == 404:
                print("Site %s does not exist" % site)
                break
            try:
                data = xmltodict.parse(response.content)
                posts = data["tumblr"]["posts"]["post"]
                for post in posts:
                    try:
                        # if post has photoset, walk into photoset for each photo
                        photoset = post["photoset"]["photo"]
                        for photo in photoset:
                            self.queue.put((mediaType, photo, targetDirectory))
                    except:
                        # select the largest resolution
                        # usually in the first element
                        self.queue.put((mediaType, post, targetDirectory))
                start += PAGE_SIZE
            except KeyError:
                break


# Main
if __name__ == '__main__':
    # read the proxies config
    proxies = readFile(PROXIES_JSON, "json")
    if proxies is not None and len(proxies) > 0:
        print "You are using proxies of :\n%s" % (str(proxies))

    # read the sites config
    sites = readFile(SITE_CONFIG)
    if None == sites or len(sites) == 0:
        print "Please config which sites you want to crawl in sites.txt."
        sys.exit(2)

    # check the saving directory
    if not os.path.exists(SAVE_DIRECTORY_PARENT):
        SAVE_DIRECTORY_PARENT = os.getcwd();

    # init Crawler
    Crawler(sites, proxies, SAVE_DIRECTORY_PARENT)
