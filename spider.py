# -*- coding:utf-8 -*-
import traceback
import random
import urllib2
import robotparser
from HTMLParser import HTMLParser
import urlparse
from urlparse import urljoin
from urlparse import urldefrag
import time
import logging
import chardet
# import cchardet
from bs4 import BeautifulSoup
import collections
import socket
import gc
import porn_filter

socket.setdefaulttimeout(10.0)

# Change default encoding to UTF 8
import sys
reload(sys)  # Warning: This could break imports, possibly.
sys.setdefaultencoding('UTF8')

# Set up logging to file and console
logging.basicConfig(filename='spider.log', filemode='a', level=logging.DEBUG,
                    format='%(levelname)s:%(asctime)s:%(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

DOMAIN_DELAY = 60
MAX_QUEUE_SIZE = 10000
# Workaround for memory issues
MAX_URLS_PER_RUN = 500
MAX_QUEUED_URLS_PER_DOMAIN = 10

class PageTooLargeException(Exception):
  """Thrown when a retrieved webpage is too long for the spider."""
  def __init__(self,*args,**kwargs):
    Exception.__init__(self,*args,**kwargs)

class Spider(object):
  def __init__(self, start, output, links, porn_probs):
    self.parser = LinkParser()
    self.url_queue = []
    self.url_queue.extend(start)
    self.last_access_times = {}
    self.visited = set()
    self.load_links(links)
    self.output_file = open(output, "a")
    self.links_file = open(links, "a")
    # Add User-Agent
    self.opener = urllib2.build_opener()
    self.opener.addheaders = [('User-agent', 'game_spider')]
    self.robot_parsers = {}
    self.count = 0
    self.porn_filter = porn_filter.PornFilter(porn_probs)

  def load_links(self, links):
    """Load links from a given file to initialize the spider."""
    try:
      with open(links, "r") as link_file:
        lines = [line.strip() for line in link_file.readlines()]
        for line in lines:
          try:
            kind, url = line.split(":", 1)
          except:
            logging.error("Invalid line in links.txt")
            continue
          if kind == "Visit":
            self.visited.add(url)
          elif kind == "Add":
            if self.valid_link(url):
              self.url_queue.append(url)
            else:
              logging.error("Invalid link in links.txt: %s" % url)
            if len(self.url_queue) > MAX_QUEUE_SIZE:
              self.trim_queue()
          else:
            logging.error("Unknown link type %s" % kind)
            continue
      for url in self.visited:
        if url in self.url_queue:
          self.url_queue.remove(url)
    except IOError:
      return
    self.trim_queue()

  def trim_queue(self):
    """Try to remove urls from the queue, while ensuring some from each domain."""
    urls_by_domain = collections.defaultdict(list)
    for url in self.url_queue:
      domain = self.get_domain(url)
      urls_by_domain[domain].append(url)
    if len(self.url_queue) > MAX_QUEUE_SIZE:
      max_urls = MAX_QUEUE_SIZE / 2
    else:
      max_urls = MAX_QUEUE_SIZE
    urls_per_domain = (max_urls / len(urls_by_domain)) + 1
    if urls_per_domain > MAX_QUEUED_URLS_PER_DOMAIN:
      urls_per_domain = MAX_QUEUED_URLS_PER_DOMAIN
    new_urls = []
    for domain in urls_by_domain:
      random.shuffle(urls_by_domain[domain])
      new_urls.extend(urls_by_domain[domain][:urls_per_domain])
    random.shuffle(new_urls)
    if len(new_urls) > max_urls:
      new_urls = new_urls[:max_urls]
    logging.info("Trimmed url_queue from %d urls to %d" % (len(self.url_queue), len(new_urls)))
    self.url_queue = new_urls

  def cleanup(self):
    """Run post access cleanup, including gc and trimming the queue."""
    for domain, last_access in self.last_access_times.items():
      if last_access + DOMAIN_DELAY < time.time():
        del self.last_access_times[domain]
    self.trim_queue()
    if len(self.robot_parsers) >= 100:
      self.robot_parsers = {}
    gc.collect()

  def is_cjk(self, character):
    """"
    Checks whether character is CJK.

    :param character: The character that needs to be checked.
    :type character: char
    :return: bool
    """
    return any([start <= ord(character) <= end for start, end in
                [(4352, 4607), (11904, 42191), (43072, 43135), (44032, 55215),
                 (63744, 64255), (65072, 65103), (65381, 65500),
                 (131072, 196607)]
                ])

  def is_kana(self, char):
    """Returns if a character is a hiragana or katakana character."""
    return 12352 <= ord(char) <= 12543

  def get_cjk_ratios(self, text):
    """Return counts of (cjk, kana, text length) for the given text."""
    kana_count = 0
    cjk_count = 0
    for c in text:
      if self.is_cjk(c): cjk_count += 1
      if self.is_kana(c): kana_count += 1
    logging.info("CJK counts: CJK %d Kana %d Total %d" % (cjk_count, kana_count, len(text)))
    return (cjk_count, kana_count, len(text))

  def extract_keywords(self, soup):
    """Get meta keywords, if any, from the given soup."""
    keywords = []
    try:
      keyword_text = soup.find("meta", {"name":"keywords"})['content']
      keywords.extend([x.strip() for x in keyword_text.split(",")])
    except:
      logging.info("Could not find meta keywords")
    logging.info("Meta keywords: %s" % ",".join(keywords))
    return keywords

  def check_text(self, text, extracted_text):
    """Check wether the page should be saved and spidered from.
       Don't continue if the page is not for games, has no kana, or is porn."""
    cjk, kana, length = self.get_cjk_ratios(text)
    if kana == 0:
      logging.info("Text had no kana, skipping")
      return False
    if "ゲーム" not in text:
      logging.info("URL not gaming related, skipping")
      return False
    porn_prob, words = self.porn_filter.get_porn_prob(extracted_text)
    logging.info("Porn probability: %6f" % porn_prob)
    logging.info("Determining words: %s" % " ".join(words))
    if porn_prob > .99:
      logging.info("Probably porn, skipping")
      return False
    return True

  def get_soup(self, text):
    """Get a soup from the current given HTML text."""
    return BeautifulSoup(text, "html.parser")

  def extract_text(self, soup):
    """Get just the text out of a given HTML page (i.e. no tags, etc.)"""
    for script in soup(["script", "style"]):
      script.extract()
    new_text = soup.get_text()
    lines = (line.strip() for line in new_text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    return '\n'.join(chunk for chunk in chunks if chunk)

  def record_text(self, url, extracted_text, keywords):
    """Write url, text, and keywords to the output file."""
    self.output_file.write("\n-+-+-+-+-+-+-+-+\n")
    self.output_file.write("Page #%08d: %s\n" % (len(self.visited), url))
    self.output_file.write("Keywords: %s\n" % (",".join(keywords)))
    self.output_file.write("-+-+-+-+-+-+-+-+\n")
    self.output_file.write(extracted_text)

  def get_domain(self, url):
    """Get base domain for a given URL."""
    return urlparse.urljoin(url, "/")

  def spam_check(self, domain):
    """Check if we have accessed a given domain in the past while."""
    if domain not in self.last_access_times: return False
    return self.last_access_times[domain] + DOMAIN_DELAY > time.time()

  def robot_check(self, domain, url):
    """Check robots.txt to see if we may visit a given page."""
    robots_url = urlparse.urljoin(domain, "/robots.txt")
    logging.info("Getting robot information for %s" % domain)
    if domain in self.robot_parsers:
      logging.info("Using cached robot information")
      robot_parser = self.robot_parsers[domain]
    else:
      robot_parser = robotparser.RobotFileParser(robots_url)
      self.robot_parsers[domain] = robot_parser
    try:
      robot_parser.read()
      result = robot_parser.can_fetch("game_spider", url)
    except: # Any error, like not being able to connect, or unicode errors
      return False
    logging.info("Result: %r for url %s" % (result, url))
    return result

  def get_next_url(self):
    """Get the next url for the spider to visit, respect spam/robot checks."""
    discarded_urls = []
    result = None
    while len(self.url_queue) > 0:
      url = self.url_queue.pop(0)
      domain = self.get_domain(url)
      if not self.spam_check(domain):
        if not self.robot_check(domain, url):
          continue
        result = url
        break
      else:
        discarded_urls.append(url)
    self.url_queue.extend(discarded_urls)
    return result

  def start(self):
    """Main loop."""
    while len(self.url_queue) > 0 and self.count < MAX_URLS_PER_RUN:
      self.count += 1
      logging.info("Visited: %d   url_queue: %d   robot_parsers: %d" % (len(self.visited), len(self.url_queue), len(self.robot_parsers)))
      next_url = self.get_next_url()
      if next_url is None:
        logging.info("No eligible URLs available. Waiting 1 second.")
        time.sleep(1)
        continue
      logging.info("Scanning next URL: %s" % next_url)
      try:
        text, status = self.get_page(next_url)
      except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        #trace = ''.join('!! ' + line for line in lines)  # Log it or whatever here
        logging.error("Error, skipping. Error Value:")
        logging.error(exc_value)
        continue
      text = self.decode(text)
      self.links_file.write("Visit:%s\n" % next_url)
      self.visited.add(next_url)
      domain = self.get_domain(next_url)
      self.last_access_times[domain] = time.time()
      logging.info("Status code %d" % status)
      if status / 100 == 2: # has a 2xx status code
        soup = self.get_soup(text)
        extracted_text = self.extract_text(soup)
        if self.check_text(text, extracted_text):
          keywords = self.extract_keywords(soup)
          self.record_text(next_url, extracted_text, keywords)
          new_links = self.get_links(next_url, text)
          for link in new_links:
            if link not in self.visited and self.valid_link(link):
              self.links_file.write("Add:%s\n" % link)
              self.url_queue.append(link)
      self.cleanup()
      time.sleep(1)

  def valid_link(self, url):
    """Check if a url appears to be an HTML page."""
    invalid = [".jpg", ".jpeg", ".gif", ".png", ".zip", ".gz", ".7z",
               ".exe", ".bin", ".dmg", ".mp4", ".mp3", ".mov", ".qt",
               ".mkv", ".pdf", ".wmv"]
    for i in invalid:
      if url.lower().endswith(i):
        return False
    return True

  def get_page(self, url, timeout=5):
    """Retrieve a given URL."""
    # TODO: ... this isn't actually using timeout, is it?
    #file_obj = urllib2.urlopen(url)
    file_obj = self.opener.open(url)
    text = file_obj.read(1000000)
    if len(text) == 1000000:
      raise PageTooLargeException("Page was larger than 1MB")
    return text, file_obj.getcode()

  def decode(self, text):
    """Try to convert text to unicode."""
    encoding = chardet.detect(text)['encoding']
    if not encoding:
      return text
    try:
      text = unicode(text, encoding, errors='ignore')
    except LookupError:
      pass
    return text

  def get_links(self, url, text):
    """Parse the links from a given text html page."""
    self.parser.feed(text)
    links = self.parser.get_links()
    result = []
    for link in links:
      absolute = urljoin(url, link)
      new_url, fragment = urldefrag(absolute)
      parse = urlparse.urlparse(new_url)
      if parse.scheme == "http" or parse.scheme == "https":
        result.append(new_url)
    return set(result)

class LinkParser(HTMLParser):
  def __init__(self):
    self.links = []
    HTMLParser.__init__(self)

  def handle_starttag(self, tag, attrs):
    if tag == 'a':
      for attr in attrs:
        label, data = attr
        if label == 'href':
          self.links.append(data)
          break

  def get_links(self):
    result = self.links[:]
    self.links = []
    return result

#urls = ["https://docs.python.org/2/library/htmlparser.html"]
urls = ["http://www.cty-net.ne.jp/~m7686438/"]
output_file = "output.txt"
links_file = "links.txt"
porn_probs = "porn_probs"
s = Spider(urls, output_file, links_file, porn_probs)
s.start()
sys.exit(0)
