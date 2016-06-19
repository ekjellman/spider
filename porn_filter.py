import re
import operator
import collections
import sys
import os
import subprocess
from subprocess import PIPE, STDOUT

def interest_comparator(x):
  """Use to sort (token, prob) tuples based on probability distance from .5"""
  x_prob = x[1]
  dist_x = abs(x_prob - .5)
  return dist_x

def parse_output(output):
  """Parse mecab output to obtain parsed tokens."""
  words = set()
  for line in output:
    s = line.split()
    if len(s) == 0:
      continue
    if s[0] == "EOS":
      continue
    words.add(s[0])
  return words

class PornFilter(object):
  def __init__(self, probs_file):
    # Dictionary from token -> probability of porn | token
    self.probs = {}
    with open(probs_file, "r") as in_file:
      for line in in_file.readlines():
        line = line.strip()
        word, prob = line.split(":")
        prob = float(prob)
        if prob > .95: prob = .95
        self.probs[word] = prob

  def get_tokens(self, string):
    """Call mecab with the given string, obtaining mecab output."""
    tokens = []
    # We use files to avoid known problems with subprocess.call hanging on
    # large piped input
    with open("mecab_input", "w") as file_out:
      file_out.write(string)
    p = subprocess.call(["mecab", "-b 65536", "-omecab_output", "mecab_input"])
    with open("mecab_output", "r") as file_in:
      output = file_in.read()
    output = output.split("\n")
    words = parse_output(output)
    return words

  def get_important_tokens(self, token_probs, num_tokens):
    """Get around num_tokens of the most important tokens in token_probs."""
    # Important is defined by distance from .5. i.e. a probability of .15 is
    # more important than a probability of .4
    if len(token_probs) == 0:
      return []
    tokens = token_probs[:num_tokens]
    border = interest_comparator(tokens[-1])
    for i in xrange(num_tokens, len(token_probs)):
      a = interest_comparator(token_probs[i])
      if a < border:
        break
      tokens.append(token_probs[i])
    tokens = tokens[:(num_tokens*2)]
    return tokens

  def get_porn_prob(self, string):
    """Calculate porn probability of a given string using naive bayes."""
    tokens = self.get_tokens(string)
    token_probs = []
    for t in tokens:
      if t in self.probs:
        token_probs.append((t, self.probs[t]))
      else:
        token_probs.append((t, .5))
    token_probs = sorted(token_probs, key=interest_comparator, reverse=True)
    important_tokens = self.get_important_tokens(token_probs, 15)
    porn_factor = 1.0
    game_factor = 1.0
    important_words = []
    for t in important_tokens:
      important_words.append(t[0])
      prob = t[1]
      # Fudge to make porn "less likely"
      if prob > .75: prob = .75
      porn_factor *= prob
      game_factor *= (1.0 - prob)
    return (porn_factor / (porn_factor + game_factor), tuple(important_words))

###############
# Here and below, code used to generate corpus
###############

#def splitter(file):
#  lines = file.readlines()
#  count = 0
#  new_file = []
#  for line in lines:
#    m = re.search("^Page #\d+: ", line)
#    if m is not None:
#      if len(new_file) > 0:
#        write_file(count, new_file)
#        count += 1
#      new_file = []
#    new_file.append(line)
#  write_file(count, new_file)
#
#def write_file(count, lines):
#  with open("file_%d" % count, "w") as out_file:
#    for line in lines:
#      out_file.write(line)
#
#
#def get_porn_probs(porn_corpus, game_corpus):
#  porn_total_count, porn_word_counts = porn_corpus
#  game_total_count, game_word_counts = game_corpus
#  words = set([x for x in porn_word_counts] + [x for x in game_word_counts])
#  probs = {}
#  for word in words:
#    porn_word_count = 1.0 + porn_word_counts[word]
#    game_word_count = 1.0 + game_word_counts[word]
#    prob_word_porn = porn_word_count / (porn_total_count + 1.0)
#    prob_word_game = game_word_count / (game_total_count + 1.0)
#    prob_porn_word = prob_word_porn / (prob_word_porn + prob_word_game)
#    probs[word] = prob_porn_word
#  return probs
#
#def get_corpus(directory):
#  probs = collections.defaultdict(int)
#  count = 0
#  for filename in os.listdir(directory):
#    count += 1
#    path = os.path.join(directory, filename)
#    output = subprocess.check_output(["mecab", "-b 65536", path])
#    output = output.split("\n")
#    words = parse_output(output)
#    for word in words:
#      probs[word] += 1
#  return (count, probs)
#
#def get_tokens(filename):
#  tokens = []
#  with open(filename, "r") as in_file:
#    output = subprocess.check_output(["mecab", "-b 65536", filename])
#    output = output.split("\n")
#    words = parse_output(output)
#  return words
#
#p = PornFilter(sys.argv[1])
#for filename in os.listdir(sys.argv[2]):
#  path = os.path.join(sys.argv[2], filename)
#  with open(path, "r") as in_file:
#    file_string = in_file.read()
#  prob = p.get_porn_prob(file_string)
#
#porn_dir = sys.argv[1]
#games_dir = sys.argv[2]
#
#porn_corpus = get_corpus(porn_dir)
#game_corpus = get_corpus(games_dir)
#
#porn_probs = get_porn_probs(porn_corpus, game_corpus)
#porn_probs_sorted = sorted(porn_probs.items(), key=operator.itemgetter(1))
#
#for prob in porn_probs_sorted:
#  print "%s: %f" % prob
