#-*- coding: utf-8 -*-
# Name: Mitesh Gadgil	
# Email: mgadgil@ucsd.edu
# PID: A53095373
from pyspark import SparkContext
sc = SparkContext()

# 5. Your program will be killed if it cannot finish in 5 minutes. The running time of last 100 submissions (yours and others) can be checked at the "View last 100 jobs" tab. For your information, here is the running time of our solution:
#    * 1GB test:  53 seconds,
#    * 5GB test:  60 seconds,
#    * 20GB test: 114 seconds.
# try join gid:[usr id, tweet]
# filter earlier
# cache things

def print_count(rdd):
    print 'Number of elements:', rdd.count()
    
with open('../Data/hw2-files-1gb.txt') as f:
    files = [l.strip() for l in f.readlines()]

textRDD = sc.textFile(','.join(files)).cache()

def print_count(rdd):
    print 'Number of elements:', rdd.count()

#RDD = sc.textFile("C:\Users\mitesh\Desktop\UCSD_BigData_2016\Data\hw2-input.txt")
#textRDD = RDD.cache()

print_count(textRDD)

# # Part 1: Parse JSON strings to JSON objects

import ujson
def safe_parse(raw_json):
    try:
        json_obj = ujson.loads(raw_json)
    except ValueError, e:
        return False
    if (("user" in json_obj) & ("text" in json_obj)):
        return True
    else: return False

# your code here
#validRDD = textRDD.map(lambda text: text.encode('utf-8'))\
#				.filter(safe_parse)\
#				.map(lambda tweet: (ujson.loads(tweet)["user"]["id_str"],ujson.loads(tweet)["text"]))

validRDD = textRDD.filter(safe_parse)\
				.map(lambda tweet: (ujson.loads(tweet)["user"]["id_str"],ujson.loads(tweet)["text"]))

def print_users_count(count):
    print 'The number of unique users is:', count

print_users_count(validRDD.keys().distinct().count())
                 
# your code here
import pickle
#partition = pickle.load(open("C:\Users\mitesh\Desktop\UCSD_BigData_2016\Data\users-partition.pickle",'rb'))
partition = pickle.load(open("../Data/users-partition.pickle",'rb'))
   
group_count = validRDD.map(lambda tuple: (partition[tuple[0]],1) if tuple[0] in partition else (7,1))\
                .reduceByKey(lambda a,b: a+b)\
                .sortByKey()
                           

def print_post_count(counts):
    for group_id, count in counts:
        print 'Group %d posted %d tweets' % (group_id, count)

print_post_count(group_count.collect())


# # Part 3:  Tokens that are relatively popular in each user partition


# %load happyfuntokenizing.py
#!/usr/bin/env python

'''
This code implements a basic, Twitter-aware tokenizer.

A tokenizer is a function that splits a string of text into words. In
Python terms, we map string and unicode objects into lists of unicode
objects.

There is not a single right way to do tokenizing. The best method
depends on the application.  This tokenizer is designed to be flexible
and this easy to adapt to new domains and tasks.  The basic logic is
this:

1. The tuple regex_strings defines a list of regular expression
   strings.

2. The regex_strings strings are put, in order, into a compiled
   regular expression object called word_re.

3. The tokenization is done by word_re.findall(s), where s is the
   user-supplied string, inside the tokenize() method of the class
   Tokenizer.

4. When instantiating Tokenizer objects, there is a single option:
   preserve_case.  By default, it is set to True. If it is set to
   False, then the tokenizer will downcase everything except for
   emoticons.

The __main__ method illustrates by tokenizing a few examples.

I've also included a Tokenizer method tokenize_random_tweet(). If the
twitter library is installed (http://code.google.com/p/python-twitter/)
and Twitter is cooperating, then it should tokenize a random
English-language tweet.


Julaiti Alafate:
  I modified the regex strings to extract URLs in tweets.
'''

__author__ = "Christopher Potts"
__copyright__ = "Copyright 2011, Christopher Potts"
__credits__ = []
__license__ = "Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Unported License: http://creativecommons.org/licenses/by-nc-sa/3.0/"
__version__ = "1.0"
__maintainer__ = "Christopher Potts"
__email__ = "See the author's website"

######################################################################

import re
import htmlentitydefs

######################################################################
# The following strings are components in the regular expression
# that is used for tokenizing. It's important that phone_number
# appears first in the final regex (since it can contain whitespace).
# It also could matter that tags comes after emoticons, due to the
# possibility of having text like
#
#     <:| and some text >:)
#
# Most imporatantly, the final element should always be last, since it
# does a last ditch whitespace-based tokenization of whatever is left.

# This particular element is used in a couple ways, so we define it
# with a name:
emoticon_string = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth      
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
    )"""

# The components of the tokenizer:
regex_strings = (
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?            
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?    
      \d{3}          # exchange
      [\-\s.]*   
      \d{4}          # base
    )"""
    ,
    # URLs:
    r"""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"""
    ,
    # Emoticons:
    emoticon_string
    ,    
    # HTML tags:
     r"""<[^>]+>"""
    ,
    # Twitter username:
    r"""(?:@[\w_]+)"""
    ,
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    ,
    # Remaining word types:
    r"""
    (?:[a-z][a-z'\-_]+[a-z])       # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots. 
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:
    
word_re = re.compile(r"""(%s)""" % "|".join(regex_strings), re.VERBOSE | re.I | re.UNICODE)

# The emoticon string gets its own regex so that we can preserve case for them as needed:
emoticon_re = re.compile(regex_strings[1], re.VERBOSE | re.I | re.UNICODE)

# These are for regularizing HTML entities to Unicode:
html_entity_digit_re = re.compile(r"&#\d+;")
html_entity_alpha_re = re.compile(r"&\w+;")
amp = "&amp;"

######################################################################

class Tokenizer:
    def __init__(self, preserve_case=False):
        self.preserve_case = preserve_case

    def tokenize(self, s):
        """
        Argument: s -- any string or unicode object
        Value: a tokenize list of strings; conatenating this list returns the original string if preserve_case=False
        """        
        # Try to ensure unicode:
        try:
            s = unicode(s)
        except UnicodeDecodeError:
            s = str(s).encode('string_escape')
            s = unicode(s)
        # Fix HTML character entitites:
        s = self.__html2unicode(s)
        # Tokenize:
        words = word_re.findall(s)
        # Possible alter the case, but avoid changing emoticons like :D into :d:
        if not self.preserve_case:            
            words = map((lambda x : x if emoticon_re.search(x) else x.lower()), words)
        return words

    def tokenize_random_tweet(self):
        """
        If the twitter library is installed and a twitter connection
        can be established, then tokenize a random tweet.
        """
        try:
            import twitter
        except ImportError:
            print "Apologies. The random tweet functionality requires the Python twitter library: http://code.google.com/p/python-twitter/"
        from random import shuffle
        api = twitter.Api()
        tweets = api.GetPublicTimeline()
        if tweets:
            for tweet in tweets:
                if tweet.user.lang == 'en':            
                    return self.tokenize(tweet.text)
        else:
            raise Exception("Apologies. I couldn't get Twitter to give me a public English-language tweet. Perhaps try again")

    def __html2unicode(self, s):
        """
        Internal metod that seeks to replace all the HTML entities in
        s with their corresponding unicode characters.
        """
        # First the digits:
        ents = set(html_entity_digit_re.findall(s))
        if len(ents) > 0:
            for ent in ents:
                entnum = ent[2:-1]
                try:
                    entnum = int(entnum)
                    s = s.replace(ent, unichr(entnum))	
                except:
                    pass
        # Now the alpha versions:
        ents = set(html_entity_alpha_re.findall(s))
        ents = filter((lambda x : x != amp), ents)
        for ent in ents:
            entname = ent[1:-1]
            try:            
                s = s.replace(ent, unichr(htmlentitydefs.name2codepoint[entname]))
            except:
                pass                    
            s = s.replace(amp, " and ")
        return s


from math import log

tok = Tokenizer(preserve_case=False)

def get_rel_popularity(c_k, c_all):
    return log(1.0 * c_k / c_all) / log(2)


def print_tokens(tokens, gid = None):
    group_name = "overall"
    if gid is not None:
        group_name = "group %d" % gid
    print '=' * 5 + ' ' + group_name + ' ' + '=' * 5
    for t, n in tokens:
        print "%s\t%.4f" % (t, n)
    print

tokenRDD = validRDD.map(lambda tup: (tup[0],tup[1].encode('utf-8')))\
				.flatMap(lambda tweet: set(tok.tokenize(tweet[1]))).cache()
print_count(tokenRDD.distinct())


# your code here
       
token_occur = tokenRDD.map(lambda x: (x,1))\
                .reduceByKey(lambda a,b: a+b)\
                .filter(lambda x: x[1]>99).cache()

token_occur1 = token_occur.map(lambda (k,v): (v,k))\
                .sortByKey(False)\
                .map(lambda tup:(tup[1],tup[0])) 

                
print_count(token_occur1.keys())        
print_tokens(token_occur1.take(20))

token_all = dict(token_occur.collect())

gid_tweet = validRDD.map(lambda tuple: (partition[tuple[0]],tuple[1].encode('utf-8')) if tuple[0] in partition else (7,tuple[1].encode('utf-8'))).cache()
                
   
for n in range(8):
    group_tweets = gid_tweet.filter(lambda x: x[0]==n)\
                    .flatMap(lambda text: set(tok.tokenize(text[1])))\
                    .filter(lambda x: x in token_all)\
                    .map(lambda x: (x,1))\
                    .reduceByKey(lambda a,b: a+b)\
                    .map(lambda tup: (get_rel_popularity(tup[1],token_all[tup[0]]),tup[0]))\
                    .sortByKey(False)\
                    .map(lambda (k,v): (v.encode('utf-8'),k))
    print_tokens(group_tweets.take(10),n)



# (4) (optional, not for grading) The users partition is generated by a machine learning algorithm that tries to group the users by their political preferences. Three of the user groups are showing supports to Bernie Sanders, Ted Cruz, and Donald Trump. 
# 
# If your program looks okay on the local test data, you can try it on the larger input by submitting your program to the homework server. Observe the output of your program to larger input files, can you guess the partition IDs of the three groups mentioned above based on your output?

# In[ ]:

# Change the values of the following three items to your guesses
users_support = [
    (3, "Bernie Sanders"),
    (5, "Ted Cruz"),
    (6, "Donald Trump")]

for gid, candidate in users_support:
    print "Users from group %d are most likely to support %s." % (gid, candidate)


