# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 18:50:48 2020

@author: Adref
"""

# This module is here to illustrate the principal points about MapReduce 

# Imports 

from collections import defaultdict



# Example of a simple function to count words in a collection 
# Here we can easily do that 

def wordCount(text):
    counts = defaultdict(int)
    for word in text.split():
        counts[word.lower()] += 1
    return counts

'''
Now if we have a text wich has more than a 30 M words we can't do the count with a simple function, thats take a lot of time     
In this case we can illustrate the main aspect of MapReduce
We are going to work on two extracts from the text of the song "Le Jour se lève" by Grand Corps Malade

'''

# We supposed that our data is splited and cleaned from the stop words, etc finally we got that 

D1 = {"./lot1.txt" : "jour lève notre grissaille"}
D2 = {"./lot2.txt" : "trottoir notre ruelle notre tour"}
D3 = {"./lot3.txt" : "jour lève notre envie vous"}
D4 = {"./lot4.txt" : "faire comprendre tous notre tour"}

def map(key, value):
    intermediate = []
    for word in value.split():
        intermediate.append((word, 1))
    return intermediate

'''
Example : 
    (Lot1, "jour lève notre grissaille") -> [(jour, 1), (lève, 1), (notre, 1), (grissaille, 1)]
'''





