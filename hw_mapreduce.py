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

# now we can go to implement the reduce function 
# so we have for each key a list of values, the main goal of reduce is to compute all times the key appear in fragments 
# input : (key, list_values) - output : (key, value)
def reduce(key, values):
    result = 0 
    for c in values:
        result += c 
    return (key, result)

'''
MapReduce -> " divide and rule "

the main steps : 
    1. Choose a way to split the data 
    2. Choose the key to use for the targeted problem
    3. Write the function code for the MAP operation
    4. Write the function code for the REDUCE operation

'''

# the MapReduce is a good strategy, but it isn't always easy to reformulate 
# It's even sometimes impossible  !!!!

# To familiarize with the MapReduce logique we go to implement others examples 

'''
First Example: Multiplication (Matrix * Vector) for the famous alogrithm PageRank used to schedule 
              the results of a research web

steps : 
    
    - Matrix is stocked as a triplet (i, j, M[i][j]) and we don't represent the 0's positions 
    - The same thing for the Vector V (i, V[i])
    - here we have 2 cases:
    
        ./ Case 1: V is small enough ro fit in the memory of the Map node
            
            * Map(key, value) will give us the peers (i, M[i][j] * V[i])
                SHUFFLE & SORT : Groups all values assosicieted with the same key i in a pair (i, [v1, v2, ...])
            * Reduce(key, value) will sum all vaulues with a gaven key
        
        ./ Case 2: V is too large to fit in the memory of the Map node 
        
            Here we have to split V to horizontal stripes and M to virticla stripes and we apply the same strategy
'''



   












