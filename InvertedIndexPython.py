#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
import nltk
import operator


# In[2]:


#reading the files
fileList = []
path = 'Desktop/bbcsports/bbc_sports_articles'
for file in os.listdir(path):
    if os.path.isfile(os.path.join(path,file)):
        fileList.append(file)
fileList.reverse()


# In[4]:


#checking to see if the files were accessed or not,
fileList


# In[5]:


#it is time to follow the steps to build an inverted index:
# 1. first we fetch the documents
data = pd.DataFrame()
fileContent = []
documentId = []
i = 1
for f in fileList:
    content = open(os.path.join(path,f), "r")
    fileContent.append(content.read())
    
    documentId.append(i)
    i += 1
    content.close()


# In[6]:


data['text'] = fileContent
data['DocId'] = documentId
data


# In[8]:


#datastructure for clean articles
cleanArticles = []

for i in data['text']:
    cleanArticles.append(i.replace("\n", " ").replace("\'", " "))
    #getting rid of single character words
    cleanArticles = [" ".join([w for w in x.split() if len(w) > 1]) for x in cleanArticles]
    #converting everything to lowerCase to decrease the total number of unique
    #words
    cleanArticles = [x.lower() for x in cleanArticles]
    
data['PartlyCleanArticle'] = cleanArticles


# In[18]:


#2. we now remove the stop words and punctuations
import string
punctuations = string.punctuation

from nltk.corpus import stopwords
stopword = stopwords.words("english")

def clean(text):
    cleanText = text
    #removing punctuations
    cleanText = "".join(word for word in cleanText if word not in punctuations)
    #time to remove the stop words
    words = cleanText.split(" ")
    words = [word for word in words if word not in stopword]
    cleanText = " ".join(words)
    return cleanText


# In[19]:


data['clean'] = data['PartlyCleanArticle'].apply(clean)
data


# In[20]:


#3. it is now time to lemmatize the sentences
from nltk.stem.wordnet import WordNetLemmatizer
lem = WordNetLemmatizer()

def lemmatizeText(text):
    words = text.split(" ")
    words = [lem.lemmatize(word, 'n') for word in words]
    words = [lem.lemmatize(word, 'v') for word in words]
    text = " ".join(words)
    return text


# In[22]:


data['cleanLemmatizedText'] = data['clean'].apply(lemmatizeText)
data


# In[23]:


def all_indices(value, qlist):
    indices = []
    idx = -1
    while True:
        try:
            idx = qlist.index(value, idx+1)
            indices.append(idx)
        except ValueError:
            break
    return indices


# In[53]:


#Now that we have cleaned the text, removed stopwords, puntuations and lemmatized it, it is time to build a 
#inverted index (a very basic one)
inputData = data['cleanLemmatizedText']
invertedIndex = dict()
wordsChecked = set()
i = 1
for text in inputData:
    words = text.split(" ")
    for word in words:
        indices = all_indices(word, words)
        if word not in wordsChecked:
            value = {"id": i, 
                     "termFrequency" : len(indices) / len(words)}
            if word not in invertedIndex:
                temp = []
                invertedIndex[word] = temp
                invertedIndex[word].append(value)
            else:
                invertedIndex[word].append(value)
        wordsChecked.add(word)
    i += 1
    wordsChecked.clear()


# In[54]:


invertedIndex


# In[56]:


for key in invertedIndex:
    if type(invertedIndex[key])  is list:
        invertedIndex[key].sort(key=operator.itemgetter('termFrequency'), reverse = True)


# In[57]:


#Check to see if sorting works as intended
invertedIndex['france']


# In[59]:


#final Inverted Index:
invertedIndex


# In[ ]:




