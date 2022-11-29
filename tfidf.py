
# Importing libraries
from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd 
import spacy  
from nltk.stem import PorterStemmer 
from nltk.stem import SnowballStemmer 
import string 
import math
import re 

# Finding the total number of books in the corpus (needed for the tfidf calculations)
test = pd.read_csv('data/books_metadata.csv')
M = len(test)

# Using the spacy library to load the english stopwords 
en = spacy.load('en_core_web_sm')
stopwords = en.Defaults.stop_words

# Using the punctuation attribute from the string library to store a string with all the special characters 
special_char = r'[' + string.punctuation + ']'

# Using the stemmer to reduce the words to a common base form, by removing the affix
# Two different stemmers, the snowball is more agressive
porter = PorterStemmer()
snowball = SnowballStemmer(language='english')

# Using regex to search for groups that have alphanumerics or ', i.e. to break a line into words
WORD_RE = re.compile(r"[\w']+") 

# To increase computation efficiency, MapReduce should only yield words with tfidf score above a specific threshold 
# An appropriate threshold is found calculationg the 5th procentile of the tfidf scores 
threshold = 0.0219


class MR_TFIDF(MRJob):  

    # 4 MapReduce steps 
    def steps(self):
        
        return [ 
            MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
            MRStep(mapper=self.mapper_2, reducer = self.reducer_2),
            MRStep(mapper = self.mapper_3, reducer = self.reducer_3),
            MRStep(mapper = self.mapper_4)
        ]

    # Takes a description as input, removes stopwords, punctuation and convert to lowercase
    # Returns a list of words.
    def split_and_clean(self, line):  
        words = []
        line = re.sub(special_char, ' ', line) #remove special characters from line
        for word in WORD_RE.findall(line): #for each word in the line
            if word.lower() not in stopwords and len(word)>1: #if the word is not a stop word and length > 1
                words.append(snowball.stem(word.lower())) #append the root of the word to a list
        return words #return the list of words

    # Mapper of the first MapReduce job
    def mapper_1(self, _, line):  
        id_found = re.search(r"(^\d+),", line)
        description_found = re.search(r",\"(.+)\",", line)
        if id_found: 
            id = int(id_found.group(1))
            if description_found:
                d = description_found.group(1)
                description = str()
                flag = True
                for char in d:
                    if char =='"':
                        flag = False
                    if flag:
                        description += char
                for word in self.split_and_clean(description): 
                    yield (word,id), 1  


    # Reducer of the first MapReduce job
    def reducer_1(self, word_id, counts): 
        yield (word_id[0], word_id[1]), sum(counts) 

    # Mapper of the second MapReduce job
    def mapper_2(self, word_id, n):
        word, id = word_id[0], word_id[1], 
        yield id, (word,n) 

    # Reducer of the second MapReduce job 
    def reducer_2(self, id, word_n):
        
        word_list = []
        n_list = []
        N = 0 
        num_el = 0

        for value in word_n:
            word, n = value[0], value[1]

            word_list.append(word)
            n_list.append(n)
            N+=int(n)

            num_el+=1

        for i in range(num_el):
            yield (word_list[i],id), (n_list[i],N)   

    # Mapper of the third MapReduce job 
    def mapper_3(self, word_id, n_N):
        word, id = word_id[0], word_id[1]
        n, N = n_N[0], n_N[1]
        yield word, (id, n, N, 1)


    # Reducer of the third MapReduce job
    def reducer_3(self, word, id_n_N_counts):

        id_list = []
        n_list = []
        N_list = []
        m = 0

        num_el = 0

        for value in id_n_N_counts:
            id, n, N, count = value[0], value[1], value[2], value[3]

            id_list.append(id)
            n_list.append(n)
            N_list.append(N)

            m+=count

            num_el +=1

        for i in range(num_el):
            yield (word, id_list[i]), (n_list[i], N_list[i],m)

    # Mapper of the fourth MapReduce job 
    def mapper_4(self, word_id, n_N_m):
        word, id = word_id[0], word_id[1]
        n, N, m = n_N_m[0], n_N_m[1], n_N_m[2]

        tf = (n/N) 
        idf = math.log(M/m) 
        tfidf = tf*idf

        if tfidf > threshold:
            yield (word, id), tfidf

    

if __name__ == '__main__':
    MR_TFIDF.run()
