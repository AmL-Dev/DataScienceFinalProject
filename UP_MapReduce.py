 
from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd

df_book = pd.read_csv("data/books_metadata.csv")

class UP_MapReduce(MRJob): #class that inherits from MRJob 

    def string_to_list(self,s, awards=False):
        if awards:
            s_ = s.strip("[]").split(", ")
            s = []
            for aw in s_:
                s.append(aw[1:-1]) # Chop off the " from front and back, somewhat tideous but works
        else:
            s = s.strip("[]").split("', '")
            # Remove "'" from start of first and end of last item
            s[0] = s[0][1:]
            s[-1] = s[-1][:-1]
        return s

    def get_genre_list(self, bookID):
        return self.string_to_list(df_book[df_book["book_id"]==bookID]["genres"], awards=False)
        
    
    def steps(self):
        #need to define the order of mappers and reducers, so that the functions takes in the right key and value
        #only one mapper and reducer per step, so therefore need several steps
        #do we need the combiner between the mapper and reducer??
        return [ 
            MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
#             MRStep(mapper=self.mapper_2, reducer = self.reducer_2),
#             MRStep(mapper = self.mapper_3, reducer = self.reducer_3),
#             MRStep(mapper = self.mapper_4)
        ]

    
    #key - ignored
    #values - corpus of documents (i.e. the data frame books_metadata)
    def mapper_1(self, _, line):  #each line is a row, i.e. a book 
        if line.find("book_id") == -1:
            list = line.split( ",")
            userID = list[2]
            bookID = list[3]
            rating = list[4]
            genres = self.get_genre_list(bookID)
            for genre in genres:
                yield (userID,genre), rating  # returns key-value pairs ((word, id), 1)

    #key - the key which was yielded by the mapper, i.e. (word, id). (key[0] = word, key[1]=id)
    #values - A generator which yields all values yielded by the mapper which correspond to key, i.e. a list of 1's
    def reducer_1(self, userID_genre, rating): 
        yield (userID_genre[0], userID_genre[1]), average(rating) # returns key-value pairs ((word, id), n)

if __name__ == '__main__':
    UP_MapReduce.run()
