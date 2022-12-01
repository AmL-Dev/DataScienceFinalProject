# Book recommendation system

This project features a book recommendation system based using the content based filtering method and MapReduce.

<p align="left">
  <img src="https://user-images.githubusercontent.com/61467804/205084333-4b131699-3063-4858-bf31-5cc43e87bf85.png" alt="List of recommendations." width=600 />
</p>

## Description

The goal of this project is to provide book recommendations to specific users. For this we have a dataset containing book ratings of every user. The main characteristics of each book is also provided. To tailor personnalized recommendations we used the content based filtering method.

In summary, the program works with following 4 steps:

1. Convert book descriptions to a set of the top n TFIDF-scoring words.
2. Construct item profiles for all books.
3. Construct user profiles for a set of users, based on read books and ratings.
4. Recommend a set of books to a subset of users using cosine similarity.

## Built With

This project was build using the following technologies:

- [Python](https://www.python.org/)
- [Pandas](https://pandas.pydata.org/)
- [scikit-learn](https://scikit-learn.org/stable/)
- [NumPy](https://numpy.org/)

## Data

We used the [Goodreads Datasets](https://sites.google.com/eng.ucsd.edu/ucsdbookgraph/home) from UCSD. Bellow are the links to the dataset to run the project. Save the dtaset to a directory called **data**.

#### **Mandatory**

To prevent having to run the data preprocessing step, we have exported our preprocessed data set:

- List of books with all of their characteristics:[books_metadata.csv](https://drive.google.com/file/d/18bopeQvWYmAUH5tga0X7dnM-HTneqVrU/view?usp=sharing)

- List of all user ratings for the book they read [user_book_ratings.csv](https://drive.google.com/file/d/1813UCk3913FR0TgGh3yJqXtOawaW54Gv/view?usp=sharing)

#### **Optional**

To run the preprocessing steps, it will require downloading the original dataset.
Bellow the links to access the original dataset:

- List of books with their title and other partial metadata: [books_titles.json](https://drive.google.com/file/d/1Iqv9TROqNgYbUDijSaDegv4EPpxO97t3/view?usp=sharing)
- Every rating of every user for different books: [goodreads_users_book.csv](https://drive.google.com/open?id=1zmylV7XW2dfQVCLeg1LbllfQtHD2KUon)
- Mapping between book ids in the json and csv files: [book_id_map.csv](https://drive.google.com/uc?id=1CHTAaNwyzvbi1TR08MJrJ03BxA266Yxr)
- Books full metadata (this data does not commes Goodreads, it needs to be mapped to the other dataset to be used): [Best_Books_Ever.csv](https://zenodo.org/record/4265096?fbclid=IwAR3GKs_9DD4UMYx7i3D9HWZl91fjdC5UQkwtX8K-b1Cos5DBVEGI-pyPyLI)

## Run the project

To run the project, follow the steps bellow after downloading the required dataset and saving it to a directory called **data**.

0. (Optinal) If you have downloaded the optional dataset, then you can run the _DataPreprocessing.ipynb_ file which preprocesses the dataset (make sure the dataset is saved to a directory called data).
1. - Before running this step, ensure that you have a directory nammed _data_ containing _books_metadata.csv_ _user_book_ratings.csv_ (either [download](#mandatory) them directly or by running step 0).

   * Run the _tfidf.ipynb_ file to compute extract the most significant words in books description.

2. - Before running this step, ensure that you have a _tfidf_scores_ file in the current directory.

   * Run the _ItemAndUserProfiles.ipynb_ file to create a store item and user profiles.

3. - Before running this step, ensure that you have an _item_profiles.txt_ and _user_profiles.txt_ file in the current directory.
   * (Optional) Run the first cell of _Recommendation.ipynb_ to get a random user
   * (Optional) In the second cell, set the id of the user you want to get recommendation for: _recommendation_user_id_ variable
   * Run all cells of _Recommendation.ipynb_ to get recommendations for a specific user.

## Sources

- Mining of Massive Datasets. Book by Jure Leskovec, Anand Rajaraman and Jeffrey D. Ullman, 2019
- Content-based recommendation algorithms on the Hadoop MapReduce framework. Paper by T. D. Pessemier, K. Vanhecke, S. Dooms, and L. Martens
