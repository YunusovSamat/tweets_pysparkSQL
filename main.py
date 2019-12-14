from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


class TweetsSpark:
    def __init__(self):
        self.tweets = None
        self.tweets_table = None
        sc = SparkContext.getOrCreate()
        self.spark = SparkSession(sc)

    # Метод для записи в переменную, выборочные данные из файла.
    def set_tweets_data(self, path):
        # Чтение данных и разбиение на колонки.
        self.tweets = self.spark.read.csv(path, header=True, escape='\"')
        self.tweets.createOrReplaceTempView("tweets")

    def set_tweets_foreign(self):
        sql = """\
        CREATE TEMP VIEW tweets_foreign AS
          SELECT userid, 
                 CAST(reply_count AS INT), 
                 account_language
            FROM tweets
            WHERE account_language != 'ru'
        """
        self.spark.sql(sql)

    def get_userid_max_rc(self):
        sql = """\
        SELECT userid FROM tweets_foreign
          WHERE reply_count = (
            SELECT MAX(reply_count)
              FROM tweets_foreign
            )
          LIMIT 1
        """
        # Result userid 4224729994
        return self.spark.sql(sql).show()

    def get_sorted_data(self):
        sql = """\
         SELECT * FROM tweeets_foreign
           ORDER BY reply_count DESC
         """
        return self.spark.sql(sql).show()
    
    
if __name__ == '__main__':
    file_path = 'file:///home/samat/Downloads/ira_tweets_csv_hashed.csv'
    ts = TweetsSpark()
    ts.set_tweets_data(file_path)
    ts.set_tweets_foreign()
    print(ts.get_userid_max_rc())
