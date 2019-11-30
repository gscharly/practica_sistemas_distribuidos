from mrjob.job import MRJob
import json
import sys
import re

WORD_RE = re.compile(r"[\w']+")
sys.path.append('.')


class MRTweetSentiment(MRJob):
    """
    Class with mapper and reducer functions. It extends MRJob, and provides
    functions to perform sentiment analysis using tweets and MapReduce based
    algorithms.
    """

    @staticmethod
    def sentiment_dictionary():
        """
        Reads a file given through --file option and returns a dictionary
        with word-score mapping for a given language.
        :return: dictionary of words with their associated sentiment score.
        """
        # TODO: find a way to access --file without specifying file name.
        score_file = open("Redondo_words.txt")
        scores = {}  # initialize an empty dictionary
        for line in score_file:
            term, score = line.split("\t")  # The file is tab-delimited.
            scores[term] = float(score)
        return scores

    def compute_sentiment(self, tweet_text):
        """
        Given a text, this function maps each word to a score dictionary in
        order to compute the associated sentiment.
        :param tweet_text: string with the tweet text.
        :return: int accumulated score in the text.
        """
        sentiment = 0
        # Reading the language file
        sent_dict = self.sentiment_dictionary()
        for word in WORD_RE.findall(tweet_text):
            sentiment_value = sent_dict.get(word.lower())
            # A word may not be included in the language dictionary
            if sentiment_value is not None:
                sentiment += sentiment_value
        return sentiment

    def mapper(self, _, line):
        """
        Map function.
        :param _:
        :param line: json corresponding to each tweet
        :return: (key, value) tuple. The value is the calculated sentiment
        of the tweet and the key is the designated region.
        """
        # Saving json as a dictionary
        tweet = json.loads(line)
        place = tweet.get('place')
        # Place may not be informed
        if place is not None:
            country = place.get('country_code')
            place_type = place.get('place_type')
            tweet_text = tweet.get('text')
            if (country == 'ES') & (tweet_text is not None) &\
                    (place_type == 'city'):
                region = place.get('name')
                sentiment = self.compute_sentiment(tweet_text)
                yield (region, sentiment)
        else:
            yield (None, 1)

    def reducer(self, region, sentiments):
        """
        Reduce function.
        :param region: str
        :param sentiments: int
        :return: (key, value) tuple. The key is the region and the value is the
        sum of the sentiments of all of the tweets that correspond to that
        region.
        """
        yield (region, sum(sentiments))


if __name__ == '__main__':
    MRTweetSentiment.run()
