from mrjob.job import MRJob
from mrjob.step import MRStep
from shapely.geometry import shape
import json
import sys
import re
from random import random
import time

sys.path.append('.')
WORD_RE = re.compile(r"[\w']+")
_LIM_TRENDS = 10


class MRTweetSentiment(MRJob):
    """
    Class with mapper and reducer functions. It extends MRJob, and provides
    functions to perform sentiment analysis using tweets and MapReduce based
    algorithms.
    """

    def configure_args(self):
        """
        Function that enables to pass an extra param through the command line.
        These values can be:
            - sentiments: the execution will return a list of regions with its
            computed sentiment score.
            - most-happy: the execution will return the region with the higher
            sentiment score.
            - trending: the execution will return the 10 most popular trending
            topics.
        """
        super(MRTweetSentiment, self).configure_args()
        self.add_passthru_arg(
            '--job-options', default='sentiments', choices=['sentiments',
                                                            'most-happy',
                                                            'trending'],
            help="Specify the output of the job")

    def steps(self):
        """
        Defines execution steps based on --job-options parameter.
        :return: list of MRStep.
        """
        if self.options.job_options == 'sentiments':
            return [
                MRStep(mapper=self.mapper_region_sentiments,
                       combiner=self.combiner_sum_values,
                       reducer=self.reducer_region_sentiments)]

        elif self.options.job_options == 'most-happy':
            return [
                MRStep(mapper=self.mapper_region_sentiments,
                       combiner=self.combiner_sum_values,
                       reducer=self.reducer_sentiments_region),
                MRStep(reducer=self.reducer_max_sentiment)]

        elif self.options.job_options == 'trending':
            return [
                MRStep(mapper=self.mapper_trending,
                       combiner=self.combiner_sum_values,
                       reducer=self.reducer_trending),
                MRStep(reducer=self.reducer_order)]

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

    @staticmethod
    def box_to_province(bounding_box):
        """
        This function receives a dict including coordinates that resemble a
        Polygon, and checks whether that Polygon is included in any bigger
        polygon (region).
        :param bounding_box: dict that includes coordinates of a tweet.
        :return: str representing the region where bounding_box is included.
        """
        with open("comunidades.json") as regions:
            shapes_regions = json.load(regions)
            for region, coordinates in shapes_regions.items():
                region_shape = shape(coordinates)
                if region_shape.contains(shape(bounding_box)):
                    return region

    def mapper_region_sentiments(self, _, line):
        """
        Map function. The value is the calculated sentiment
        of the tweet and the key is the designated region.
        :param line: json corresponding to each tweet.
        :return: (key, value) tuple.
        """
        # Saving json as a dictionary
        tweet = json.loads(line)
        place = tweet.get('place')

        # Place may not be informed
        if place is not None:
            country = place.get('country_code')
            bounding_box = place.get('bounding_box')
            tweet_text = tweet.get('text')
            if (country == 'ES') & (tweet_text is not None):
                # region = place.get('name')
                region = self.box_to_province(bounding_box)
                if region:
                    sentiment = self.compute_sentiment(tweet_text)
                    yield (region, sentiment)

    @staticmethod
    def mapper_trending(_, line):
        """
        Map function. The key is a trending topic word and the
        value is 1.
        :param line: json corresponding to each tweet
        :return: (key, value) tuple.
        """
        # Saving json as a dictionary
        tweet = json.loads(line)
        place = tweet.get('place')
        # Place may not be informed
        if place is not None:
            country = place.get('country_code')
            place_type = place.get('place_type')
            tweet_text = tweet.get('text')
            lang = tweet.get('lang')
            if (country == 'ES') & (tweet_text is not None) & \
                    (lang == 'es'):
                for word in tweet_text.split():
                    if '#' in word:
                        yield (word.split('#')[1], 1)

    @staticmethod
    def combiner_sum_values(key, values):
        """
        Combiner function.
        :param key: str
        :param values: int
        :return: (key, value) tuple. Value is the sum of values.
        """
        yield (key, sum(values))

    @staticmethod
    def reducer_region_sentiments(region, sentiments):
        """
        Reduce function. It produces a tuple with a region and the sum of the
        corresponding sentiments.
        :param region: str
        :param sentiments: int
        :return: (key, value) tuple.
        """
        yield (region, sum(sentiments))

    @staticmethod
    def reducer_sentiments_region(region, sentiments):
        """
        Reduce function. For a same key (None) it produces a tuple with the
        sum of sentiments for a region, and the region.
        :param region: str
        :param sentiments: int
        :return: None, (key, value) tuple.
        """
        yield None, (sum(sentiments), region)

    @staticmethod
    def reducer_max_sentiment(_, sentiment_region):
        """
        Reduce function. It produces a tuple with the region with the highest
        sentiment score, and that same score.
        :param _:
        :param sentiment_region: (key, value) tuple.
        :return: (key, value) tuple.
        """
        yield max(sentiment_region)

    @staticmethod
    def reducer_trending(word, counts):
        """
        Reduce function. For a same key (None) it produces a tuple with the sum
        of occurrences of a word, and the word.
        :param word: str
        :param counts: int
        :return: None, (key, value) tuple.
        """
        yield None, (sum(counts), word)

    @staticmethod
    def reducer_order(_, count_words):
        """
        Reduce function. It collects every word with its occurrences, and
        returns an ordered list of 10 words.
        :param _:
        :param count_words: (key, value) tuple
        :return: (key, value) tuple
        """
        words = 0
        for count, trend in sorted(count_words, reverse=True):
            if words < _LIM_TRENDS:
                words += 1
                yield (trend, count)
            else:
                break


if __name__ == '__main__':
    MRTweetSentiment.run()
