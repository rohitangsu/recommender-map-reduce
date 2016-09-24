from mrjob.job import MRJob
from metrics import  correlation
from metrics import cosine, regularized_correlation
from math import sqrt

try:
    from itertools import combinations
except ImportError:
    from metrics import combinations


PRIOR_COUNT = 10
PRIOR_CORRELATION = 0


class SemicolonValueProtocol(object):

    # don't need to implement read() since we aren't using it

    def write(self, key, values):
        return ';'.join(str(v) for v in values)



class ItemSimilarity(MRJob):

    def steps(self):
        """
           Mapper1: Group by user rating on the product..
           Reducer1: Count rating and output frequency..
        """
        return [
            self.mr(mapper=self.group_by_user_rating,
                    reducer=self.count_ratings_users_freq),
            self.mr(mapper=self.pairwise_items,
                    reducer=self.calculate_similarity),
            self.mr(mapper=self.calculate_ranking,
                    reducer=self.top_similar_items)]

    def group_by_user_rating(self, line):
        user, prod = line.split('|')
        yield user, (prod, 1)
    
    def count_ratings_users_freq(self, user_id, values):
        item_count = 0
        item_sum = 0
        final = []
        for item_id, rating in values:
            item_count += 1
            item_sum += rating
            final.append((item_id, rating))

        yield user_id, (item_count, item_sum, final)


    def pairwise_items(self, user_id, values):
        item_count, item_sum, ratings = values
        #print item_count, item_sum, [r for r in combinations(ratings, 2)]
        #bottleneck at combinations
        for item1, item2 in combinations(ratings, 2):
            yield (item1[0], item2[0]), \
                    (item1[1], item2[1])

    def calculate_similarity(self, pair_key, lines):
        sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        item_pair, co_ratings = pair_key, lines
        item_xname, item_yname = item_pair
        for item_x, item_y in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            sum_x += item_x
            n += 1

        corr_sim = correlation(n, sum_xy, sum_x, \
                 sum_y, sum_xx, sum_yy)

        reg_corr_sim = regularized_correlation(n, sum_xy, sum_x, \
                sum_y, sum_xx, sum_yy, PRIOR_COUNT, PRIOR_CORRELATION)

        cos_sim = cosine(sum_xy, sqrt(sum_xx), sqrt(sum_yy))

        jaccard_sim = 0.0

        yield (item_xname, item_yname), (corr_sim, \
            cos_sim, reg_corr_sim, jaccard_sim, n)


    def calculate_ranking(self, item_keys, values):
        '''
        Emit items with similarity in key for ranking:
        19,0.4    70,1
        19,0.6    21,2
        21,0.6    19,2
        21,0.9    70,1
        70,0.4    19,1
        70,0.9    21,1
        '''
        corr_sim, cos_sim, reg_corr_sim, jaccard_sim, n = values
        item_x, item_y = item_keys
        if int(n) > 0:
            yield (item_x, corr_sim, cos_sim, reg_corr_sim, jaccard_sim), \
                     (item_y, n)

    def top_similar_items(self, key_sim, similar_ns):
        '''
        For each item emit K closest items in comma separated file:
        De La Soul;A Tribe Called Quest;0.6;1
        De La Soul;2Pac;0.4;2
        '''
        item_x, corr_sim, cos_sim, reg_corr_sim, jaccard_sim = key_sim
        for item_y, n in similar_ns:
            yield None, (item_x, item_y, corr_sim, cos_sim, reg_corr_sim,
                jaccard_sim, n)


if __name__ == '__main__':
    ItemSimilarity.run()