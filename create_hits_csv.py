from os import path, listdir, makedirs
from pandas import read_csv, DataFrame, concat
from time import clock
from collections import Counter
from traceback import print_exc
from numpy.random import permutation

ANNOTATIONS_DATA_FOLDER = 'Annotations Data'
ALL_DATA_ANNOTATIONS_PATH = 'All annotations.csv'
TEMP_FOLDER = 'Temporary csv file for analysis'


class HitsCSV(object):
    def __init__(self, csv_path):
        self.__raw_data = read_csv(csv_path)
        self.__csv_headers = list(self.__raw_data)

    def create_new_csv_according_parameters(self, number_tweets_per_hit, variables_list, diseases_filter_list,
                                            talk_about_filter_list, filename, hits_size):
        original_file_header, hit_headers_temp, hits_header, hits_data_list = [], [], [], []
        tweet_idx, tweets_per_filter_size = 0, number_tweets_per_hit * hits_size
        for header_tuple in variables_list:
            # The headers of the original csv
            original_file_header.append(header_tuple[0])
            # The headers of new csv that we will create
            hit_headers_temp.append(header_tuple[1])

        # Check if the header exists in the original csv
        for header in original_file_header:
            if header not in self.__csv_headers:
                print("The '{}' does not exist".format(header))
                exit()

        # Set the new csv header names
        for idx in range(1, number_tweets_per_hit + 1):
            for header in hit_headers_temp:
                hits_header.append(header + '_' + str(idx))

        if hits_size < 1 or type(hits_size) is not int:
            print("The number of hits can not be '{}'".format(hits_size))
            exit()

        sub_csv_data = \
            self.__create_new_csv_after_filtering(diseases_filter_list, talk_about_filter_list, tweets_per_filter_size)

        # Shuffle rows randomly
        sub_csv_data = sub_csv_data.reindex(permutation(sub_csv_data.index))
        sub_csv_data.to_csv(path.join(TEMP_FOLDER, 'Shuffle rows data.csv'))
        sub_csv_data_list = [[]]
        for row in sub_csv_data.itertuples(index=True, name='Pandas'):
            if tweet_idx == number_tweets_per_hit:
                tweet_idx = 0
                hits_size -= 1
                if hits_size == 0:
                    break
                sub_csv_data_list.append([])

            for header in original_file_header:
                sub_csv_data_list[-1].append(getattr(row, header))
            tweet_idx += 1

        DataFrame(sub_csv_data_list, columns=hits_header).to_csv(filename, index=False)

    def __create_new_csv_after_filtering(self, diseases_filter_list, talk_about_filter_list, tweets_per_filter_size):
        sub_csv_data = self.__raw_data
        # Selects the lines in which diseases appear after filtering
        disease_header_filter = diseases_filter_list[0]
        rows_filter_list = list(diseases_filter_list[1])
        sub_csv_data = sub_csv_data[sub_csv_data[disease_header_filter].isin(rows_filter_list)]
        tweets_per_filter_size /= len(rows_filter_list)

        # After running the filter for diseases, only the rows that passed the 'talk about' filter are selected.
        column_header = talk_about_filter_list[0]
        rows_filter_list = list(talk_about_filter_list[1])
        sub_csv_data = sub_csv_data[sub_csv_data[column_header].isin(rows_filter_list)]
        tweets_per_filter_size /= len(rows_filter_list)
        # Rounding a number to an integer, in addition to the addition of 1 because of the headings.
        tweets_per_filter_size = int(tweets_per_filter_size) + 2

        balanced_tweets_csv_data = []
        for disease_row_filter in diseases_filter_list[1]:
            disease_header_filter = diseases_filter_list[0]
            disease_row_filter_list = [disease_row_filter]
            sub_csv_data_temp = sub_csv_data[sub_csv_data[disease_header_filter].isin(disease_row_filter_list)]
            for talk_about_filter in talk_about_filter_list[1]:
                talk_about_header_filter = talk_about_filter_list[0]
                talk_about_filter_row_filter_list = [talk_about_filter]
                balanced_tweets_csv_data.append(sub_csv_data_temp[sub_csv_data_temp[talk_about_header_filter]
                                                .isin(talk_about_filter_row_filter_list)].head(tweets_per_filter_size))

        sub_csv_data = DataFrame(data=concat(balanced_tweets_csv_data))
        sub_csv_data.to_csv(path.join(TEMP_FOLDER, 'sub_csv_temp.csv'))
        return sub_csv_data

    def statistic_analysis_of_the_tweets_data(self, diseases_tuple, column_header_for_disease_analysis):
        variables_names_in_column_header = list(set(self.__raw_data[column_header_for_disease_analysis]))
        analysis_data_output = []
        for disease in diseases_tuple[1]:
            analysis_data_output.append([])
            sub_data = self.__raw_data[self.__raw_data[diseases_tuple[0]].isin([disease])]
            for row_filter in variables_names_in_column_header:
                tweets_size_after_filtering = \
                    sub_data[sub_data[column_header_for_disease_analysis].isin([row_filter])].shape[0]
                analysis_data_output.append([disease, row_filter, '{} / {}'.format(tweets_size_after_filtering,
                                                                                   len(sub_data))])

        DataFrame(data=analysis_data_output,
                  columns=['disease', column_header_for_disease_analysis, 'amount of tweets'])\
            .to_csv(path.join(TEMP_FOLDER, 'statistic_analysis.csv'), index=False)


def create_one_uniting_file_with_all_annotation(all_annotations_des_file_path, merge_files_path_list):
    if len(merge_files_path_list) == 0:
        print("Error: There are no files that need to be consolidated.")
        exit()
    csv_obj_list = [read_csv(csv_path) for csv_path in merge_files_path_list]
    original_header_size = len(csv_obj_list)
    csv_headers_list = []
    for headers_per_csv in csv_obj_list:
        for header in headers_per_csv:
            csv_headers_list.append(header)
    for header_count in Counter(csv_headers_list).values():
        if header_count != original_header_size:
            print("Warning: Please note that some of the files have more or fewer columns!\n"
                  "Or that the header names are not the same as the rest of the files!")
            break
    DataFrame(concat(csv_obj_list), columns=set(csv_headers_list)).to_csv(all_annotations_des_file_path, index=False)


def main():
    time_start = clock()
    merge_files_path_list = [path.join(ANNOTATIONS_DATA_FOLDER, filename) for filename in
                             listdir(ANNOTATIONS_DATA_FOLDER)]
    try:
        if not path.isdir(TEMP_FOLDER):
            makedirs(TEMP_FOLDER)
        create_one_uniting_file_with_all_annotation(ALL_DATA_ANNOTATIONS_PATH, merge_files_path_list)

        if not path.isfile(ALL_DATA_ANNOTATIONS_PATH):
            print("Error: The '{}' does not exist!".format(ALL_DATA_ANNOTATIONS_PATH))
            exit()

        hits_obj = HitsCSV(ALL_DATA_ANNOTATIONS_PATH)
        hits_obj.statistic_analysis_of_the_tweets_data(('disease', ('Asthma', 'HIV', 'Fibromyalgia')), 'talk_about')
        variables_list = [('tweet_id', 'tweet_id'), ('user_id', 'user_id'), ('text', 'tweet_text')]
        # Asthma    'talk_about'
        diseases_filter_list = ('disease', ('HIV', 'Fibromyalgia', 'Asthma'))
        talk_about_filter_list = ('talk_about', ('celeb', 'himself', 'none'))
        hits_obj.create_new_csv_according_parameters(5, variables_list, diseases_filter_list, talk_about_filter_list,
                                                     'Hits_data.csv', 18)
    except Exception as _:
        print_exc()
    print("\nThe total time taken for the run program is {:.2f}".format(clock() - time_start))

if __name__ == "__main__":
    main()
