from modules.Reco_Engine.recsys import *
from modules.Reco_Engine.generic_preprocessing import *

import pymysql
import pymysql.cursors
import pandas as pd
import numpy as np
import json


class Recommendation:
    def __init__(self):
        self.__HOST = 'dev-agnii-rds.cfcjtuuefhbn.ap-south-1.rds.amazonaws.com'
        self.__USER = 'priyanka'
        self.__PASSWORD = '7G2YMUa9fE'
        self.__DATABASE = 'dev_agnii_02may19'
        self.__CHARSET = 'utf8mb4'
        self.__data = None

    def __execute_query(self, query, parameter):
        conn = pymysql.connect(host=self.__HOST, user=self.__USER, password=self.__PASSWORD,
                               database=self.__DATABASE, charset=self.__CHARSET)
        if parameter == 'SELECT':
            data = pd.read_sql(query, conn)
            return data

        elif parameter == 'UPDATE':
            with conn.cursor() as cur:
                cur.execute(query)

            conn.commit()
            return "Done"

    def __get_data(self):
        all_query = """
        SELECT id, user_id, innovation_id, count
        FROM innovations_most_viewed
        """
        self.__data = self.__execute_query(all_query, parameter='SELECT')
        self.__data['user_id'] = self.__data['user_id'].astype(str)

    def __preprocess_data(self, parameter):
        self.__get_data()

        interactions = create_interaction_matrix(df=self.__data,
                                                 user_col='user_id', item_col='innovation_id', rating_col='count')
        if parameter == 'interactions':
            return interactions

        if parameter == 'users':
            user_dict = create_user_dict(interactions=interactions)
            return user_dict

        if parameter == 'innovations':
            innovation_dict = create_item_dict(df=self.__data,
                                               id_col='innovation_id', name_col='innovation_id')

            return innovation_dict

    def __create_model(self):
        model = runMF(interactions=self.__preprocess_data('interactions'), n_components=30,
                      loss='warp', epoch=30, n_jobs=5)

        return model

    def __top_most_viewed(self):
        top_most = """
        SELECT innovation_id
        FROM innovations_most_viewed
        ORDER BY count DESC
        LIMIT 10
        """
        top = self.__execute_query(top_most, 'SELECT')

        query = """
        UPDATE user_recommendation
        SET innovation_id = "{}"
        WHERE user_id = 0
        """.format(list(top['innovation_id']))

        self.__execute_query(query, 'UPDATE')

    def get_recommendation(self):
        self.__top_most_viewed()

        update_query = """
        INSERT INTO user_recommendation(user_id, innovation_id)
        VALUES({}, "{}")
        ON DUPLICATE KEY UPDATE innovation_id = VALUES(innovation_id)
        """
        model = self.__create_model()
        for user in self.__data['user_id'].unique():
            a = sample_recommendation_user(model=model,
                                           interactions=self.__preprocess_data('interactions'),
                                           user_id=str(user),
                                           user_dict=self.__preprocess_data('users'),
                                           item_dict=self.__preprocess_data('innovations'),
                                           threshold=4, nrec_items=10)
            self.__execute_query(update_query.format(user, a), parameter='UPDATE')


if __name__ == '__main__':
    reco_engine = Recommendation()
    reco_engine.get_recommendation()