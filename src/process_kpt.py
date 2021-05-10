from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

spark = SparkSession.builder \
    .master("local") \
    .appName("KommatiParaTrader") \
    .config("spark.some.config.option", "some-value").getOrCreate()

logger = logging.getLogger("driver_logger")
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s | %(filename)s | %(funcName)s | %(lineno)d | %(message)s')


class ProcessKPT:
    """
    Class ProcessKPT
    """

    def __init__(self, data1_path, data2_path, country_filter):
        """
        This method is treated as the constructor of the class ProcessKPT
        :param data1_path: path for the client data
        :param data2_path: path for the financial data
        :param country_filter: filter condition for country
        """
        self.data1_path = data1_path
        self.data2_path = data2_path
        self.country_filter = country_filter
        self.logger = logger
        self.input_process()

    @staticmethod
    def filter_process(client_info_df, column, filter_value):
        """
        This is a generic method used for performing filter on the given dataframe
        :param client_info_df:  given dataframe
        :param column: column on which filter needs to be applied
        :param filter_value: filter condition

        :return: filtered datframe
        """
        filtered_df = client_info_df \
            .filter(col(column).isin(filter_value))
        logger.info("Required filter is applied to the input dataframe")
        return filtered_df

    @staticmethod
    def rename_col(filtered_df, prev_col, new_col):
        """
        This is a generic method used to rename column for the given dataframe
        :param filtered_df: given dataframe
        :param prev_col: existing column name
        :param new_col: new name of the column needs to be renamed as
        :return: updated dataframe with renamed column
        """
        renamed_df = filtered_df.withColumnRenamed(prev_col, new_col)
        logger.info("Columns have been renamed to meaningful names for client's better understanding")
        return renamed_df

    def input_process(self):
        """
        This is the method where the application gets the input to read client and financial data
        and filters the client data based on the country.
        :return: client dataframe and financial dataframe
        """
        try:
            self.logger.info("Process received required input")
            client_info_df = spark.read.csv(self.data1_path, header=True)
            financial_info_df = spark.read.csv(self.data2_path, header=True)
            country_specific_client_info = ProcessKPT.filter_process(client_info_df, 'country', self.country_filter)
        except Exception as e:
            raise e

        return country_specific_client_info, financial_info_df

    @classmethod
    def get_required_data(cls, country_specific_client_info, financial_info_df):
        """
        This method is used to perform join operation on the given condition then renaming of columns
        and provide output.
        renaming is done
        :param country_specific_client_info: client dataframe
        :param financial_info_df: financial dataframe
        :return: updated dataframe
        """
        try:
            res_df = country_specific_client_info \
                .join(financial_info_df,
                      country_specific_client_info['id'] == financial_info_df['id'],
                      'left_outer')\
                .drop(financial_info_df['id'])

            id_rename_df = ProcessKPT.rename_col(res_df, 'id', 'client_identifier')
            btc_a_rename_df = ProcessKPT.rename_col(id_rename_df, 'btc_a', 'bitcoin_address')
            final_rename_df = ProcessKPT.rename_col(btc_a_rename_df, 'cc_t', 'credit_card_type')
            client_req_details_df = final_rename_df.\
                select('client_identifier', 'email', 'bitcoin_address', 'credit_card_type')

        except Exception as e:
            raise e

        return client_req_details_df


if __name__ == "__main__":
    """
    Main method of the process.
    """

    logger.info("**********Process Started**********")

    dataset1_path = "../input_data/dataset_one.csv"
    dataset2_path = "../input_data/dataset_two.csv"
    filter_cols = ['United Kingdom', 'Netherlands']

    kpt_obj = ProcessKPT(dataset1_path, dataset2_path, filter_cols)
    logger.info("ProcessKPT class is initialized")

    client_df, financial_df = kpt_obj.input_process()
    final_df = ProcessKPT.get_required_data(client_df, financial_df)

    """The below code for writing dataframe to csv is not working in my local,
    due to which converted to pandas in below lines"""
    # final_df.write.csv('../client_data/output.csv')
    # final_df.write.format('com.databricks.spark.csv').save('../client_data/output.csv')

    final_df.toPandas().to_csv('../client_data/output.csv')
    logger.info("Final output is exported to a csv in 'client_data' directory")

    logger.info("**********Process Completed Successfully**********")
