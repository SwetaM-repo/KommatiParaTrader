from chispa import assert_df_equality

from pyspark.sql import SparkSession
from src.main.process_kpt import ProcessKPT

spark = SparkSession.builder \
    .master("local") \
    .appName("TEST_PROCESS_KPT") \
    .config("spark.some.config.option", "some-value").getOrCreate()

test_df1 = spark.read.csv("./test_data/filter_ip.csv", header=True)
test_df2 = spark.read.csv("./test_data/input_2.csv", header=True)
expected_df = spark.read.csv("./test_data/filter_op.csv", header=True)


def test_filter_process():
    """
    This test method will test if the filter condition is working properly on the dataframe or not

    :return: assertion True/fail
    """
    res_df = ProcessKPT.filter_process(test_df1, 'country', ['United Kingdom', 'Netherlands'])
    exp_df = expected_df.drop('client_identifier')
    assert_df_equality(res_df, exp_df)


def test_rename_process():
    """
    This test method will test if the rename process is working properly on the dataframe or not

    :return: assertion True/fail
    """
    res_df = ProcessKPT.rename_col(expected_df.drop('client_identifier'), 'id', 'client_identifier')
    exp_df = expected_df.select('client_identifier', 'first_name', 'last_name', 'email', 'country')
    assert_df_equality(res_df, exp_df)


def test_input_process():
    """
    This test method will test if it takes input and performs filter properly on the dataframe or not

    :return: assertion True/fail
    """
    ds1 = "./test_data/filter_ip.csv"
    ds2 = "./test_data/filter_op.csv"
    obj_kpt = ProcessKPT(ds1, ds2, ['United Kingdom', 'Netherlands'])
    res_df = ProcessKPT.filter_process(test_df1, 'country', ['United Kingdom', 'Netherlands'])
    exp_df = expected_df.drop('client_identifier')
    assert_df_equality(res_df, exp_df)


def test_get_required_data():
    """
    This test method will test if it performs join operation on the given condition properly on
    the dataframe or not

    :return: assertion True/fail
    """

    res_df = ProcessKPT\
        .get_required_data(test_df1, test_df2.drop('bitcoin_address', 'credit_card_type', 'email', 'client_identifier'))
    exp_df = test_df2.select('client_identifier', 'email', 'bitcoin_address', 'credit_card_type')
    assert_df_equality(res_df, exp_df)
