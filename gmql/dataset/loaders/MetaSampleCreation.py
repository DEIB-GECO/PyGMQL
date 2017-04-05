import logging
from .Materializations import meta_rdd_to_pandas


logger = logging.getLogger('gmql_logger')


def create_sample_meta(meta_dataset):
    logger.info("collecting metadata")
    # collecting everything
    meta = meta_dataset.collect()
    # dataframe construction
    pandas_df = meta_rdd_to_pandas(meta)
    # sampling
    # TODO: creation of sample
    sample_df = pandas_df.sample(20)

    raise NotImplementedError("To be implemented")


