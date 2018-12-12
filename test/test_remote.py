import unittest
import gmql as gl
import shutil
import os
import logging

gl.set_progress(False)
gl.set_remote_address("http://genomic.elet.polimi.it/gmql-rest/")


class GMQLRemoteTester(unittest.TestCase):
    def setUp(self):
        log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        try:
            gl.login()
        except Exception:
            self.skipTest("Impossible to connect to the remote system! Remote testing will not be done!")
        self.rm = gl.get_remote_manager()
        self.remote_output_path = "./remote_out/"
        os.makedirs(self.remote_output_path)

    def tearDown(self):
        shutil.rmtree(self.remote_output_path)

    def gdataframe_equality(self, gd1, gd2):
        sample_mapping = {}

        r1, r2 = gd1.regs, gd2.regs
        m1, m2 = gd1.meta, gd2.meta

        # removing the samples having no regions
        m1 = m1.loc[set(m1.index).difference(set(m1.index).difference(set(r1.index)))]
        m2 = m2.loc[set(m2.index).difference(set(m2.index).difference(set(r2.index)))]

        coords_cols = ['chr', 'start', 'stop'] + (['strand'] if 'strand' in r1.columns else [])
        rest_cols = sorted(set(r1.columns).difference(set(coords_cols)))
        r1, r2 = r1.sort_values(coords_cols + rest_cols), r2.sort_values(coords_cols + rest_cols)

        for i, idx1 in enumerate(r1.index):
            sample_mapping[idx1] = r2.index[i]

        r1.index = r1.index.map(lambda x: sample_mapping[x])
        r1, r2 = r1.sort_index(), r2.sort_index()
        m1.index = m1.index.map(lambda x: sample_mapping.get(x))

        m1, m2 = m1.sort_index(0).sort_index(1), m2.sort_index(0).sort_index(1)

        self.assertTrue((m1.applymap(set) == m2.applymap(set)).all().all())
        self.assertTrue((r1.fillna(0) == r2.fillna(0)).all().all())

    def test_remote_select_1(self):
        querytext = """
        #SELECT 1
            RES = SELECT(region: (chr == chr2 OR chr == chr3) AND NOT(strand == + OR strand == -)
                AND start >= 130 AND stop <= 250) Example_Dataset_2;
            MATERIALIZE RES INTO select_1;
        """

        logging.info("Query: {}".format(querytext))
        logging.info("Executing REMOTE TEXTUAL query")
        respaths = self.rm.query(querytext, self.remote_output_path)
        dataset_name = respaths.iloc[0].dataset
        respaths = os.path.join(self.remote_output_path, dataset_name)
        logging.info("Deleting remote dataset {}".format(dataset_name))
        self.rm.delete_dataset(dataset_name)
        res_query = gl.load_from_path(respaths, all_load=True)

        logging.info("Executing LOCAL PYTHON query")
        d = gl.get_example_dataset("Example_Dataset_2")
        d = d.select(region_predicate=(d.chr.isin(['chr2', 'chr3'])) & (~d.strand.isin(['+', '-'])) &
                                      (d.start >= 130) & (d.stop <= 250))
        res_local = d.materialize()
        self.gdataframe_equality(res_query, res_local)

        logging.info("Executing REMOTE PYTHON query")
        gl.set_mode("remote")
        res_remote = d.materialize()
        self.gdataframe_equality(res_local, res_remote)


if __name__ == '__main__':
    unittest.main()
