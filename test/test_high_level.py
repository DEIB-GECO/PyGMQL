import unittest
import gmql as gl
from gmql.dataset.GDataframe import GDataframe
from gmql.dataset.GMQLDataset import GMQLDataset
from pkg_resources import resource_filename
import os
from glob import glob
import shutil

gl.set_progress(False)


class GMQLQueryTester(unittest.TestCase):

    def setUp(self):
        self.query_results_path = resource_filename("gmql", os.path.join("resources", "example_queries_results"))

    def get_query_results(self, queryname):
        datapath = glob(os.path.join(self.query_results_path, "*{}".format(queryname)))[0]
        return gl.load_from_path(datapath, all_load=True)

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

    def test_load_example_datasets(self):
        self.assertIsInstance(gl.get_example_dataset(name="Example_Dataset_1"), GMQLDataset)
        self.assertIsInstance(gl.get_example_dataset(name="Example_Dataset_2"), GMQLDataset)

        self.assertIsInstance(gl.get_example_dataset(name="Example_Dataset_1", load=True), GDataframe)
        self.assertIsInstance(gl.get_example_dataset(name="Example_Dataset_2", load=True), GDataframe)

    def test_select_1(self):
        """ #SELECT 1
            RES = SELECT(region: (chr == chr2 OR chr == chr3) AND NOT(strand == + OR strand == -)
                AND start >= 130 AND stop <= 250) Example_Dataset_2;
            MATERIALIZE RES INTO select_1;
        """

        res = gl.get_example_dataset("Example_Dataset_2")
        res = res.select(region_predicate=(res.chr.isin(['chr2', 'chr3'])) & (~res.strand.isin(['+', '-'])) &
                                          (res.start >= 130) & (res.stop <= 250))
        res = res.materialize()
        self.gdataframe_equality(res, self.get_query_results("select_1"))

    def test_project_1(self):
        """ # project_1
            D = SELECT() Example_Dataset_1;
            RES = PROJECT(region_update: length AS right - left) D;
            MATERIALIZE RES INTO project_1;
        """

        res = gl.get_example_dataset("Example_Dataset_1")
        res = res.project(new_field_dict={'length': res.right - res.left})
        res = res.materialize()
        self.gdataframe_equality(res, self.get_query_results("project_1"))

    def test_extend_2(self):
        """ # extend_2
            D = SELECT() Example_Dataset_1;
            RES = EXTEND(region_count AS COUNT(), min_pvalue AS MIN(pvalue)) D;
            MATERIALIZE RES INTO extend_2;
        """

        res = gl.get_example_dataset("Example_Dataset_1")
        res = res.extend({'region_count': gl.COUNT(), 'min_pvalue': gl.MIN("pvalue")})
        res = res.materialize()
        self.gdataframe_equality(res, self.get_query_results("extend_2"))

    def test_order_1(self):
        """ # order_1
            D = SELECT(region: chr == chr1) Example_Dataset_1;
            D1 = EXTEND(Region_count AS COUNT()) D;
            RES = ORDER(Region_count DESC; meta_top: 2) D1;
            MATERIALIZE RES INTO order_1;
        """

        res = gl.get_example_dataset("Example_Dataset_1")
        res = res.reg_select(res.chr == 'chr1')
        res = res.extend({'Region_count': gl.COUNT()})
        res = res.order(meta=['Region_count'], meta_ascending=[False], meta_top="top", meta_k=2)
        res = res.materialize()
        self.gdataframe_equality(res, self.get_query_results("order_1"))

    def test_group_1(self):
        """ # group_1
            D = SELECT(region: chr == chr2) Example_Dataset_2;
            RES = GROUP(controlId; meta_aggregates: max_cell_tier AS MAX(cell_tier)) D;
            MATERIALIZE RES INTO group_1;
        """

        res = gl.get_example_dataset("Example_Dataset_2")
        res = res.reg_select(res.chr == 'chr2')
        res = res.group(meta=['controlId'], meta_aggregates={'max_cell_tier': gl.MAX("cell_tier")})
        res = res.materialize()
        self.gdataframe_equality(res, self.get_query_results("group_1"))

    def test_merge_1(self):
        """ # merge_1
            D = SELECT(region: chr == chr1) Example_Dataset_1;
            RES = MERGE() D;
            MATERIALIZE RES INTO merge_1;
        """

        res = gl.get_example_dataset("Example_Dataset_1")
        res = res.reg_select(res.chr == 'chr1')
        res = res.merge()
        res = res.materialize()
        self.gdataframe_equality(res, self.get_query_results("merge_1"))

    def test_difference_1(self):
        """ # difference_1
            D1 = SELECT(region: chr == chr2) Example_Dataset_1;
            D2 = SELECT(cell_karyotype == "cancer"; region: chr == chr2) Example_Dataset_2;
            RES = DIFFERENCE() D1 D2;
            MATERIALIZE RES INTO difference_1;
        """

        d1 = gl.get_example_dataset("Example_Dataset_1")
        d1 = d1.reg_select(d1.chr == 'chr2')

        d2 = gl.get_example_dataset("Example_Dataset_2")
        d2 = d2.select(meta_predicate=d2['cell_karyotype'] == 'cancer', region_predicate=d2.chr == 'chr2')

        res = d1.difference(d2)
        res = res.materialize()
        self.gdataframe_equality(res, self.get_query_results("difference_1"))

    def test_map_1(self):
        """ # map_1
            D1 = SELECT(region: chr == chr2) Example_Dataset_1;
            D2 = SELECT(region: chr == chr2) Example_Dataset_2;
            RES = MAP(avg_score AS AVG(score)) D1 D2;
            MATERIALIZE RES INTO map_1;
        """

        d1 = gl.get_example_dataset("Example_Dataset_1")
        d1 = d1.reg_select(d1.chr == 'chr2')

        d2 = gl.get_example_dataset("Example_Dataset_2")
        d2 = d2.reg_select(d2.chr == 'chr2')

        res = d1.map(d2, new_reg_fields={'avg_score': gl.AVG("score")}, refName="D1", expName="D2")
        res = res.materialize("./tmp")
        shutil.rmtree("./tmp")
        self.gdataframe_equality(res, self.get_query_results("map_1"))

    def test_join_1(self):
        """ # join_1
            D1 = SELECT(region: chr == chr2) Example_Dataset_1;
            D2 = SELECT(region: chr == chr2) Example_Dataset_2;
            RES = JOIN(MD(1), DGE(20); output: RIGHT; joinby: cell_karyotype) D1 D2;
            MATERIALIZE RES INTO join_1;
        """

        d1 = gl.get_example_dataset("Example_Dataset_1")
        d1 = d1.reg_select(d1.chr == 'chr2')

        d2 = gl.get_example_dataset("Example_Dataset_2")
        d2 = d2.reg_select(d2.chr == 'chr2')

        res = d1.join(d2, [gl.MD(1), gl.DGE(20)], output="RIGHT", joinBy=['cell_karyotype'], refName="D1", expName="D2")
        res = res.materialize("./tmp")
        shutil.rmtree("./tmp")
        self.gdataframe_equality(res, self.get_query_results("join_1"))

    def test_cover_2(self):
        """ # cover_2
            D = SELECT(region: chr == chr2) Example_Dataset_2;
            RES = COVER(2, 3; groupby: cell; aggregate: min_pvalue AS MIN(pvalue)) D;
            MATERIALIZE RES INTO cover_2;
        """

        d = gl.get_example_dataset("Example_Dataset_2")
        d = d.reg_select(d.chr == 'chr2')
        d = d.normal_cover(2, 3, groupBy=['cell'], new_reg_fields={'min_pvalue': gl.MIN("pvalue")})
        d = d.materialize()
        self.gdataframe_equality(d, self.get_query_results("cover_2"))


    # @staticmethod
    # def test_complex_query():
    #     d = gl.load_from_path("../../data/narrow_sample")
    #     d = d.reg_select(d.pvalue >= 0.05)
    #     d = d[d['experiment_target'] == 'ARID3A']
    #     d = d.reg_project(new_field_dict={
    #         'newField': (d.start + d.pvalue) / 2
    #     })
    #     d = d.join(d, genometric_predicate=[gl.DLE(100000)])
    #     d = d.map(d)
    #     d = d.cover(minAcc=1, maxAcc="ANY")
    #     d = d.materialize()


if __name__ == '__main__':
    unittest.main()
