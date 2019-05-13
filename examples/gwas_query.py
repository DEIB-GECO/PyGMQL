import gmql as gl
import os
import time

# at the end of the query, to upload to S3:
# aws s3 cp GWAS_Noam_1m_1s_m52xlarge.txt s3://pygmql-experiments/

gmql_repository = "hdfs:///"


def main():
    
    print("Input dataset statistics")
    print("Size: ", os.popen("hdfs dfs -du -h -s /HG19_ENCODE_BROAD").read())
    print("# Files: ", os.popen("hdfs dfs -ls /HG19_ENCODE_BROAD | wc -l").read())
    print("Size: ", os.popen("hdfs dfs -du -h -s /GWAS_Noam").read())
    print("# Files: ", os.popen("hdfs dfs -ls /GWAS_Noam | wc -l").read())

    start_time = time.time()
    gl.set_master("yarn")
    
    x = time.time()
    gwas = gl.load_from_path(gmql_repository + "GWAS_Noam")
    y = time.time()
    print("Loading of GWAS: ", time.strftime("%H:%M:%S", time.gmtime(y - x)))

    x = time.time()
    broad = gl.load_from_path(gmql_repository + "HG19_ENCODE_BROAD")
    y = time.time()
    print("Loading of BROAD: ", time.strftime("%H:%M:%S", time.gmtime(y - x)))

    acetyl = broad[broad['experiment_target'] == 'H3K27ac-human']
    peaked = acetyl.reg_project(new_field_dict={'peak': acetyl.right/2 + acetyl.left/2})
    enlarge = peaked.reg_project(new_field_dict={'left': peaked.peak - 1500, 'right': peaked.peak + 1500})
    X = enlarge.normal_cover(1, "ANY", groupBy=['biosample_term_name'])
    Y = X.normal_cover(1, 2)
    Z = X.join(Y, [gl.DLE(0)], 'left')

    M = Z.map(gwas, refName="Z", expName="GWAS", new_reg_fields={'bag': gl.BAG('trait')})
    N = M.reg_select(M.count_Z_GWAS > 0)
    P = N.reg_project(["count_Z_GWAS", "bag"])

    print("Starting execution")
    x = time.time()
    P.materialize(gmql_repository + "GWAS_result", all_load=False)
    y = time.time()
    print("Execution time: ", time.strftime("%H:%M:%S", time.gmtime(y - x)))

    print("Output dataset statistics")
    print("Size: ", os.popen("hdfs dfs -du -h -s /GWAS_result/files/").read())
    print("# Files: ", os.popen("hdfs dfs -ls /GWAS_result/files | wc -l").read())

    print("Loading result")
    x = time.time()
    result = gl.load_from_path(gmql_repository + "GWAS_result").materialize()
    y = time.time()
    print("Loading time: ", time.strftime("%H:%M:%S", time.gmtime(y - x)))

    print("REGIONS\n--------------------")
    print(result.regs.head())
    print("METADATA\n--------------------")
    print(result.meta.head())

    end_time = time.time()
    print("Total time: ", time.strftime("%H:%M:%S", time.gmtime(end_time - start_time)))


if __name__ == "__main__":
    main()
