# yes | sudo yum install git
# yes | sudo yum install tmux
# sudo pip-3.4 install git+https://github.com/DEIB-GECO/PyGMQL.git

# hadoop distcp s3n://tica-test-datasets/unzip/HG19_BED_ANNOTATION/ hdfs:///

# hadoop distcp s3n://tica-test-datasets/unzip/HG19_ANNOTATION_GENCODE/ hdfs:///
# hadoop distcp s3n://tica-test-datasets/unzip/HG19_ENCODE_BROAD/ hdfs:///
# hadoop distcp s3n://tica-test-datasets/unzip/HG19_ENCODE_NARROW/ hdfs:///

# sudo wget https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
# sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
# sudo yum install -y apache-maven

# alternatives --config java
# alternatives --config javac




# sudo vim   /usr/local/lib/python3.4/site-packages/gmql/dataset/loaders/RegLoaderFile.py
# sudo vim   /usr/local/lib/python3.4/site-packages/gmql/settings.py


import gmql as gl
gl.set_master("yarn")

gmql_repository = "hdfs://"
my_out = gmql_repository + "/test/"

# bed_annotation_ds = "/HG19_BED_ANNOTATION"
narrow_ds = "/HG19_ENCODE_NARROW"
broad_ds = "/HG19_ENCODE_BROAD"
annotation_ds = "/HG19_ANNOTATION_GENCODE"



tss = gl.load_from_path(gmql_repository+ annotation_ds)
# tss.materialize(my_out + "/tss3/", all_load=False)

tss = tss[(tss['annotation_type'] == 'gene') & (tss['release_version'] == '19') ]
tss = tss.reg_select(tss.gene_type=="protein_coding").project()
# tss.materialize(my_out + '/tss2/', all_load=False)


proms = tss.reg_project(new_field_dict={'start': tss.start - 500, 'stop': tss.stop + 200}).project()
# proms.materialize(my_out + '/proms/', all_load=False) # samples  wc: 20345   81380  525973

encode_narrow = gl.load_from_path(gmql_repository + narrow_ds)
tfbs_raw = encode_narrow[(encode_narrow['assay'] == "ChIP-seq") & (encode_narrow['biosample_term_name'] == "GM12878") & \
                                                    (encode_narrow['output_type'] == "optimal idr thresholded peaks") & \
                         ~(encode_narrow['audit_not_compliant'].isin(["antibody not characterized to standard",
                                                                      "control low read depth",
                                                                      "insufficient read depth",
                                                                      "insufficient replicate concordance",
                                                                      "partially characterized antibody", 
                                                                      "poor library complexity", 
                                                                      "severe bottlenecking"])) & \
                        ~(encode_narrow['experiment_target'].isin(['H3K27ac-human',
                                                                   'H3K4me3-human',
                                                                   'H3K36me3-human',
                                                                   'H3K4me1-human',
                                                                   'H3K4me2-human',
                                                                   'H3K4me3-human',
                                                                   'H3K79me2-human',
                                                                   'H4K20me1-human']))]

#tfbs_raw.materialize(my_out + '/tfbs_raw_k562/', all_load=False) 


# all of them are not stranded
narrow_peaks = tfbs_raw.reg_project(new_field_dict={
    'left': tfbs_raw.left + tfbs_raw.peak,
    'right': tfbs_raw.left + tfbs_raw.peak + 1
})

cov = narrow_peaks.normal_cover(1, "ANY",groupBy=["experiment_target"]).project()
# cov.materialize(my_out + '/cov/', all_load=False) # 121 samples  wc: 90,330,169 361320676 2341216760


# for each tfbs
tfbs_in_prom = cov.join(proms,genometric_predicate=[gl.DLE(0)],output="left")
# tfbs_in_prom.materialize(my_out + '/tfbs_in_prom/', all_load=False) # 121 samples wc: 54,885,157 219540628 1422239666 ~ 450K per sample
# real    46m2.768s

pairs = tfbs_in_prom.join(tfbs_in_prom, genometric_predicate=[gl.DLE(700), gl.MD(1)],output="contig")
# pairs.materialize(my_out + '/pairs/', all_load=False) # 14641 samples wc: 3,453,183,605 13,812,734,420 89,596,592,840 ~ 235K per sample

pairs_dist = pairs.project(new_field_dict={ "distance" : pairs.right - pairs.left })
pairs_dist.materialize(my_out + '/pairs_dist_gm12878/', all_load=False) # 109,858,355 549291775 3218481907



