# import os
# from . import SCHEMA_FILE


# def load_reg_from_path(path, parser=None):
#     if parser is None:
#         # get the parser for the dataset
#         parser = RegionParser.from_schema_file(path)
#     # we need to take only the files of the regions, so only the files that does NOT end with '.meta'
#     all_files = set(glob(pathname=path + '/*'))
#     meta_files = set(glob(pathname=path + '/*.gdm.meta'))
#
#     only_region_files = all_files - meta_files
#     # print(only_region_files)
#     logger = logging.getLogger()
#     logger.debug("Loading region data from path {}".format(path))
#
#     n_files = len(only_region_files)
#     dfs = []
#     for file in tqdm.tqdm(only_region_files, total=n_files, disable=not is_progress_enabled()):
#         if file.endswith("schema") or file.endswith("_SUCCESS") or \
#            file.endswith(".xml"):
#             continue
#         abs_path = os.path.abspath(file)
#         key = generateNameKey(abs_path)
#
#         df = parser.parse_regions(abs_path)
#         df.index = np.repeat(key, len(df))
#         dfs.append(df)
#     if len(dfs) > 0:
#         result = pd.concat(objs=dfs, copy=False)
#     else:
#         result = pd.DataFrame(columns=parser.get_ordered_attributes())
#     result.index.name = "id_sample"
#     # ordering the columns
#     new_reg_fixed_fields = reg_fixed_fileds.copy()
#     if parser.strandPos is None:
#         new_reg_fixed_fields.remove("strand")
#     result = result[new_reg_fixed_fields + [c for c in result.columns if c not in new_reg_fixed_fields]]
#     # sorting index
#     result = result.sort_index()
#     return result


# def getHdfsSchema(path):
#     path = getGCSchemaPath(path)
#     client = InsecureClient('http://ip-172-31-37-168.us-east-2.compute.internal:50070', 'hadoop')
#     with client.read(path[5:]) as reader:
#         xml = reader.read().decode("utf-8")
#     tree = ET.ElementTree(ET.fromstring(xml))
#     return tree
#
#
# def getGCSchemaPath(path):
#     if path.endswith("/"):
#         return path + SCHEMA_FILE
#     else:
#         return path + "/" + SCHEMA_FILE
#
#
# def getGCSchema(path):
#     fs = gcsfs.GCSFileSystem(token=get_gcloud_token())
#     xml = fs.read_block(getGCSchemaPath(path), 0, None).decode("utf-8")
#     tree = ET.ElementTree(ET.fromstring(xml))
#     return tree
#
#
# def getLocalSchema(path):
#     schema_file = get_schema_path(path)
#     tree = ET.parse(schema_file)
#     return tree
#
#
# def __get_parser_old(path):
#     if path.startswith("gs://"):
#         tree = getGCSchema(path)
#     elif path.startswith("hdfs://"):
#         tree = getHdfsSchema(path)
#     else:
#         tree = getLocalSchema(path)
#     gmqlSchema = tree.getroot().getchildren()[0]
#     parser_type = gmqlSchema.get('type')
#     coordinate_system = gmqlSchema.get("coordinate_system")
#     if coordinate_system is None:
#         coordinate_system = COORDS_DEFAULT
#     field_nodes = gmqlSchema.getchildren()
#
#     i = 0
#     chrPos, startPos, stopPos, strandPos, otherPos = None, None, None, None, None
#     otherPos = []
#
#     if parser_type == GTF:
#         chrPos = 0      # seqname
#         startPos = 3    # start
#         stopPos = 4     # end
#         strandPos = 6   # strand
#         otherPos = [(1, 'source', 'string'), (2, 'feature', 'string'),
#                     (5, 'score', 'float'), (7, 'frame', 'string')]
#
#         for field in field_nodes:
#             name = list(field.itertext())[0]#.lower()
#             type = field.get('type').lower()
#             if name not in {'seqname', 'start', 'end', 'strand',
#                             'source', 'feature', 'score', 'frame'}:
#                 otherPos.append((i, name, type))
#             i += 1
#
#     else:
#         for field in field_nodes:
#             name = list(field.itertext())[0]#.lower()
#             type = field.get('type').lower()
#
#             if name in chr_aliases and chrPos is None:
#                 chrPos = i
#             elif name in start_aliases and startPos is None:
#                 startPos = i
#             elif name in stop_aliases and stopPos is None:
#                 stopPos = i
#             elif name in strand_aliases and strandPos is None:
#                 strandPos = i
#             else:   # other positions
#                 otherPos.append((i, name, type))
#             i += 1
#     if len(otherPos) == 0:
#         otherPos = None
#     return RegionParser(chrPos, startPos, stopPos, strandPos,
#                         otherPos, coordinate_system=coordinate_system,
#                         schema_format=parser_type)



