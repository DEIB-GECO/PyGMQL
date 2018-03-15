import gmql as gl
print("PyGMQL imported")

d = gl.load_from_path("./data/narrow_sample")
print("Data loaded")

d = d.reg_select(d.pvalue >= 0.05)
print("Region selection")

d = d[d['experiment_target'] == 'ARID3A']
print("Meta selection")

d = d.reg_project(new_field_dict={
    'newField': (d.start + d.pvalue)/2
})
print("Region projection")

d = d.join(d, genometric_predicate=[gl.DLE(100000)])
print("Genometric join")

d = d.map(d)
print("Genometric map")

d = d.cover(minAcc=1, maxAcc="ANY")
print("Genometric cover")

d = d.materialize()
print("Materialization")

print("REGION DATA")
print(d.regs.head())

print("METADATA")
print(d.meta.head())