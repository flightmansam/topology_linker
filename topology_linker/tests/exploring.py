import topology_linker.res.FGinvestigation.fginvestigation.extraction as ext

query = ("SELECT *"
        f" FROM OBJECT WHERE OBJECT_NAME IN "
        f" ('MC10')")

# query = (
#         f"Select OBJECT.OBJECT_NO, ATTRIBUTE_VALUE, OBJECT_NAME "
#         f" From OBJECT_ATTR_VALUE INNER JOIN OBJECT"
#         f" ON OBJECT_ATTR_VALUE.OBJECT_NO = OBJECT.OBJECT_NO"
#         f" WHERE OBJECT_NAME IN ('MC10')"
#         f" AND ATTRIBUTE_TYPE = '117'"
# )

obj_data = ext.get_data_ordb(query)

print(obj_data.to_string())
#list of all regs in the database
# all_regs = obj_data['SITE_NAME'].unique()
#
#
# display_filename_prefix_middle = '├───'
# display_filename_prefix_middle_bus = '╞//═'
# display_filename_prefix_last_bus = '╘//═'
# display_filename_prefix_last = '└───'
# display_parent_prefix_middle = '     '
# display_parent_prefix_last = '│    '

