import topology_linker.res.FGinvestigation.fginvestigation.extraction as ext

query = ("SELECT OBJECT_NO, ASSET_CODE, SITE_NAME"
        f" FROM V_D_SITE_DETAILS WHERE SITE_NAME = 'MC10 CAMPBELLS SWAMP ESC'")

# query = (
#         f"Select * From OBJECT WHERE OBJECT_NAME = 'MC10 CAMPBELLS SWAMP ESC'"
# )

obj_data = ext.get_data_ordb(query)

print(obj_data)
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

